// Package stability implements etcd's side of HeirRaft's node-stability
// scorer (TASKS.md T5.1, DESIGN.md §2.1): it samples local OS/cgroup CPU and
// memory signals and etcd's own WAL fsync latency, and composes them into a
// score behind raft's stability.Scorer interface. Wiring a Runner instance
// into raft.Config and etcd's flags/startup path is T5.2's job -- this
// package is self-contained and etcd-startup-agnostic so it can be unit
// tested with fake samplers.
//
// DESIGN_UPDATE.md D7 (2026-09-28): four signals -- CPU pressure (PSI, not
// usage), memory, WAL fsync latency (fully bad at 50 ms, not 1 s), and
// heartbeat jitter, measured locally on a follower from the variation in
// the leader's heartbeat inter-arrival times (no new messages). Weights are
// DESIGN.md §5's defaults (CPU .3, mem .2, fsync .3, jitter .2). The
// weighted score ranks heirs; Critical() reports any single critical signal
// so the leader can hand over (weighted + critical trigger, chosen by
// Piyumi).
package stability

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	raftstability "go.etcd.io/raft/v3/stability"
)

// CPUSampler reports current CPU utilisation as a fraction in [0,1] (0 =
// idle, 1 = fully saturated against whatever capacity the implementation
// normalises against -- host, cgroup quota, etc).
type CPUSampler interface {
	Sample() (float64, error)
}

// MemSampler reports current memory pressure as a fraction in [0,1] (0 =
// no pressure, 1 = at the configured limit).
type MemSampler interface {
	Sample() (float64, error)
}

// weights is DESIGN.md §5's default composite: CPU .3, mem .2, fsync .3,
// jitter .2. Any single fully bad signal lowers the score by >= 51 of 255,
// above 2x the default HysteresisMargin (DESIGN_UPDATE.md D7).
func weights() map[raftstability.Signal]float64 {
	return map[raftstability.Signal]float64{
		raftstability.SignalCPU:    0.3,
		raftstability.SignalMemory: 0.2,
		raftstability.SignalFsync:  0.3,
		raftstability.SignalJitter: 0.2,
	}
}

// DefaultBounds gives the reference bounds (healthy at Min, fully bad at
// Max) for the four signals (DESIGN_UPDATE.md D7):
//   - CPU pressure (stall fraction): healthy <= 2 %, fully bad at 50 %.
//   - memory (fraction of limit): [0,1], unchanged from v1.
//   - WAL fsync: healthy up to 10 ms (etcd's guidance: p99 < 10 ms), fully
//     bad at 50 ms (v1's 1 s bound made the signal useless). The 10 ms floor
//     keeps ordinary disk jitter out of the score (measured 2026-09-28).
//   - heartbeat jitter (|inter-arrival - interval|): healthy 2 ms, fully
//     bad 100 ms, i.e. a whole heartbeat interval off.
//
// All host dependent: callers can override entries in Config.Bounds.
func DefaultBounds() map[raftstability.Signal]raftstability.Bounds {
	return map[raftstability.Signal]raftstability.Bounds{
		raftstability.SignalCPU:    {Min: 0.02, Max: 0.5},
		raftstability.SignalMemory: {Min: 0, Max: 1},
		raftstability.SignalFsync:  {Min: 0.010, Max: 0.05},
		raftstability.SignalJitter: {Min: 0.002, Max: 0.1},
	}
}

// maxHeartbeatGapIntervals: an inter-arrival gap longer than this many
// heartbeat intervals is a leader change, restart or partition, not jitter,
// and is not sampled.
const maxHeartbeatGapIntervals = 5

const defaultPeriod = 500 * time.Millisecond

// Config configures a Runner.
type Config struct {
	Logger *zap.Logger                                   // defaults to a no-op logger if nil
	Period time.Duration                                 // CPU/mem sampling period; defaults to 500ms (T5.1 accept criterion)
	CPU    CPUSampler                                    // optional; nil means the CPU signal is never sampled
	Mem    MemSampler                                    // optional; nil means the memory signal is never sampled
	Bounds map[raftstability.Signal]raftstability.Bounds // required; see DefaultBounds
	// HeartbeatInterval is the leader's heartbeat period. Zero disables the
	// jitter signal (ObserveHeartbeat becomes a no-op).
	HeartbeatInterval time.Duration
}

// Runner periodically samples CPU/memory and exposes a push-style hook for
// WAL fsync latency, composing all of it into a raft/stability.Scorer via an
// EWMA (DESIGN.md §2.1 "Scorer v1"). A *Runner implements
// go.etcd.io/raft/v3/stability.Scorer directly, so it can be assigned to
// raft.Config.StabilityScorer as-is.
type Runner struct {
	lg     *zap.Logger
	period time.Duration
	cpu    CPUSampler
	mem    MemSampler
	scorer *raftstability.EWMAScorer

	hbInterval time.Duration

	// Per-period accumulators (guarded by aggMu). fsync and heartbeat
	// jitter arrive per operation; SampleNow feeds each one's MEAN over the
	// period into the EWMA. Fed per operation, one fast fsync on a
	// throttled disk lifted the signal above CriticalLevel and broke the
	// leader's DegradeWindow run (graceful handover took 25 s, 2026-09-28).
	aggMu     sync.Mutex
	hbLast    time.Time
	fsyncSum  time.Duration
	fsyncN    int
	jitterSum time.Duration
	jitterN   int
}

// NewRunner builds a Runner. cfg.Bounds must have entries for CPU, Memory,
// and Fsync (DefaultBounds provides sane starting values); cfg.CPU/cfg.Mem
// may be nil to disable that signal.
func NewRunner(cfg Config) (*Runner, error) {
	scorer, err := raftstability.NewEWMAScorer(raftstability.EWMAConfig{
		Alpha:         0.2,
		Weights:       weights(),
		Bounds:        cfg.Bounds,
		CriticalLevel: 0.1,
	})
	if err != nil {
		return nil, err
	}

	period := cfg.Period
	if period <= 0 {
		period = defaultPeriod
	}
	lg := cfg.Logger
	if lg == nil {
		lg = zap.NewNop()
	}

	return &Runner{
		lg:         lg,
		period:     period,
		cpu:        cfg.CPU,
		mem:        cfg.Mem,
		scorer:     scorer,
		hbInterval: cfg.HeartbeatInterval,
	}, nil
}

// Score implements go.etcd.io/raft/v3/stability.Scorer.
func (r *Runner) Score() uint8 { return r.scorer.Score() }

// HeartbeatInterval returns the configured leader heartbeat period (0 = the
// jitter signal is off).
func (r *Runner) HeartbeatInterval() time.Duration { return r.hbInterval }

// Critical implements go.etcd.io/raft/v3/stability.CriticalReporter: true
// if any single signal is critical (DESIGN_UPDATE.md D7).
func (r *Runner) Critical() bool { return r.scorer.Critical() }

// ObserveHeartbeat feeds the jitter signal from one MsgHeartbeat arrival
// from the leader at time now: the sample is |gap - HeartbeatInterval|.
// Measured locally on each follower, so a node with a worse link to the
// leader scores worse without any new messages. The first arrival and gaps
// over maxHeartbeatGapIntervals (leader change, restart) are not sampled.
func (r *Runner) ObserveHeartbeat(now time.Time) {
	if r.hbInterval <= 0 {
		return
	}
	r.aggMu.Lock()
	defer r.aggMu.Unlock()
	last := r.hbLast
	r.hbLast = now
	if last.IsZero() {
		return
	}
	gap := now.Sub(last)
	if gap < 0 || gap > maxHeartbeatGapIntervals*r.hbInterval {
		return
	}
	dev := gap - r.hbInterval
	if dev < 0 {
		dev = -dev
	}
	r.jitterSum += dev
	r.jitterN++
}

// ObserveFsync feeds one WAL fsync latency sample into the fsync signal.
// Safe to call from any goroutine. This is the hook etcd's WAL package will
// call alongside its existing walFsyncSec.Observe (server/storage/wal/wal.go)
// once T5.2 wires a Runner instance into etcd's startup path.
func (r *Runner) ObserveFsync(d time.Duration) {
	r.aggMu.Lock()
	r.fsyncSum += d
	r.fsyncN++
	r.aggMu.Unlock()
}

// SampleNow closes one sampling period: it pulls CPU/memory samples and
// feeds the period's mean fsync latency and mean heartbeat jitter (each only
// if there were any) into the EWMA. Run calls it every Period; tests and
// callers may call it directly.
func (r *Runner) SampleNow() { r.sampleOnce() }

// sampleOnce pulls one CPU/mem sample (skipping either signal whose sampler
// is nil or errors) and logs the resulting composite score at debug level
// (T5.1 accept criterion: "scorer value visible in etcd log at debug").
func (r *Runner) sampleOnce() {
	r.aggMu.Lock()
	fsyncSum, fsyncN, jitterSum, jitterN := r.fsyncSum, r.fsyncN, r.jitterSum, r.jitterN
	r.fsyncSum, r.fsyncN, r.jitterSum, r.jitterN = 0, 0, 0, 0
	r.aggMu.Unlock()
	if fsyncN > 0 {
		r.scorer.Sample(raftstability.SignalFsync, (fsyncSum / time.Duration(fsyncN)).Seconds())
	}
	if jitterN > 0 {
		r.scorer.Sample(raftstability.SignalJitter, (jitterSum / time.Duration(jitterN)).Seconds())
	}
	if r.cpu != nil {
		if v, err := r.cpu.Sample(); err != nil {
			// The first sample of a delta-based sampler has no history yet:
			// expected once per start, not worth a warning.
			if !errors.Is(err, errNoPressureHistory) {
				r.lg.Warn("stability: CPU sample failed", zap.Error(err))
			}
		} else {
			r.scorer.Sample(raftstability.SignalCPU, v)
		}
	}
	if r.mem != nil {
		if v, err := r.mem.Sample(); err != nil {
			r.lg.Warn("stability: memory sample failed", zap.Error(err))
		} else {
			r.scorer.Sample(raftstability.SignalMemory, v)
		}
	}
	r.lg.Debug("stability: score updated", zap.Uint8("score", r.scorer.Score()))
}

// Run samples CPU/memory every Period until ctx is cancelled. Intended to be
// started in its own goroutine by the host (T5.2's job to actually call it).
func (r *Runner) Run(ctx context.Context) {
	t := time.NewTicker(r.period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sampleOnce()
		}
	}
}
