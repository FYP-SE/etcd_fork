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

// Critical alarm (2026-09-28): a signal is critical once its RAW value for
// a sampling period, normalised with the same bounds as the score, is at or
// below criticalLevel for criticalPeriods consecutive periods that had a
// sample for it. Driving the alarm from the smoothed EWMA instead took ~10
// periods (~5 s) to go critical, so a slow-disk leader handed over only
// after 6.8 s (smoke batch 20260928_220425). The smoothed score still ranks
// heirs.
const (
	criticalLevel   = 0.1
	criticalPeriods = 2
)

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
	bounds map[raftstability.Signal]raftstability.Bounds

	// criticalRun[sig]: consecutive periods with evidence in which sig's
	// raw period value was critical. Guarded by aggMu.
	criticalRun [4]int

	hbInterval time.Duration

	// Per-period accumulators (guarded by aggMu). fsync and heartbeat
	// jitter arrive per operation; SampleNow feeds each one's MEAN over the
	// period into the EWMA. Fed per operation, one fast fsync on a
	// throttled disk lifted the signal above CriticalLevel and broke the
	// leader's DegradeWindow run (graceful handover took 25 s, 2026-09-28).
	aggMu    sync.Mutex
	hbLast   time.Time
	hbFrom   uint64
	fsyncSum time.Duration
	fsyncN   int
	// jitterMaxLate: the period's largest (gap - interval), floored at 0;
	// jitterGaps: how many gaps were seen (0 = no heartbeat evidence).
	jitterMaxLate time.Duration
	jitterGaps    int
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
		bounds:     cfg.Bounds,
		hbInterval: cfg.HeartbeatInterval,
	}, nil
}

// Score implements go.etcd.io/raft/v3/stability.Scorer.
func (r *Runner) Score() uint8 { return r.scorer.Score() }

// HeartbeatInterval returns the configured leader heartbeat period (0 = the
// jitter signal is off).
func (r *Runner) HeartbeatInterval() time.Duration { return r.hbInterval }

// SignalHealth returns each signal's smoothed health in [0,1] by name
// (cpu, memory, fsync, jitter), for the etcd_heirraft_signal_health metric.
func (r *Runner) SignalHealth() map[string]float64 {
	return map[string]float64{
		"cpu":    r.scorer.Health(raftstability.SignalCPU),
		"memory": r.scorer.Health(raftstability.SignalMemory),
		"fsync":  r.scorer.Health(raftstability.SignalFsync),
		"jitter": r.scorer.Health(raftstability.SignalJitter),
	}
}

// Critical implements go.etcd.io/raft/v3/stability.CriticalReporter: true if
// any single signal's raw period value has been critical for criticalPeriods
// consecutive periods with evidence (DESIGN_UPDATE.md D7).
func (r *Runner) Critical() bool {
	r.aggMu.Lock()
	defer r.aggMu.Unlock()
	for _, n := range r.criticalRun {
		if n >= criticalPeriods {
			return true
		}
	}
	return false
}

// observePeriod feeds one period's raw value for sig into the EWMA and the
// critical run.
func (r *Runner) observePeriod(sig raftstability.Signal, value float64) {
	r.scorer.Sample(sig, value)
	b, ok := r.bounds[sig]
	if !ok {
		return
	}
	health := 1 - (value-b.Min)/(b.Max-b.Min)
	r.aggMu.Lock()
	if health <= criticalLevel {
		r.criticalRun[sig]++
	} else {
		r.criticalRun[sig] = 0
	}
	r.aggMu.Unlock()
}

// ObserveHeartbeat records one MsgHeartbeat arrival from leader `from` at
// time now, for the jitter signal. The signal is heartbeat LATENESS: per
// sampling period, the largest gap between consecutive heartbeats from the
// same leader minus HeartbeatInterval (0 if none was late). Every heartbeat
// counts, with or without a ReadIndex Context: extra read heartbeats only
// shorten gaps, and raft's regular tick heartbeat itself carries the
// pending read context while a read is outstanding, so it cannot be
// filtered out (2026-09-29). A different sender (leader change) starts a
// new series, and gaps over maxHeartbeatGapIntervals are not sampled.
// Measured locally on each follower; no new messages.
func (r *Runner) ObserveHeartbeat(from uint64, now time.Time) {
	if r.hbInterval <= 0 {
		return
	}
	r.aggMu.Lock()
	defer r.aggMu.Unlock()
	last, lastFrom := r.hbLast, r.hbFrom
	r.hbLast, r.hbFrom = now, from
	if last.IsZero() || from != lastFrom {
		return
	}
	gap := now.Sub(last)
	if gap < 0 || gap > maxHeartbeatGapIntervals*r.hbInterval {
		return
	}
	late := gap - r.hbInterval
	if late < 0 {
		late = 0
	}
	if late > r.jitterMaxLate {
		r.jitterMaxLate = late
	}
	r.jitterGaps++
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
	fsyncSum, fsyncN, jitterLate, jitterGaps := r.fsyncSum, r.fsyncN, r.jitterMaxLate, r.jitterGaps
	r.fsyncSum, r.fsyncN, r.jitterMaxLate, r.jitterGaps = 0, 0, 0, 0
	if jitterGaps == 0 && r.hbInterval > 0 {
		// No heartbeat evidence this period: this node is the leader or has
		// lost it. Jitter evidence is void, so a new leader cannot inherit a
		// follower-time jitter alarm. (fsync keeps no-evidence = unchanged:
		// sparse fsyncs are normal.)
		r.criticalRun[raftstability.SignalJitter] = 0
	}
	r.aggMu.Unlock()
	if fsyncN > 0 {
		r.observePeriod(raftstability.SignalFsync, (fsyncSum / time.Duration(fsyncN)).Seconds())
	}
	if jitterGaps > 0 {
		r.observePeriod(raftstability.SignalJitter, jitterLate.Seconds())
	}
	if r.cpu != nil {
		if v, err := r.cpu.Sample(); err != nil {
			// The first sample of a delta-based sampler has no history yet:
			// expected once per start, not worth a warning.
			if !errors.Is(err, errNoPressureHistory) {
				r.lg.Warn("stability: CPU sample failed", zap.Error(err))
			}
		} else {
			r.observePeriod(raftstability.SignalCPU, v)
		}
	}
	if r.mem != nil {
		if v, err := r.mem.Sample(); err != nil {
			r.lg.Warn("stability: memory sample failed", zap.Error(err))
		} else {
			r.observePeriod(raftstability.SignalMemory, v)
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
