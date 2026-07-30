// Package stability implements etcd's side of HeirRaft's node-stability
// scorer (TASKS.md T5.1, DESIGN.md §2.1): it samples local OS/cgroup CPU and
// memory signals and etcd's own WAL fsync latency, and composes them into a
// score behind raft's stability.Scorer interface. Wiring a Runner instance
// into raft.Config and etcd's flags/startup path is T5.2's job -- this
// package is self-contained and etcd-startup-agnostic so it can be unit
// tested with fake samplers.
//
// RTT jitter (DESIGN.md §2.1's fourth signal) is intentionally omitted in
// this version: TASKS.md's T5.1 text permits "RTT jitter ... if available,
// else omit v1 and note", and DESIGN.md doesn't specify how to aggregate the
// per-peer RTT samples rafthttp already collects (etcd's peer prober is
// per-follower; a leader has one RTT series per peer, not one local scalar)
// into the single local signal EWMAScorer.Sample expects -- that's a design
// question left to the supervisors, not something to improvise (see project
// root CLAUDE.md's working-conventions on this). The remaining three
// signals' weights (DESIGN.md §5 defaults: CPU .3, mem .2, fsync .3) are
// renormalised to sum to 1 -- see v1Weights.
package stability

import (
	"context"
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

// v1Weights is DESIGN.md §5's default composite (CPU .3, mem .2, fsync .3,
// jitter .2), minus the omitted jitter signal, renormalised so the
// remaining three still sum to 1: .3/.8=.375, .2/.8=.25, .3/.8=.375.
func v1Weights() map[raftstability.Signal]float64 {
	return map[raftstability.Signal]float64{
		raftstability.SignalCPU:    0.375,
		raftstability.SignalMemory: 0.25,
		raftstability.SignalFsync:  0.375,
	}
}

// DefaultBounds gives sane reference bounds for the three v1 signals:
//   - CPU / memory are already normalised fractions by this package's
//     samplers, so their bounds are simply [0,1].
//   - WAL fsync latency defaults to healthy at 1ms, fully unhealthy at 1s --
//     these are etcd-specific and host/disk dependent, so callers with a
//     different storage profile should override this entry in Config.Bounds.
func DefaultBounds() map[raftstability.Signal]raftstability.Bounds {
	return map[raftstability.Signal]raftstability.Bounds{
		raftstability.SignalCPU:    {Min: 0, Max: 1},
		raftstability.SignalMemory: {Min: 0, Max: 1},
		raftstability.SignalFsync:  {Min: 0.001, Max: 1.0},
	}
}

const defaultPeriod = 500 * time.Millisecond

// Config configures a Runner.
type Config struct {
	Logger *zap.Logger // defaults to a no-op logger if nil
	Period time.Duration // CPU/mem sampling period; defaults to 500ms (T5.1 accept criterion)
	CPU    CPUSampler    // optional; nil means the CPU signal is never sampled
	Mem    MemSampler    // optional; nil means the memory signal is never sampled
	Bounds map[raftstability.Signal]raftstability.Bounds // required; see DefaultBounds
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
}

// NewRunner builds a Runner. cfg.Bounds must have entries for CPU, Memory,
// and Fsync (DefaultBounds provides sane starting values); cfg.CPU/cfg.Mem
// may be nil to disable that signal.
func NewRunner(cfg Config) (*Runner, error) {
	scorer, err := raftstability.NewEWMAScorer(raftstability.EWMAConfig{
		Alpha:   0.2,
		Weights: v1Weights(),
		Bounds:  cfg.Bounds,
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
		lg:     lg,
		period: period,
		cpu:    cfg.CPU,
		mem:    cfg.Mem,
		scorer: scorer,
	}, nil
}

// Score implements go.etcd.io/raft/v3/stability.Scorer.
func (r *Runner) Score() uint8 { return r.scorer.Score() }

// ObserveFsync feeds one WAL fsync latency sample into the fsync signal.
// Safe to call from any goroutine. This is the hook etcd's WAL package will
// call alongside its existing walFsyncSec.Observe (server/storage/wal/wal.go)
// once T5.2 wires a Runner instance into etcd's startup path.
func (r *Runner) ObserveFsync(d time.Duration) {
	r.scorer.Sample(raftstability.SignalFsync, d.Seconds())
}

// sampleOnce pulls one CPU/mem sample (skipping either signal whose sampler
// is nil or errors) and logs the resulting composite score at debug level
// (T5.1 accept criterion: "scorer value visible in etcd log at debug").
func (r *Runner) sampleOnce() {
	if r.cpu != nil {
		if v, err := r.cpu.Sample(); err != nil {
			r.lg.Warn("stability: CPU sample failed", zap.Error(err))
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
