// T5.1 (TASKS.md) -- etcd-side stability scorer. Written before runner.go's
// implementation, per research-plan-docs/CLAUDE.md's TDD convention. All
// tests here use fake CPU/mem samplers (func-typed), never the real
// /proc-reading implementation (proc_sampler_test.go covers that in
// isolation), so scoring behaviour is deterministic and independent of the
// host machine's actual load.
package stability

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	raftstability "go.etcd.io/raft/v3/stability"
)

// fakeSampler is a func-typed CPUSampler/MemSampler for tests.
type fakeSampler func() (float64, error)

func (f fakeSampler) Sample() (float64, error) { return f() }

func constSampler(v float64) fakeSampler {
	return func() (float64, error) { return v, nil }
}

func newTestRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return r
}

// TestNewRunner_RejectsInvalidEWMAConfig confirms Runner surfaces
// EWMAConfig validation errors (e.g. a Bounds override missing an entry)
// rather than silently building a broken scorer.
func TestNewRunner_RejectsInvalidEWMAConfig(t *testing.T) {
	_, err := NewRunner(Config{
		CPU:    constSampler(0),
		Mem:    constSampler(0),
		Bounds: map[raftstability.Signal]raftstability.Bounds{}, // missing CPU/Mem/Fsync entries
	})
	if err == nil {
		t.Fatal("expected error for incomplete Bounds, got nil")
	}
}

// TestRunner_ScoreStartsHealthy mirrors EWMAScorer's own "healthy until
// proven otherwise" contract: a freshly built Runner (no samples fed yet)
// reports a fully-healthy score.
func TestRunner_ScoreStartsHealthy(t *testing.T) {
	r := newTestRunner(t, Config{CPU: constSampler(0), Mem: constSampler(0), Bounds: DefaultBounds()})
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d, want 255 (healthy default)", got)
	}
}

// TestRunner_SampleOnceFeedsCPUAndMem drives one manual sample pull with
// samplers pinned at "fully unhealthy" (busy=1.0, used=1.0) and checks the
// composite score drops from its healthy default.
func TestRunner_SampleOnceFeedsCPUAndMem(t *testing.T) {
	r := newTestRunner(t, Config{CPU: constSampler(1.0), Mem: constSampler(1.0), Bounds: DefaultBounds()})
	before := r.Score()
	for i := 0; i < 50; i++ { // enough EWMA iterations (alpha=0.2) to move well off 255
		r.sampleOnce()
	}
	after := r.Score()
	if after >= before {
		t.Fatalf("Score() after 50 unhealthy samples = %d, want < initial %d", after, before)
	}
	// fsync is never sampled here, so its EWMA stays at the healthy default
	// (1.0) and alone contributes its full v1Weights share (.375) to the
	// composite once CPU/mem have converged to 0: floor ~= .375*255 = 96.
	if wantFloor := uint8(100); after > wantFloor {
		t.Fatalf("Score() = %d after sustained unhealthy CPU+mem samples, want <= %d (fsync-only floor)", after, wantFloor)
	}
}

// TestRunner_SampleOnceSurvivesSamplerError confirms a failing sampler
// doesn't panic or corrupt the score -- it's simply skipped for that tick.
func TestRunner_SampleOnceSurvivesSamplerError(t *testing.T) {
	failing := fakeSampler(func() (float64, error) { return 0, errors.New("boom") })
	r := newTestRunner(t, Config{CPU: failing, Mem: failing, Bounds: DefaultBounds()})
	r.sampleOnce() // must not panic
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d after only-failing samples, want unchanged healthy default 255", got)
	}
}

// TestRunner_ObserveFsyncFeedsScore confirms the WAL fsync push-style hook
// (the mechanism wal.go will call into, wiring itself is T5.2's job) moves
// the composite score when fed sustained slow fsyncs.
func TestRunner_ObserveFsyncFeedsScore(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()}) // no CPU/Mem samplers at all
	before := r.Score()
	for i := 0; i < 50; i++ {
		r.ObserveFsync(2 * time.Second) // far past DefaultBounds' fsync Max
	}
	after := r.Score()
	if after >= before {
		t.Fatalf("Score() after 50 slow fsync observations = %d, want < initial %d", after, before)
	}
}

// TestRunner_NilSamplersAreNoOp confirms a Runner built with CPU/Mem left
// nil (e.g. a deployment that only wants the fsync signal) never panics on
// sampleOnce and never touches those signals' EWMA state -- Score reflects
// only whatever ObserveFsync feeds it.
func TestRunner_NilSamplersAreNoOp(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 10; i++ {
		r.sampleOnce() // CPU, Mem both nil
	}
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d with nil samplers and no fsync observations, want 255", got)
	}
}

// TestRunner_RunTicksUntilCancelled confirms Run's loop actually calls
// sampleOnce on the configured period and stops promptly when ctx is
// cancelled -- the goroutine T5.2 will start in etcd's server startup path.
func TestRunner_RunTicksUntilCancelled(t *testing.T) {
	calls := make(chan struct{}, 100)
	counting := fakeSampler(func() (float64, error) {
		select {
		case calls <- struct{}{}:
		default:
		}
		return 0, nil
	})
	r := newTestRunner(t, Config{CPU: counting, Bounds: DefaultBounds(), Period: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("Run did not sample within 1s of a 5ms period")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of context cancellation")
	}
}

// TestRunner_DefaultPeriodIsFiveHundredMillis pins T5.1's accept criterion
// ("Sampling goroutine, 500 ms period") as an explicit regression test, not
// just an inline comment.
func TestRunner_DefaultPeriodIsFiveHundredMillis(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	if r.period != 500*time.Millisecond {
		t.Fatalf("period = %v, want 500ms default", r.period)
	}
}

// TestRunner_LogsScoreAtDebug pins the accept criterion "scorer value
// visible in etcd log at debug": sampleOnce must emit a debug-level log
// line carrying the current score.
func TestRunner_LogsScoreAtDebug(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	r := newTestRunner(t, Config{
		Logger: zap.New(core),
		CPU:    constSampler(0),
		Bounds: DefaultBounds(),
	})
	r.sampleOnce()

	entries := logs.FilterMessage("stability: score updated").All()
	if len(entries) != 1 {
		t.Fatalf("got %d 'stability: score updated' debug entries, want 1", len(entries))
	}
	if entries[0].Level != zap.DebugLevel {
		t.Fatalf("log level = %v, want debug", entries[0].Level)
	}
	found := false
	for _, f := range entries[0].Context {
		if f.Key == "score" {
			found = true
		}
	}
	if !found {
		t.Fatal("debug log entry missing a 'score' field")
	}
}

// TestRunner_NilLoggerDefaultsToNop confirms a Config with no Logger set
// doesn't panic (the common case: most fake-sampler tests above omit it).
func TestRunner_NilLoggerDefaultsToNop(t *testing.T) {
	r := newTestRunner(t, Config{CPU: constSampler(0), Bounds: DefaultBounds()})
	r.sampleOnce() // must not panic despite no Logger in cfg
}
