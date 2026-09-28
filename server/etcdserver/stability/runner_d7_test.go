// DESIGN_UPDATE.md D7 on the etcd side: four signals (CPU pressure, memory,
// fsync, heartbeat jitter), tighter bounds, and Critical() passthrough.
package stability

import (
	"testing"
	"time"

	raftstability "go.etcd.io/raft/v3/stability"
)

func TestDefaultBounds_D7(t *testing.T) {
	b := DefaultBounds()
	want := map[raftstability.Signal]raftstability.Bounds{
		raftstability.SignalCPU:    {Min: 0.02, Max: 0.5},
		raftstability.SignalMemory: {Min: 0, Max: 1},
		raftstability.SignalFsync:  {Min: 0.010, Max: 0.05},
		raftstability.SignalJitter: {Min: 0.002, Max: 0.1},
	}
	for sig, w := range want {
		if b[sig] != w {
			t.Errorf("DefaultBounds()[%d] = %+v, want %+v", sig, b[sig], w)
		}
	}
}

// 50 ms fsyncs are now fully bad (v1 needed 1 s): critical, and the score
// drops by at least 2x the default hysteresis margin.
func TestRunner_FiftyMsFsyncIsCritical(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: 100 * time.Millisecond})
	for i := 0; i < 50; i++ {
		r.ObserveFsync(50 * time.Millisecond)
		r.SampleNow()
	}
	if !r.Critical() {
		t.Fatal("Critical() = false after sustained 50 ms fsyncs")
	}
	if got := r.Score(); 255-int(got) < 40 {
		t.Fatalf("Score() = %d, want a drop of >= 40", got)
	}
}

// heartbeats feeds arrivals and closes a sampling period every 5 of them,
// as Run's 500 ms ticker would at a 100 ms heartbeat.
func heartbeats(r *Runner, start time.Time, gaps ...time.Duration) time.Time {
	now := start
	r.ObserveHeartbeat(now)
	for i, g := range gaps {
		now = now.Add(g)
		r.ObserveHeartbeat(now)
		if i%5 == 4 {
			r.SampleNow()
		}
	}
	r.SampleNow()
	return now
}

func repeat(d time.Duration, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func TestRunner_RegularHeartbeatsStayHealthy(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: 100 * time.Millisecond})
	heartbeats(r, time.Unix(0, 0), repeat(101*time.Millisecond, 200)...)
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d with 1 ms heartbeat jitter, want 255", got)
	}
	if r.Critical() {
		t.Fatal("Critical() with regular heartbeats")
	}
}

// Heartbeats alternating 0 ms / 200 ms apart deviate 100 ms from the
// interval every time: fully bad jitter.
func TestRunner_JitteryHeartbeatsAreCritical(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: 100 * time.Millisecond})
	var gaps []time.Duration
	for i := 0; i < 200; i++ {
		gaps = append(gaps, 0, 200*time.Millisecond)
	}
	heartbeats(r, time.Unix(0, 0), gaps...)
	if !r.Critical() {
		t.Fatal("Critical() = false with 100 ms heartbeat jitter")
	}
	if got := r.Score(); 255-int(got) < 40 {
		t.Fatalf("Score() = %d, want a drop of >= 40", got)
	}
}

// A gap of more than 5 intervals is a leader change or restart, not jitter.
func TestRunner_LongHeartbeatGapIgnored(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: 100 * time.Millisecond})
	now := heartbeats(r, time.Unix(0, 0), repeat(100*time.Millisecond, 10)...)
	heartbeats(r, now.Add(3*time.Second), repeat(100*time.Millisecond, 10)...)
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d after one long gap, want 255", got)
	}
}

// Without a configured interval the jitter signal is off.
func TestRunner_NoHeartbeatIntervalDisablesJitter(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	var gaps []time.Duration
	for i := 0; i < 100; i++ {
		gaps = append(gaps, 0, 200*time.Millisecond)
	}
	heartbeats(r, time.Unix(0, 0), gaps...)
	if r.Critical() {
		t.Fatal("jitter counted with no HeartbeatInterval configured")
	}
}

// Runner is a raft CriticalReporter, so raft's graceful handover sees it.
func TestRunner_ImplementsCriticalReporter(t *testing.T) {
	var _ raftstability.CriticalReporter = newTestRunner(t, Config{Bounds: DefaultBounds()})
}

// fsync up to 10 ms (etcd's guidance: WAL fsync p99 < 10 ms) is fully
// healthy, so ordinary disk jitter is not score noise (2026-09-28: laptop
// fsync varies ~2-20 ms and healthy nodes' scores spread p99 25 points).
func TestRunner_FsyncUnderTenMsIsFullyHealthy(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 200; i++ {
		r.ObserveFsync(time.Duration(2+i%9) * time.Millisecond) // 2..10 ms
		if i%10 == 9 {
			r.SampleNow()
		}
	}
	if got := r.Score(); got != 255 {
		t.Fatalf("Score() = %d with fsync 2-10 ms, want 255", got)
	}
}

// Per-period aggregation (2026-09-28, slow-disk integration run): a
// throttled disk gives mostly slow fsyncs with the odd fast one. Fed per
// operation, a single fast fsync lifted the EWMA from 0 to 0.2 health, above
// CriticalLevel 0.1, broke DegradeWindow's consecutive run, and graceful
// handover took 25 s. The runner now feeds one MEAN per sampling period, so
// a period averaging 72 ms stays fully bad and Critical() holds throughout.
func TestRunner_CriticalHoldsWithOccasionalFastFsync(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for period := 0; period < 30; period++ {
		for i := 0; i < 9; i++ {
			r.ObserveFsync(80 * time.Millisecond)
		}
		r.ObserveFsync(2 * time.Millisecond) // the odd fast one
		r.SampleNow()
		if period >= 12 && !r.Critical() { // after EWMA convergence
			t.Fatalf("period %d: Critical() = false with fsync mean 72 ms", period)
		}
	}
}

// A period with no fsyncs (idle follower) leaves the fsync signal alone.
func TestRunner_EmptyPeriodDoesNotResetFsync(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 30; i++ {
		r.ObserveFsync(80 * time.Millisecond)
		r.SampleNow()
	}
	before := r.Score()
	r.SampleNow()
	r.SampleNow()
	if got := r.Score(); got != before {
		t.Fatalf("Score() %d -> %d over periods with no fsync", before, got)
	}
}

func TestRunner_SignalHealth(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 30; i++ {
		r.ObserveFsync(80 * time.Millisecond)
		r.SampleNow()
	}
	h := r.SignalHealth()
	for _, name := range []string{"cpu", "memory", "fsync", "jitter"} {
		if _, ok := h[name]; !ok {
			t.Fatalf("SignalHealth() missing %q: %v", name, h)
		}
	}
	if h["fsync"] > 0.05 || h["cpu"] != 1 {
		t.Fatalf("SignalHealth() = %v, want fsync ~0 and cpu 1", h)
	}
}
