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
		raftstability.SignalFsync:  {Min: 0.001, Max: 0.05},
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
	for i := 0; i < 200; i++ {
		r.ObserveFsync(50 * time.Millisecond)
	}
	if !r.Critical() {
		t.Fatal("Critical() = false after sustained 50 ms fsyncs")
	}
	if got := r.Score(); 255-int(got) < 40 {
		t.Fatalf("Score() = %d, want a drop of >= 40", got)
	}
}

func heartbeats(r *Runner, start time.Time, gaps ...time.Duration) time.Time {
	now := start
	r.ObserveHeartbeat(now)
	for _, g := range gaps {
		now = now.Add(g)
		r.ObserveHeartbeat(now)
	}
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
