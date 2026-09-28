package stability

import (
	"testing"
	"time"
)

// Jitter as heartbeat LATENESS (2026-09-29). raft's regular tick heartbeat
// carries the pending ReadIndex context whenever a linearizable read is
// outstanding (bcastHeartbeatWithCtx(readOnly.heartbeatCtx())), and extra
// ReadIndex heartbeats are sent off the tick. Neither "count every
// heartbeat, measure |gap - interval|" (extra heartbeats -> short gaps ->
// fake jitter) nor "skip heartbeats with a Context" (drops tick heartbeats
// -> 200/300 ms gaps -> fake jitter) works under read load: measured with
// the probe running, healthy followers' jitter health fell to ~0.6 and
// recovered to 0.99 when the probe stopped. The sample is now, per period,
// the largest gap between consecutive heartbeats from the same leader minus
// the interval (0 if none was late). Extra heartbeats only shorten gaps.

const hb = 100 * time.Millisecond

func jitterRunner(t *testing.T) *Runner {
	return newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: hb})
}

// Regular ticks plus extra read heartbeats in between: healthy.
func TestJitter_ExtraHeartbeatsAreNotJitter(t *testing.T) {
	r := jitterRunner(t)
	now := time.Unix(0, 0)
	for p := 0; p < 20; p++ {
		for i := 0; i < 5; i++ {
			now = now.Add(hb)
			r.ObserveHeartbeat(1, now)
			r.ObserveHeartbeat(1, now.Add(13*time.Millisecond)) // read heartbeat
			r.ObserveHeartbeat(1, now.Add(61*time.Millisecond)) // another
		}
		r.SampleNow()
	}
	if h := r.SignalHealth()["jitter"]; h < 0.99 {
		t.Fatalf("jitter health %v with extra read heartbeats, want ~1", h)
	}
	if r.Critical() {
		t.Fatal("Critical() from extra read heartbeats")
	}
}

// Late heartbeats (a bad link): a 180 ms gap is 80 ms late -> mostly bad.
func TestJitter_LateHeartbeatsAreJitter(t *testing.T) {
	r := jitterRunner(t)
	now := time.Unix(0, 0)
	r.ObserveHeartbeat(1, now)
	for p := 0; p < 20; p++ {
		now = now.Add(180 * time.Millisecond)
		r.ObserveHeartbeat(1, now)
		now = now.Add(hb)
		r.ObserveHeartbeat(1, now)
		r.SampleNow()
	}
	if h := r.SignalHealth()["jitter"]; h > 0.3 {
		t.Fatalf("jitter health %v with 80 ms-late heartbeats, want low", h)
	}
}

// A heartbeat from a different sender (new leader) starts a new series:
// the gap across the leader change is not jitter.
func TestJitter_LeaderChangeIsNotJitter(t *testing.T) {
	r := jitterRunner(t)
	now := time.Unix(0, 0)
	for i := 0; i < 5; i++ {
		now = now.Add(hb)
		r.ObserveHeartbeat(1, now)
	}
	now = now.Add(350 * time.Millisecond) // failover gap
	for i := 0; i < 5; i++ {
		r.ObserveHeartbeat(2, now)
		now = now.Add(hb)
	}
	r.SampleNow()
	r.SampleNow()
	if h := r.SignalHealth()["jitter"]; h < 0.99 {
		t.Fatalf("jitter health %v after a leader change, want ~1", h)
	}
}

// A period with no heartbeat gaps (this node is now leader, or has no
// leader) clears the jitter alarm: a new leader must not inherit a
// follower-time alarm (2026-09-28 alarm check: two false graceful
// handovers right after a node became leader).
func TestJitter_EmptyPeriodClearsJitterAlarm(t *testing.T) {
	r := jitterRunner(t)
	now := time.Unix(0, 0)
	r.ObserveHeartbeat(1, now)
	for p := 0; p < 3; p++ {
		now = now.Add(200 * time.Millisecond)
		r.ObserveHeartbeat(1, now)
		r.SampleNow()
	}
	if !r.Critical() {
		t.Fatal("setup: jitter alarm not raised")
	}
	r.SampleNow() // became leader: no heartbeats this period
	if r.Critical() {
		t.Fatal("jitter alarm survived a period with no heartbeats")
	}
}

// ...while fsync keeps the no-evidence rule (sparse fsyncs are normal).
func TestJitter_EmptyPeriodDoesNotClearFsyncAlarm(t *testing.T) {
	r := jitterRunner(t)
	badFsyncPeriod(r)
	badFsyncPeriod(r)
	r.SampleNow()
	if !r.Critical() {
		t.Fatal("fsync alarm cleared by a period with no fsync")
	}
}
