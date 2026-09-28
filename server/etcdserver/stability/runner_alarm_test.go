package stability

import (
	"testing"
	"time"
)

// Critical alarm from raw per-period values (2026-09-28). The smoothed
// EWMA took ~10 periods (~5 s) to fall from healthy to critical, so a slow-
// disk leader handed over only after 6.8 s (smoke batch 20260928_220425).
// Critical() now fires when any signal's RAW period value is at or below
// CriticalLevel for criticalPeriods (2) consecutive periods with evidence:
// 1 s of consistent evidence, and one bad period alone can never fire it.
// The smoothed score (heir ranking) is unchanged.

func badFsyncPeriod(r *Runner) {
	for i := 0; i < 9; i++ {
		r.ObserveFsync(80 * time.Millisecond)
	}
	r.ObserveFsync(2 * time.Millisecond) // the odd fast one: mean still 72 ms
	r.SampleNow()
}

func goodFsyncPeriod(r *Runner) {
	r.ObserveFsync(3 * time.Millisecond)
	r.SampleNow()
}

func TestAlarm_TwoConsecutiveBadPeriods(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	badFsyncPeriod(r)
	if r.Critical() {
		t.Fatal("Critical() after ONE bad period, want false (needs 2 consecutive)")
	}
	badFsyncPeriod(r)
	if !r.Critical() {
		t.Fatal("Critical() = false after 2 consecutive bad periods")
	}
}

func TestAlarm_GoodPeriodBreaksRun(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	badFsyncPeriod(r)
	goodFsyncPeriod(r)
	badFsyncPeriod(r)
	if r.Critical() {
		t.Fatal("Critical() with bad, good, bad: want false")
	}
}

func TestAlarm_ClearsAfterOneGoodPeriod(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 5; i++ {
		badFsyncPeriod(r)
	}
	goodFsyncPeriod(r)
	if r.Critical() {
		t.Fatal("Critical() still true after a healthy period")
	}
}

// No fsync in a period is no evidence either way: the run neither grows nor
// resets.
func TestAlarm_EmptyPeriodIsNoEvidence(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	badFsyncPeriod(r)
	r.SampleNow() // no fsync this period
	if r.Critical() {
		t.Fatal("empty period counted as a bad one")
	}
	badFsyncPeriod(r)
	if !r.Critical() {
		t.Fatal("empty period reset the run: bad, empty, bad should be 2 consecutive bad periods with evidence")
	}
}

// Moderately slow (half-bad) fsync never raises the alarm: it only lowers
// the score.
func TestAlarm_HalfBadIsNotCritical(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	for i := 0; i < 10; i++ {
		r.ObserveFsync(30 * time.Millisecond) // health 0.5 with bounds 10-50 ms
		r.SampleNow()
	}
	if r.Critical() {
		t.Fatal("Critical() with fsync at 30 ms")
	}
	if r.Score() == 255 {
		t.Fatal("30 ms fsync did not lower the score at all")
	}
}

// The alarm reacts long before the smoothed score is fully bad: after 2
// periods the EWMA fsync health is still ~0.64.
func TestAlarm_FasterThanEWMA(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds()})
	badFsyncPeriod(r)
	badFsyncPeriod(r)
	if !r.Critical() {
		t.Fatal("alarm not raised after 2 bad periods")
	}
	if h := r.SignalHealth()["fsync"]; h < 0.5 {
		t.Fatalf("smoothed fsync health %v after 2 periods: the score must stay smooth (alpha 0.2)", h)
	}
}

// Jitter uses the same rule.
func TestAlarm_Jitter(t *testing.T) {
	r := newTestRunner(t, Config{Bounds: DefaultBounds(), HeartbeatInterval: 100 * time.Millisecond})
	now := time.Unix(0, 0)
	r.ObserveHeartbeat(1, now)
	for p := 0; p < 2; p++ {
		for i := 0; i < 5; i++ {
			if i%2 == 0 {
				now = now.Add(0)
			} else {
				now = now.Add(200 * time.Millisecond)
			}
			r.ObserveHeartbeat(1, now)
		}
		r.SampleNow()
	}
	if !r.Critical() {
		t.Fatal("alarm not raised after 2 periods of 100 ms heartbeat jitter")
	}
}
