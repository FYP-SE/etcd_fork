// DESIGN_UPDATE.md D7: CPU *pressure* (time runnable-but-waiting), not CPU
// usage. The v1 usage signal read etcd's own cgroup usage, which a
// stress-ng sidecar competing on the same cpuset never raised; pressure
// does, because etcd's tasks then wait for the CPU.
package stability

import (
	"testing"
	"time"
)

func pressureFile(total uint64) string {
	return "some avg10=1.00 avg60=0.50 avg300=0.10 total=" + itoa(total) + "\n" +
		"full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

func TestPressureSampler_FirstCallHasNoHistory(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cgroup/cpu.pressure", pressureFile(1_000_000))
	s := newPressureSampler(dir+"/cgroup", dir+"/proc")
	if _, err := s.Sample(); err == nil {
		t.Fatal("first Sample() succeeded, want no-history error")
	}
}

func TestPressureSampler_StallFractionFromTotalDelta(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cgroup/cpu.pressure", pressureFile(1_000_000))
	s := newPressureSampler(dir+"/cgroup", dir+"/proc")
	now := time.Unix(100, 0)
	s.now = func() time.Time { return now }
	s.Sample()
	// 500 ms of stall in 1 s of wall time -> 0.5.
	writeFile(t, dir, "cgroup/cpu.pressure", pressureFile(1_500_000))
	now = now.Add(time.Second)
	got, err := s.Sample()
	if err != nil {
		t.Fatal(err)
	}
	if got < 0.499 || got > 0.501 {
		t.Fatalf("Sample() = %v, want 0.5", got)
	}
}

func TestPressureSampler_ClampsToOne(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cgroup/cpu.pressure", pressureFile(0))
	s := newPressureSampler(dir+"/cgroup", dir+"/proc")
	now := time.Unix(100, 0)
	s.now = func() time.Time { return now }
	s.Sample()
	writeFile(t, dir, "cgroup/cpu.pressure", pressureFile(3_000_000)) // 3 s stall in 1 s (many tasks)
	now = now.Add(time.Second)
	if got, _ := s.Sample(); got != 1 {
		t.Fatalf("Sample() = %v, want clamped to 1", got)
	}
}

func TestPressureSampler_FallsBackToHostPSI(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "proc/pressure/cpu", pressureFile(0))
	s := newPressureSampler(dir+"/cgroup", dir+"/proc")
	now := time.Unix(100, 0)
	s.now = func() time.Time { return now }
	s.Sample()
	writeFile(t, dir, "proc/pressure/cpu", pressureFile(250_000))
	now = now.Add(time.Second)
	got, err := s.Sample()
	if err != nil {
		t.Fatal(err)
	}
	if got < 0.249 || got > 0.251 {
		t.Fatalf("Sample() = %v, want 0.25 from /proc/pressure/cpu", got)
	}
}

func TestPressureSampler_NoSource(t *testing.T) {
	dir := t.TempDir()
	s := newPressureSampler(dir+"/cgroup", dir+"/proc")
	if _, err := s.Sample(); err == nil {
		t.Fatal("Sample() with no PSI file succeeded")
	}
}
