// T5.1 (TASKS.md): real /proc + cgroupfs-backed CPUSampler/MemSampler,
// written before proc_sampler.go per the project's TDD convention. All
// tests point the samplers at fixture files under t.TempDir() (via the
// unexported procRoot/cgroupRoot/now overrides) rather than the real host's
// /proc or /sys/fs/cgroup, so results are deterministic across machines and
// CI, and don't depend on this dev host actually running inside a
// container's own cgroup v2 namespace (the real testbed does, per
// experiments/ENVIRONMENT.md; this dev sandbox does not, confirmed by
// direct inspection before writing this file).
package stability

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// --- cgroup v2 CPU ---------------------------------------------------------

func TestProcCPUSampler_CgroupV2_FirstCallReturnsErrNoHistory(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cpu.stat", "usage_usec 1000000\n")
	writeFile(t, dir, "cpu.max", "100000 100000\n")

	s := newProcCPUSampler(dir, dir)
	if _, err := s.Sample(); err == nil {
		t.Fatal("expected error on first call (no prior snapshot to diff against), got nil")
	}
}

func TestProcCPUSampler_CgroupV2_QuotaBound(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cpu.max", "100000 100000\n") // 1.0 CPU allotted
	writeFile(t, dir, "cpu.stat", "usage_usec 1000000\n")

	fakeNow := time.Unix(0, 0)
	s := newProcCPUSampler(dir, dir)
	s.now = func() time.Time { return fakeNow }

	s.Sample() // priming call: establishes baseline, errors by design (see FirstCallReturnsErrNoHistory)

	// Advance 1 wall second; cgroup used exactly its full 1.0 CPU quota
	// (1,000,000 usec of CPU time in 1,000,000 usec of wall time) -> busy=1.0.
	fakeNow = fakeNow.Add(time.Second)
	writeFile(t, dir, "cpu.stat", "usage_usec 2000000\n")
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("second Sample: %v", err)
	}
	if diff := got - 1.0; diff < -0.01 || diff > 0.01 {
		t.Fatalf("busy fraction = %v, want ~1.0 (used exactly its quota)", got)
	}
}

func TestProcCPUSampler_CgroupV2_HalfQuotaUsed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cpu.max", "100000 100000\n") // 1.0 CPU allotted
	writeFile(t, dir, "cpu.stat", "usage_usec 0\n")

	fakeNow := time.Unix(0, 0)
	s := newProcCPUSampler(dir, dir)
	s.now = func() time.Time { return fakeNow }
	s.Sample() // priming call

	fakeNow = fakeNow.Add(time.Second)
	writeFile(t, dir, "cpu.stat", "usage_usec 500000\n") // half the 1.0 quota used
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("second Sample: %v", err)
	}
	if diff := got - 0.5; diff < -0.01 || diff > 0.01 {
		t.Fatalf("busy fraction = %v, want ~0.5", got)
	}
}

func TestProcCPUSampler_CgroupV2_ClampsAboveQuota(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cpu.max", "100000 100000\n")
	writeFile(t, dir, "cpu.stat", "usage_usec 0\n")

	fakeNow := time.Unix(0, 0)
	s := newProcCPUSampler(dir, dir)
	s.now = func() time.Time { return fakeNow }
	s.Sample() // priming call

	fakeNow = fakeNow.Add(time.Second)
	// Burst above the allotted quota (bursting is legal in cgroup v2 within
	// a period accounting window) -- fraction must still clamp to 1.0.
	writeFile(t, dir, "cpu.stat", "usage_usec 3000000\n")
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("second Sample: %v", err)
	}
	if got != 1.0 {
		t.Fatalf("busy fraction = %v, want clamped to 1.0", got)
	}
}

func TestProcCPUSampler_CgroupV2_UnlimitedQuotaFallsBackToNumCPU(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cpu.max", "max 100000\n") // unlimited quota
	writeFile(t, dir, "cpu.stat", "usage_usec 0\n")

	fakeNow := time.Unix(0, 0)
	s := newProcCPUSampler(dir, dir)
	s.now = func() time.Time { return fakeNow }
	s.numCPU = 2 // pin, instead of depending on the test host's real core count
	s.Sample()   // priming call

	fakeNow = fakeNow.Add(time.Second)
	// 1,000,000 usec of CPU time used in 1s wall time across 2 cores -> 0.5 busy.
	writeFile(t, dir, "cpu.stat", "usage_usec 1000000\n")
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("second Sample: %v", err)
	}
	if diff := got - 0.5; diff < -0.01 || diff > 0.01 {
		t.Fatalf("busy fraction = %v, want ~0.5 (1 of 2 cores)", got)
	}
}

// --- /proc/stat fallback (no cgroup v2 files present) ----------------------

func TestProcCPUSampler_FallsBackToProcStatWhenNoCgroup(t *testing.T) {
	cgroupDir := t.TempDir() // deliberately empty: no cpu.stat/cpu.max
	procDir := t.TempDir()
	// /proc/stat "cpu" line: user nice system idle iowait irq softirq ...
	writeFile(t, procDir, "stat", "cpu  100 0 100 800 0 0 0 0 0 0\n")

	fakeNow := time.Unix(0, 0)
	s := newProcCPUSampler(cgroupDir, procDir)
	s.now = func() time.Time { return fakeNow }
	s.Sample() // priming call

	fakeNow = fakeNow.Add(time.Second)
	// total += 200 (100 user + 100 system), idle += 0 -> busy fraction = 1.0
	writeFile(t, procDir, "stat", "cpu  200 0 200 800 0 0 0 0 0 0\n")
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("second Sample: %v", err)
	}
	if diff := got - 1.0; diff < -0.01 || diff > 0.01 {
		t.Fatalf("busy fraction = %v, want ~1.0", got)
	}
}

func TestProcCPUSampler_ErrorsWhenNeitherSourceAvailable(t *testing.T) {
	emptyDir := t.TempDir()
	s := newProcCPUSampler(emptyDir, emptyDir)
	if _, err := s.Sample(); err == nil {
		t.Fatal("expected error when neither cgroup nor /proc/stat files exist, got nil")
	}
}

// --- Memory ------------------------------------------------------------

func TestProcMemSampler_CgroupV2_UsesCurrentOverMax(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "memory.current", "500000000\n") // 500MB
	writeFile(t, dir, "memory.max", "1000000000\n")    // 1GB limit

	s := newProcMemSampler(dir, dir)
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if diff := got - 0.5; diff < -0.001 || diff > 0.001 {
		t.Fatalf("pressure = %v, want 0.5", got)
	}
}

func TestProcMemSampler_CgroupV2_UnlimitedMaxFallsBackToMeminfo(t *testing.T) {
	cgroupDir := t.TempDir()
	writeFile(t, cgroupDir, "memory.current", "500000000\n")
	writeFile(t, cgroupDir, "memory.max", "max\n") // unlimited

	procDir := t.TempDir()
	writeFile(t, procDir, "meminfo", "MemTotal:       1000000 kB\nMemAvailable:    250000 kB\n")

	s := newProcMemSampler(cgroupDir, procDir)
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	// pressure = 1 - available/total = 1 - 0.25 = 0.75
	if diff := got - 0.75; diff < -0.001 || diff > 0.001 {
		t.Fatalf("pressure = %v, want 0.75", got)
	}
}

func TestProcMemSampler_FallsBackToMeminfoWhenNoCgroup(t *testing.T) {
	cgroupDir := t.TempDir() // empty
	procDir := t.TempDir()
	writeFile(t, procDir, "meminfo", "MemTotal:       2000000 kB\nMemAvailable:   2000000 kB\n")

	s := newProcMemSampler(cgroupDir, procDir)
	got, err := s.Sample()
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if got != 0 {
		t.Fatalf("pressure = %v, want 0 (all memory available)", got)
	}
}

func TestProcMemSampler_ErrorsWhenNeitherSourceAvailable(t *testing.T) {
	emptyDir := t.TempDir()
	s := newProcMemSampler(emptyDir, emptyDir)
	if _, err := s.Sample(); err == nil {
		t.Fatal("expected error when neither cgroup nor /proc/meminfo exist, got nil")
	}
}

// --- Constructors default to the real host paths ----------------------

func TestNewCPUMemSamplers_DefaultToRealHostPaths(t *testing.T) {
	cpu := NewCPUSampler()
	mem := NewMemSampler()
	if cpu == nil || mem == nil {
		t.Fatal("NewCPUSampler/NewMemSampler must never return nil")
	}
	// Not asserting on Sample() output here (host-dependent, and the first
	// CPU call always errors by design -- see FirstCallReturnsErrNoHistory)
	// -- just that construction against the real paths doesn't panic.
}
