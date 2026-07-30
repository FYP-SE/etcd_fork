package stability

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// procCPUSampler implements CPUSampler by preferring cgroup v2 CPU
// accounting (cpu.stat's usage_usec against cpu.max's quota, matching the
// container cgroup the real testbed runs etcd in -- see
// experiments/ENVIRONMENT.md), falling back to host-wide /proc/stat when
// those files aren't present or the quota is unlimited (e.g. a bare-metal
// dev run, or cgroup v1).
type procCPUSampler struct {
	cgroupRoot string
	procRoot   string
	now        func() time.Time
	numCPU     int

	havePrev bool
	prevWall time.Time
	prevUsec uint64 // cgroup usage_usec, or (user+system) proc/stat jiffies scaled to usec
	prevIdle uint64 // /proc/stat idle jiffies (fallback path only)
}

func newProcCPUSampler(cgroupRoot, procRoot string) *procCPUSampler {
	return &procCPUSampler{
		cgroupRoot: cgroupRoot,
		procRoot:   procRoot,
		now:        time.Now,
		numCPU:     runtime.NumCPU(),
	}
}

// NewCPUSampler returns a CPUSampler reading the real host's cgroupfs/procfs.
func NewCPUSampler() CPUSampler {
	return newProcCPUSampler("/sys/fs/cgroup", "/proc")
}

// Sample implements CPUSampler. The first call always errors: a busy
// fraction requires two snapshots to diff against.
func (s *procCPUSampler) Sample() (float64, error) {
	now := s.now()

	if usec, quota, ok := s.readCgroup(); ok {
		return s.diff(now, usec, quota)
	}
	if usec, idle, ok := s.readProcStat(); ok {
		return s.diffProcStat(now, usec, idle)
	}
	return 0, fmt.Errorf("stability: no CPU accounting source available under %q or %q", s.cgroupRoot, s.procRoot)
}

// readCgroup reads cpu.stat's usage_usec and cpu.max's quota (CPUs
// allotted, e.g. 1.0 for "100000 100000"; 0 means unlimited). ok is false if
// the files aren't present (no cgroup v2, or wrong path).
func (s *procCPUSampler) readCgroup() (usageUsec uint64, quotaCPUs float64, ok bool) {
	statBytes, err := os.ReadFile(s.cgroupRoot + "/cpu.stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(statBytes), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			usageUsec, _ = strconv.ParseUint(fields[1], 10, 64)
		}
	}

	maxBytes, err := os.ReadFile(s.cgroupRoot + "/cpu.max")
	if err != nil {
		return usageUsec, 0, true // usage present, quota unknown -> treat as unlimited
	}
	fields := strings.Fields(string(maxBytes))
	if len(fields) >= 2 && fields[0] != "max" {
		quotaUsec, errQ := strconv.ParseFloat(fields[0], 64)
		periodUsec, errP := strconv.ParseFloat(fields[1], 64)
		if errQ == nil && errP == nil && periodUsec > 0 {
			quotaCPUs = quotaUsec / periodUsec
		}
	}
	return usageUsec, quotaCPUs, true
}

// readProcStat reads /proc/stat's aggregate "cpu" line: user, nice, system,
// idle, iowait, irq, softirq, steal, guest, guest_nice (jiffies). Returns
// busyJiffies (everything but idle+iowait) and idle (idle+iowait).
func (s *procCPUSampler) readProcStat() (busyJiffies, idleJiffies uint64, ok bool) {
	data, err := os.ReadFile(s.procRoot + "/stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		vals := make([]uint64, 0, len(fields)-1)
		for _, f := range fields[1:] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return 0, 0, false
			}
			vals = append(vals, v)
		}
		var total, idle uint64
		for i, v := range vals {
			total += v
			if i == 3 || i == 4 { // idle, iowait
				idle += v
			}
		}
		return total - idle, idle, true
	}
	return 0, 0, false
}

func (s *procCPUSampler) diff(now time.Time, usageUsec uint64, quotaCPUs float64) (float64, error) {
	defer func() { s.havePrev = true; s.prevWall = now; s.prevUsec = usageUsec }()
	if !s.havePrev {
		return 0, fmt.Errorf("stability: no prior CPU snapshot yet")
	}
	wallUsec := float64(now.Sub(s.prevWall).Microseconds())
	if wallUsec <= 0 {
		return 0, fmt.Errorf("stability: non-positive wall-clock delta since last CPU sample")
	}
	deltaUsec := float64(usageUsec - s.prevUsec)
	if usageUsec < s.prevUsec {
		deltaUsec = 0 // counter reset (e.g. cgroup recreated) -- treat as idle rather than underflow
	}

	capacity := quotaCPUs
	if capacity <= 0 {
		capacity = float64(s.numCPU)
	}
	frac := deltaUsec / (wallUsec * capacity)
	return clamp01(frac), nil
}

func (s *procCPUSampler) diffProcStat(now time.Time, busyJiffies, idleJiffies uint64) (float64, error) {
	usec := busyJiffies // jiffies aren't usec, but diff below only cares about relative units cancelling in the ratio
	defer func() { s.havePrev = true; s.prevWall = now; s.prevUsec = usec; s.prevIdle = idleJiffies }()
	if !s.havePrev {
		return 0, fmt.Errorf("stability: no prior CPU snapshot yet")
	}
	deltaBusy := busyJiffies - s.prevUsec
	deltaIdle := idleJiffies - s.prevIdle
	if busyJiffies < s.prevUsec || idleJiffies < s.prevIdle {
		return 0, fmt.Errorf("stability: /proc/stat counters went backwards (reset?)")
	}
	total := deltaBusy + deltaIdle
	if total == 0 {
		return 0, fmt.Errorf("stability: no /proc/stat progress since last sample")
	}
	return clamp01(float64(deltaBusy) / float64(total)), nil
}

func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

// procMemSampler implements MemSampler by preferring cgroup v2's
// memory.current/memory.max, falling back to host-wide /proc/meminfo when
// those files aren't present or the limit is unbounded ("max").
type procMemSampler struct {
	cgroupRoot string
	procRoot   string
}

func newProcMemSampler(cgroupRoot, procRoot string) *procMemSampler {
	return &procMemSampler{cgroupRoot: cgroupRoot, procRoot: procRoot}
}

// NewMemSampler returns a MemSampler reading the real host's cgroupfs/procfs.
func NewMemSampler() MemSampler {
	return newProcMemSampler("/sys/fs/cgroup", "/proc")
}

func (s *procMemSampler) Sample() (float64, error) {
	if v, ok := s.readCgroup(); ok {
		return v, nil
	}
	if v, ok := s.readMeminfo(); ok {
		return v, nil
	}
	return 0, fmt.Errorf("stability: no memory accounting source available under %q or %q", s.cgroupRoot, s.procRoot)
}

func (s *procMemSampler) readCgroup() (float64, bool) {
	curBytes, err := os.ReadFile(s.cgroupRoot + "/memory.current")
	if err != nil {
		return 0, false
	}
	current, err := strconv.ParseFloat(strings.TrimSpace(string(curBytes)), 64)
	if err != nil {
		return 0, false
	}

	maxBytes, err := os.ReadFile(s.cgroupRoot + "/memory.max")
	if err != nil {
		return 0, false
	}
	maxStr := strings.TrimSpace(string(maxBytes))
	if maxStr == "max" {
		return 0, false // unbounded -- fall back to host meminfo
	}
	limit, err := strconv.ParseFloat(maxStr, 64)
	if err != nil || limit <= 0 {
		return 0, false
	}
	return clamp01(current / limit), true
}

func (s *procMemSampler) readMeminfo() (float64, bool) {
	data, err := os.ReadFile(s.procRoot + "/meminfo")
	if err != nil {
		return 0, false
	}
	var total, available float64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total = v
		case "MemAvailable:":
			available = v
		}
	}
	if total <= 0 {
		return 0, false
	}
	return clamp01(1 - available/total), true
}
