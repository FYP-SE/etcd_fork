package stability

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// pressureSampler reports CPU pressure (DESIGN_UPDATE.md D7): the fraction
// of wall time in which at least one of this cgroup's tasks was runnable
// but waiting for a CPU ("some" line of Linux PSI). Unlike CPU usage, this
// rises when another process on the same CPUs (e.g. a stress-ng sidecar)
// crowds etcd out. It uses the cumulative "total" counter (microseconds)
// rather than avg10, so it reacts within one sampling period instead of a
// 10 s window.
type pressureSampler struct {
	cgroupRoot string
	procRoot   string
	now        func() time.Time

	havePrev  bool
	prevWall  time.Time
	prevStall uint64
}

func newPressureSampler(cgroupRoot, procRoot string) *pressureSampler {
	return &pressureSampler{cgroupRoot: cgroupRoot, procRoot: procRoot, now: time.Now}
}

// NewCPUPressureSampler returns a CPUSampler reading this cgroup's
// cpu.pressure, falling back to host-wide /proc/pressure/cpu.
func NewCPUPressureSampler() CPUSampler {
	return newPressureSampler("/sys/fs/cgroup", "/proc")
}

var errNoPressureHistory = errors.New("stability: first CPU pressure sample, no history yet")

func (s *pressureSampler) Sample() (float64, error) {
	now := s.now()
	stall, err := s.readStallUsec()
	if err != nil {
		return 0, err
	}
	if !s.havePrev {
		s.havePrev, s.prevWall, s.prevStall = true, now, stall
		return 0, errNoPressureHistory
	}
	wall := now.Sub(s.prevWall)
	delta := stall - s.prevStall
	if stall < s.prevStall { // counter reset
		delta = 0
	}
	s.prevWall, s.prevStall = now, stall
	if wall <= 0 {
		return 0, fmt.Errorf("stability: non-positive CPU pressure sampling interval %v", wall)
	}
	return clamp01(float64(delta) / float64(wall.Microseconds())), nil
}

func (s *pressureSampler) readStallUsec() (uint64, error) {
	for _, p := range []string{
		filepath.Join(s.cgroupRoot, "cpu.pressure"),
		filepath.Join(s.procRoot, "pressure", "cpu"),
	} {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		if v, ok := parseSomeTotal(string(data)); ok {
			return v, nil
		}
	}
	return 0, fmt.Errorf("stability: no CPU pressure (PSI) source under %q or %q", s.cgroupRoot, s.procRoot)
}

// parseSomeTotal extracts total= from the "some" line of a PSI file.
func parseSomeTotal(data string) (uint64, bool) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, f := range fields[1:] {
			if v, ok := strings.CutPrefix(f, "total="); ok {
				n, err := strconv.ParseUint(v, 10, 64)
				return n, err == nil
			}
		}
	}
	return 0, false
}
