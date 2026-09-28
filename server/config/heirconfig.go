package config

import (
	"fmt"
	"strconv"
	"strings"
)

// HeirTunables mirrors go.etcd.io/raft/v3.Config's DESIGN.md §5 tunables by
// name and type. Zero fields are left for raft.Config.validate() to fill
// with its own defaults -- this parser never invents defaults of its own,
// so a partially-specified --experimental-heir-config only overrides what
// the operator actually named.
type HeirTunables struct {
	FreshnessSlack    int
	HeirSyncGrace     int
	HeirTimeout       int
	HysteresisMargin  uint8
	MinHeirTenure     int
	HeirJitter        float64
	NonHeirBackoff    float64
	HandoverThreshold uint8
	DegradeWindow     int
	HandoverCooldown  int
}

// ParseHeirTunables parses the --experimental-heir-config=<k=v,...> flag
// value into a HeirTunables. An empty string yields the zero value (every
// tunable left to raft.Config's own defaults). Unknown keys, malformed
// pairs, non-numeric or out-of-range values, and duplicate keys are all
// reported as errors rather than silently ignored -- an operator's typo in
// a k=v tunable string should fail loudly at startup, not silently no-op.
func ParseHeirTunables(s string) (HeirTunables, error) {
	var t HeirTunables
	if strings.TrimSpace(s) == "" {
		return t, nil
	}

	seen := make(map[string]bool)
	for _, pair := range strings.Split(s, ",") {
		key, value, err := splitPair(pair)
		if err != nil {
			return HeirTunables{}, err
		}
		if seen[key] {
			return HeirTunables{}, fmt.Errorf("experimental-heir-config: duplicate key %q", key)
		}
		seen[key] = true

		switch key {
		case "freshness-slack":
			// DESIGN_UPDATE.md D5. raft.Config reads 0 as "default 1", so an
			// explicit 0 (strict) maps to raft.FreshnessSlackStrict (-1).
			v, err := parseNonNegInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			if v == 0 {
				v = -1
			}
			t.FreshnessSlack = v
		case "heir-sync-grace":
			// DESIGN_UPDATE.md D6, in ticks.
			v, err := parseNonNegInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HeirSyncGrace = v
		case "heir-timeout":
			// DESIGN_UPDATE.md D1, in ticks; raft.Config validates the
			// range (2 <= H < ElectionTick, H > HeartbeatTick).
			v, err := parseNonNegInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HeirTimeout = v
		case "hysteresis-margin":
			v, err := parseUint(key, value, 8)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HysteresisMargin = uint8(v)
		case "min-heir-tenure":
			v, err := parseInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.MinHeirTenure = v
		case "heir-jitter":
			v, err := parseFloat(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HeirJitter = v
		case "non-heir-backoff":
			v, err := parseFloat(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.NonHeirBackoff = v
		case "handover-threshold":
			v, err := parseUint(key, value, 8)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HandoverThreshold = uint8(v)
		case "degrade-window":
			v, err := parseInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.DegradeWindow = v
		case "handover-cooldown":
			v, err := parseInt(key, value)
			if err != nil {
				return HeirTunables{}, err
			}
			t.HandoverCooldown = v
		default:
			return HeirTunables{}, fmt.Errorf("experimental-heir-config: unrecognised key %q", key)
		}
	}
	return t, nil
}

func splitPair(pair string) (key, value string, err error) {
	parts := strings.Split(pair, "=")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("experimental-heir-config: malformed key=value pair %q", pair)
	}
	key = strings.TrimSpace(parts[0])
	value = strings.TrimSpace(parts[1])
	if key == "" || value == "" {
		return "", "", fmt.Errorf("experimental-heir-config: malformed key=value pair %q", pair)
	}
	return key, value, nil
}

func parseUint(key, value string, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return 0, fmt.Errorf("experimental-heir-config: %s=%q: %w", key, value, err)
	}
	return v, nil
}

func parseNonNegInt(key, value string) (int, error) {
	v, err := parseInt(key, value)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("experimental-heir-config: %s=%q: must be >= 0", key, value)
	}
	return v, nil
}

func parseInt(key, value string) (int, error) {
	v, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("experimental-heir-config: %s=%q: %w", key, value, err)
	}
	return v, nil
}

func parseFloat(key, value string) (float64, error) {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("experimental-heir-config: %s=%q: %w", key, value, err)
	}
	return v, nil
}
