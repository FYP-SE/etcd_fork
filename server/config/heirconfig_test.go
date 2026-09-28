// T5.2 (TASKS.md): parser for the --experimental-heir-config=<k=v,...> flag
// (etcd_fork branch heirraft/integration). Written before heirconfig.go per
// research-plan-docs/CLAUDE.md's TDD convention. This parser has zero
// dependency on go.etcd.io/raft/v3 -- it produces a plain HeirTunables
// struct that etcdserver/bootstrap.go's raftConfig() applies onto
// raft.Config fields by name, keeping the config package raft-free (matches
// this file's own package: no existing file here imports raft).
package config

import (
	"testing"
)

func TestParseHeirTunables_EmptyStringIsAllZero(t *testing.T) {
	got, err := ParseHeirTunables("")
	if err != nil {
		t.Fatalf("ParseHeirTunables(\"\"): %v", err)
	}
	want := HeirTunables{}
	if got != want {
		t.Fatalf("got %+v, want zero value %+v", got, want)
	}
}

func TestParseHeirTunables_AllKeysRecognised(t *testing.T) {
	got, err := ParseHeirTunables("max-heir-lag=512,hysteresis-margin=30,min-heir-tenure=5," +
		"heir-jitter=0.15,non-heir-backoff=2.0," +
		"handover-threshold=180,degrade-window=4,handover-cooldown=3")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	want := HeirTunables{
		MaxHeirLag:        512,
		HysteresisMargin:  30,
		MinHeirTenure:     5,
		HeirJitter:        0.15,
		NonHeirBackoff:    2.0,
		HandoverThreshold: 180,
		DegradeWindow:     4,
		HandoverCooldown:  3,
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseHeirTunables_PartialSetLeavesRestZero(t *testing.T) {
	got, err := ParseHeirTunables("hysteresis-margin=40")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	want := HeirTunables{HysteresisMargin: 40}
	if got != want {
		t.Fatalf("got %+v, want %+v (rest zero -- raft.Config.validate() fills its own defaults for zero fields)", got, want)
	}
}

func TestParseHeirTunables_IgnoresWhitespaceAroundPairs(t *testing.T) {
	got, err := ParseHeirTunables(" max-heir-lag = 100 , min-heir-tenure = 3 ")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	want := HeirTunables{MaxHeirLag: 100, MinHeirTenure: 3}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseHeirTunables_UnknownKeyErrors(t *testing.T) {
	if _, err := ParseHeirTunables("not-a-real-key=1"); err == nil {
		t.Fatal("expected error for unrecognised key, got nil")
	}
}

func TestParseHeirTunables_MalformedPairErrors(t *testing.T) {
	for _, in := range []string{"max-heir-lag", "max-heir-lag=", "=5", "max-heir-lag=5=6", ","} {
		if _, err := ParseHeirTunables(in); err == nil {
			t.Fatalf("ParseHeirTunables(%q): expected error, got nil", in)
		}
	}
}

func TestParseHeirTunables_NonNumericValueErrors(t *testing.T) {
	if _, err := ParseHeirTunables("max-heir-lag=not-a-number"); err == nil {
		t.Fatal("expected error for non-numeric value, got nil")
	}
}

func TestParseHeirTunables_ValueOutOfRangeForTypeErrors(t *testing.T) {
	// HysteresisMargin and HandoverThreshold are uint8 (raft.Config's own
	// types, DESIGN.md §5's score-out-of-255 fields) -- 256 overflows.
	if _, err := ParseHeirTunables("hysteresis-margin=256"); err == nil {
		t.Fatal("expected error for uint8 overflow, got nil")
	}
	if _, err := ParseHeirTunables("handover-threshold=999"); err == nil {
		t.Fatal("expected error for uint8 overflow, got nil")
	}
}

func TestParseHeirTunables_NegativeValueForUnsignedFieldErrors(t *testing.T) {
	if _, err := ParseHeirTunables("max-heir-lag=-1"); err == nil {
		t.Fatal("expected error for negative value on a uint64 field, got nil")
	}
}

func TestParseHeirTunables_DuplicateKeyErrors(t *testing.T) {
	if _, err := ParseHeirTunables("max-heir-lag=1,max-heir-lag=2"); err == nil {
		t.Fatal("expected error for duplicate key, got nil")
	}
}

// heir-staleness was removed with raft's HeirStaleness (DESIGN_UPDATE.md D4:
// followers keep the heir for the whole term). An old config naming it must
// fail loudly, not silently run without the expiry it asked for.
func TestParseHeirTunables_RejectsRemovedHeirStaleness(t *testing.T) {
	if _, err := ParseHeirTunables("heir-staleness=4"); err == nil {
		t.Fatal("ParseHeirTunables(heir-staleness=4) succeeded, want unrecognised-key error")
	}
}
