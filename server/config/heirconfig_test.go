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
	got, err := ParseHeirTunables("freshness-slack=2,heir-sync-grace=15,hysteresis-margin=30,min-heir-tenure=5," +
		"heir-jitter=0.15,non-heir-backoff=2.0," +
		"handover-threshold=180,degrade-window=4,handover-cooldown=3")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	want := HeirTunables{
		FreshnessSlack:    2,
		HeirSyncGrace:     15,
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
	got, err := ParseHeirTunables(" freshness-slack = 3 , min-heir-tenure = 3 ")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	want := HeirTunables{FreshnessSlack: 3, MinHeirTenure: 3}
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
	for _, in := range []string{"freshness-slack", "freshness-slack=", "=5", "freshness-slack=5=6", ","} {
		if _, err := ParseHeirTunables(in); err == nil {
			t.Fatalf("ParseHeirTunables(%q): expected error, got nil", in)
		}
	}
}

func TestParseHeirTunables_NonNumericValueErrors(t *testing.T) {
	if _, err := ParseHeirTunables("freshness-slack=not-a-number"); err == nil {
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
	if _, err := ParseHeirTunables("hysteresis-margin=-1"); err == nil {
		t.Fatal("expected error for negative value on a uint8 field, got nil")
	}
}

func TestParseHeirTunables_DuplicateKeyErrors(t *testing.T) {
	if _, err := ParseHeirTunables("freshness-slack=1,freshness-slack=2"); err == nil {
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

// freshness-slack=0 is a real setting (strict: match the freshest follower),
// but raft.Config reads a zero FreshnessSlack as "default 1", so the parser
// maps an explicit 0 to raft.FreshnessSlackStrict (-1). DESIGN_UPDATE.md D5.
func TestParseHeirTunables_FreshnessSlackZeroIsStrict(t *testing.T) {
	got, err := ParseHeirTunables("freshness-slack=0")
	if err != nil {
		t.Fatalf("ParseHeirTunables: %v", err)
	}
	if got.FreshnessSlack != -1 {
		t.Fatalf("FreshnessSlack = %d, want -1 (raft.FreshnessSlackStrict)", got.FreshnessSlack)
	}
}

func TestParseHeirTunables_NegativeSlackAndGraceRejected(t *testing.T) {
	for _, in := range []string{"freshness-slack=-1", "heir-sync-grace=-1"} {
		if _, err := ParseHeirTunables(in); err == nil {
			t.Errorf("ParseHeirTunables(%q) succeeded, want error", in)
		}
	}
}

// max-heir-lag was removed with raft's MaxHeirLag (DESIGN_UPDATE.md D5).
func TestParseHeirTunables_RejectsRemovedMaxHeirLag(t *testing.T) {
	if _, err := ParseHeirTunables("max-heir-lag=256"); err == nil {
		t.Fatal("ParseHeirTunables(max-heir-lag=256) succeeded, want unrecognised-key error")
	}
}
