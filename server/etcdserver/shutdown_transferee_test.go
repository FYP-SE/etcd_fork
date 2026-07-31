// T5.2 (TASKS.md, etcd_fork branch heirraft/integration): shutdown
// transferee selection. Written before pickShutdownTransferee/
// shutdownTransferee landed in server.go, per the project's TDD convention.
// pickShutdownTransferee is the pure decision logic factored out of
// shutdownTransferee specifically so it's testable without a full running
// EtcdServer (raftStatus/cluster/transport all need real I/O-backed state).
package etcdserver

import (
	"testing"

	"go.etcd.io/etcd/client/pkg/v3/types"
)

func alwaysVotingMember(types.ID) bool { return true }
func neverVotingMember(types.ID) bool  { return false }
func alwaysConnected(types.ID) bool    { return true }
func neverConnected(types.ID) bool     { return false }

func fallbackTo(id types.ID, ok bool) func() (types.ID, bool) {
	return func() (types.ID, bool) { return id, ok }
}

func TestPickShutdownTransferee_PrefersHeirWhenHeirRaftEnabled(t *testing.T) {
	heir := types.ID(2)
	got, ok := pickShutdownTransferee(true, heir, alwaysVotingMember, alwaysConnected, fallbackTo(types.ID(3), true))
	if !ok || got != heir {
		t.Fatalf("got (%v, %v), want (%v, true) -- heir should be preferred", got, ok, heir)
	}
}

func TestPickShutdownTransferee_FallsBackWhenHeirRaftDisabled(t *testing.T) {
	heir := types.ID(2)
	fallback := types.ID(3)
	got, ok := pickShutdownTransferee(false, heir, alwaysVotingMember, alwaysConnected, fallbackTo(fallback, true))
	if !ok || got != fallback {
		t.Fatalf("got (%v, %v), want (%v, true) -- HeirRaft disabled must ignore heir entirely", got, ok, fallback)
	}
}

func TestPickShutdownTransferee_FallsBackWhenNoHeirDesignated(t *testing.T) {
	fallback := types.ID(3)
	got, ok := pickShutdownTransferee(true, types.ID(0), alwaysVotingMember, alwaysConnected, fallbackTo(fallback, true))
	if !ok || got != fallback {
		t.Fatalf("got (%v, %v), want (%v, true) -- heir=None (0) must fall back", got, ok, fallback)
	}
}

func TestPickShutdownTransferee_FallsBackWhenHeirNotVotingMember(t *testing.T) {
	// A learner (or a member that's since left the cluster) can't receive
	// leadership -- must fall back even though a heir value is set.
	fallback := types.ID(3)
	got, ok := pickShutdownTransferee(true, types.ID(2), neverVotingMember, alwaysConnected, fallbackTo(fallback, true))
	if !ok || got != fallback {
		t.Fatalf("got (%v, %v), want (%v, true) -- non-voting heir must fall back", got, ok, fallback)
	}
}

func TestPickShutdownTransferee_FallsBackWhenHeirNotConnected(t *testing.T) {
	// A stale heir announcement (DESIGN.md §2.4 staleness bound) shouldn't
	// be trusted for an actual transfer if the transport reports it's not
	// currently reachable.
	fallback := types.ID(3)
	got, ok := pickShutdownTransferee(true, types.ID(2), alwaysVotingMember, neverConnected, fallbackTo(fallback, true))
	if !ok || got != fallback {
		t.Fatalf("got (%v, %v), want (%v, true) -- disconnected heir must fall back", got, ok, fallback)
	}
}

func TestPickShutdownTransferee_PropagatesFallbackFailure(t *testing.T) {
	// HeirRaft disabled and the fallback itself finds nothing healthy --
	// must propagate ok=false, not silently invent a transferee.
	got, ok := pickShutdownTransferee(false, types.ID(2), alwaysVotingMember, alwaysConnected, fallbackTo(types.ID(0), false))
	if ok {
		t.Fatalf("got (%v, %v), want ok=false when fallback itself fails", got, ok)
	}
}
