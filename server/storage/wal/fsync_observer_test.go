// T5.2 (TASKS.md, etcd_fork branch heirraft/integration): WAL fsync
// observer hook, the mechanism server/etcdserver/stability.Runner's
// ObserveFsync feeds off. nil by default (no HeirRaft flags), so every
// pre-existing WAL test is unaffected -- this file only tests the new hook
// in isolation.
package wal

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/raft/v3/raftpb"
)

func TestWAL_SetFsyncObserver_CalledOnSave(t *testing.T) {
	p := t.TempDir()
	w, err := Create(zaptest.NewLogger(t), p, []byte("data"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer w.Close()

	var mu sync.Mutex
	var calls int
	var lastDur time.Duration
	w.SetFsyncObserver(func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		lastDur = d
	})

	state := &raftpb.HardState{Term: new(uint64(1))}
	ents := []*raftpb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("x")}}
	if err := w.Save(state, ents); err != nil {
		t.Fatalf("Save: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("fsync observer was never called after Save")
	}
	if lastDur < 0 {
		t.Fatalf("observed duration = %v, want >= 0", lastDur)
	}
}

func TestWAL_NilFsyncObserverIsDefaultAndSafe(t *testing.T) {
	p := t.TempDir()
	w, err := Create(zaptest.NewLogger(t), p, []byte("data"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer w.Close()

	// No SetFsyncObserver call at all -- must not panic, byte-identical to
	// stock behaviour (this is the "flags off" accept criterion at the WAL
	// layer).
	state := &raftpb.HardState{Term: new(uint64(1))}
	ents := []*raftpb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("x")}}
	if err := w.Save(state, ents); err != nil {
		t.Fatalf("Save with no observer set: %v", err)
	}
}

func TestWAL_SetFsyncObserver_CanBeClearedWithNil(t *testing.T) {
	p := t.TempDir()
	w, err := Create(zaptest.NewLogger(t), p, []byte("data"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer w.Close()

	var calls int
	w.SetFsyncObserver(func(time.Duration) { calls++ })
	w.SetFsyncObserver(nil)

	state := &raftpb.HardState{Term: new(uint64(1))}
	ents := []*raftpb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("x")}}
	if err := w.Save(state, ents); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if calls != 0 {
		t.Fatalf("observer called %d times after being cleared, want 0", calls)
	}
}
