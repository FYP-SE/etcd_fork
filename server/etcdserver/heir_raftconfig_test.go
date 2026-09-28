// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// DESIGN_UPDATE.md D2: --experimental-heir-lease reaches raft.Config, and
// only takes effect alongside --experimental-heir-election.
func TestRaftConfig_HeirLeaseWired(t *testing.T) {
	cfg := config.ServerConfig{
		Logger:                   zaptest.NewLogger(t),
		ElectionTicks:            10,
		ExperimentalHeirElection: true,
		ExperimentalHeirLease:    true,
	}
	rc, _ := raftConfig(cfg, 1, raft.NewMemoryStorage()) // runner not started: nothing to stop
	if !rc.HeirLease || !rc.HeirElection {
		t.Fatalf("HeirLease=%v HeirElection=%v, want both true", rc.HeirLease, rc.HeirElection)
	}
}

func TestRaftConfig_StockLeavesHeirLeaseOff(t *testing.T) {
	cfg := config.ServerConfig{Logger: zaptest.NewLogger(t), ElectionTicks: 10}
	rc, runner := raftConfig(cfg, 1, raft.NewMemoryStorage())
	if runner != nil {
		t.Fatal("stock config built a stability runner")
	}
	if rc.HeirLease || rc.HeirElection || rc.StabilityScorer != nil {
		t.Fatalf("stock raft.Config has HeirRaft on: %+v", rc)
	}
}

// DESIGN_UPDATE.md D7: the runner gets the heartbeat interval (tick) so the
// jitter signal works, and a leader heartbeat reaching Process feeds it.
func TestRaftConfig_RunnerGetsHeartbeatInterval(t *testing.T) {
	cfg := config.ServerConfig{
		Logger:                   zaptest.NewLogger(t),
		ElectionTicks:            10,
		TickMs:                   100,
		ExperimentalHeirElection: true,
	}
	_, runner := raftConfig(cfg, 1, raft.NewMemoryStorage())
	if runner == nil {
		t.Fatal("no stability runner built")
	}
	if got := runner.HeartbeatInterval(); got != 100*time.Millisecond {
		t.Fatalf("runner heartbeat interval = %v, want 100ms", got)
	}
}

func TestObserveHeirHeartbeat(t *testing.T) {
	cfg := config.ServerConfig{
		Logger:                   zaptest.NewLogger(t),
		ElectionTicks:            10,
		TickMs:                   100,
		ExperimentalHeirElection: true,
	}
	_, runner := raftConfig(cfg, 1, raft.NewMemoryStorage())
	hb := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum()}
	app := &raftpb.Message{Type: raftpb.MsgApp.Enum()}
	now := time.Unix(0, 0)
	for i := 0; i < 400; i++ {
		observeHeirHeartbeat(runner, app, now) // ignored: not a heartbeat
		gap := time.Duration(0)
		if i%2 == 1 {
			gap = 200 * time.Millisecond // 100 ms off the interval each time
		}
		now = now.Add(gap)
		observeHeirHeartbeat(runner, hb, now)
	}
	if !runner.Critical() {
		t.Fatal("jittery heartbeats through observeHeirHeartbeat did not reach the runner")
	}
	observeHeirHeartbeat(nil, hb, now) // HeirRaft off: must not panic
}

// ReadIndex heartbeats (linearizable reads) carry a Context and are sent
// whenever a read arrives, not on the heartbeat tick. Counting them made
// every follower look jittery under a read-probing client (found in the
// 2026-09-28 integration run: healthy followers scored ~186, the leader
// ~230). Only plain tick heartbeats feed the jitter signal.
func TestObserveHeirHeartbeat_IgnoresReadIndexHeartbeats(t *testing.T) {
	cfg := config.ServerConfig{
		Logger:                   zaptest.NewLogger(t),
		ElectionTicks:            10,
		TickMs:                   100,
		ExperimentalHeirElection: true,
	}
	_, runner := raftConfig(cfg, 1, raft.NewMemoryStorage())
	plain := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum()}
	readIdx := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), Context: []byte("read-request-id")}
	now := time.Unix(0, 0)
	for i := 0; i < 400; i++ {
		now = now.Add(100 * time.Millisecond)
		observeHeirHeartbeat(runner, plain, now)
		observeHeirHeartbeat(runner, readIdx, now.Add(37*time.Millisecond)) // off-tick
	}
	if runner.Critical() || runner.Score() != 255 {
		t.Fatalf("score %d critical %v: ReadIndex heartbeats leaked into the jitter signal", runner.Score(), runner.Critical())
	}
}
