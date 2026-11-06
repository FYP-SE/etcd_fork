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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/raft/v3/raftpb"
)

// Task D1c: with Dynatune on, raft is ticked once per MILLISECOND, because
// Dynatune writes millisecond values into raft's tick-counted timeouts (see
// raftConfig). Everything that converts between ticks and wall-clock time in
// the server has to follow, and the heartbeat contention detector -- which
// assumes one fixed heartbeat period -- has to stand down, because every
// follower now has its own interval.

func stockTickConfig() config.ServerConfig {
	return config.ServerConfig{TickMs: 100, ElectionTicks: 10}
}

func dynatuneTickConfig() config.ServerConfig {
	c := stockTickConfig()
	c.ExperimentalDynatune = true
	return c
}

func TestRaftTickInterval(t *testing.T) {
	require.Equal(t, 100*time.Millisecond, raftTickInterval(stockTickConfig()), "stock: one tick = --heartbeat-interval")
	require.Equal(t, time.Millisecond, raftTickInterval(dynatuneTickConfig()), "Dynatune: one tick = 1 ms")
	c := dynatuneTickConfig()
	c.TickMs = 50
	require.Equal(t, time.Millisecond, raftTickInterval(c), "independent of --heartbeat-interval")
}

func TestRaftTicks_ConvertsStockTicksIntoRaftTicks(t *testing.T) {
	require.Equal(t, 9, raftTicks(stockTickConfig(), 9), "stock: unchanged")
	require.Equal(t, 900, raftTicks(dynatuneTickConfig(), 9), "9 stock ticks of 100 ms = 900 raft ticks of 1 ms")
	c := dynatuneTickConfig()
	c.TickMs = 50
	require.Equal(t, 950, raftTicks(c, 19))
	require.Equal(t, 0, raftTicks(dynatuneTickConfig(), 0))
}

// The startup fast-forward (adjustTicks) must advance raft by the same
// wall-clock time in both modes: ElectionTicks-1 / -2 stock ticks.
func TestForwardTicks_AdvanceTheSameWallClockTimeInBothModes(t *testing.T) {
	for _, minus := range []int{1, 2} {
		want := time.Duration(10-minus) * 100 * time.Millisecond
		for name, cfg := range map[string]config.ServerConfig{"stock": stockTickConfig(), "dynatune": dynatuneTickConfig()} {
			n := forwardTicks(cfg, minus)
			require.Equal(t, want, ticksToDuration(cfg, n), "%s, ElectionTicks-%d", name, minus)
		}
	}
	require.Equal(t, 9, forwardTicks(stockTickConfig(), 1))
	require.Equal(t, 900, forwardTicks(dynatuneTickConfig(), 1))
	require.Equal(t, 800, forwardTicks(dynatuneTickConfig(), 2))
}

func TestRaftNode_TickerFollowsTheTickInterval(t *testing.T) {
	// heartbeat (the stock period) is an hour: only the 2 ms tick interval can
	// produce a tick in time.
	r := newRaftNode(raftNodeConfig{
		lg:           zaptest.NewLogger(t),
		heartbeat:    time.Hour,
		tickInterval: 2 * time.Millisecond,
		dynatune:     true,
	})
	defer r.ticker.Stop()
	select {
	case <-r.ticker.C:
	case <-time.After(2 * time.Second):
		t.Fatal("no tick within 2 s: the ticker ignores tickInterval")
	}
}

func TestRaftNode_TickerFallsBackToHeartbeatWithoutATickInterval(t *testing.T) {
	r := newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), heartbeat: 5 * time.Millisecond})
	defer r.ticker.Stop()
	select {
	case <-r.ticker.C:
	case <-time.After(2 * time.Second):
		t.Fatal("no tick within 2 s: existing behaviour (ticker = heartbeat) broken")
	}
}

func TestRaftNode_ContentionDetectorOnlyInStockMode(t *testing.T) {
	stock := newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), heartbeat: 100 * time.Millisecond})
	defer stock.ticker.Stop()
	require.NotNil(t, stock.td)

	dyn := newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), heartbeat: 100 * time.Millisecond, dynatune: true})
	defer dyn.ticker.Stop()
	require.Nil(t, dyn.td)
}

func heartbeatWarnings(t *testing.T, dynatune bool) int {
	t.Helper()
	core, logs := observer.New(zap.WarnLevel)
	r := newRaftNode(raftNodeConfig{
		lg:          zap.New(core),
		heartbeat:   time.Millisecond, // detector tolerates 2 ms between heartbeats to a peer
		dynatune:    dynatune,
		isIDRemoved: func(uint64) bool { return false },
	})
	defer r.ticker.Stop()
	hb := func() []*raftpb.Message {
		return []*raftpb.Message{{Type: raftpb.MsgHeartbeat.Enum(), To: new(uint64(2))}}
	}
	require.Len(t, r.processMessages(hb()), 1)
	time.Sleep(30 * time.Millisecond)
	require.Len(t, r.processMessages(hb()), 1, "heartbeats are still sent")
	return logs.FilterMessageSnippet("failed to send out heartbeat on time").Len()
}

// Control + test: the same two heartbeats 30 ms apart trip the detector in
// stock mode and are silent in Dynatune mode, where followers' intervals are
// individually tuned (up to ~1 s on a LAN) and a fixed period is meaningless.
func TestProcessMessages_HeartbeatContentionWarningIsStockOnly(t *testing.T) {
	require.Positive(t, heartbeatWarnings(t, false), "control: stock mode must warn")
	require.Zero(t, heartbeatWarnings(t, true), "Dynatune mode must not warn")
}

func TestRaftNode_HeartbeatLeaderResetWorksWithoutADetector(t *testing.T) {
	// raftNode.start calls r.td.Reset() on leadership change; with no detector
	// that must be a no-op, not a nil dereference.
	r := newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), heartbeat: time.Millisecond, dynatune: true})
	defer r.ticker.Stop()
	require.NotPanics(t, func() { r.resetHeartbeatDetector() })
	stock := newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), heartbeat: time.Millisecond})
	defer stock.ticker.Stop()
	require.NotPanics(t, func() { stock.resetHeartbeatDetector() })
}
