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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/raft/v3"
)

// Task D1: how --experimental-dynatune and its tunables reach raft.Config.
// Units: with Dynatune on, raft is ticked once per millisecond, so raft's
// HeartbeatTick/ElectionTick are in milliseconds. The server-level durations
// derived from ServerConfig.TickMs/ElectionTicks (leases, ReadIndex, RPC
// timeouts) are untouched and stay in wall-clock ms.

func dynatuneServerConfig(t *testing.T) config.ServerConfig {
	return config.ServerConfig{
		Logger:                     zaptest.NewLogger(t),
		TickMs:                     100,
		ElectionTicks:              10, // --election-timeout=1000
		ExperimentalDynatune:       true,
		MaxElectionMetricsCapacity: 500,
		MinElectionMetricsCapacity: 7,
		ElectionSafetyFactor:       2,
		HeartbeatReachabilityGoal:  0.999,
		HeartbeatK:                 9,
		OptimizeHeartbeatInterval:  true,
	}
}

func TestRaftConfig_StockIsUnchangedByDynatuneSupport(t *testing.T) {
	cfg := config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 100, ElectionTicks: 10}
	rc, runner := raftConfig(cfg, 1, raft.NewMemoryStorage())
	require.Nil(t, runner)
	require.False(t, rc.Dynatune)
	require.Equal(t, 1, rc.HeartbeatTick)
	require.Equal(t, 10, rc.ElectionTick)
	require.Zero(t, rc.MaxElectionMetricsCapacity)
	require.Zero(t, rc.MinElectionMetricsCapacity)
	require.Zero(t, rc.ElectionSafetyFactor)
	require.Zero(t, rc.HeartbeatReachabilityGoal)
	require.Zero(t, rc.K)
	require.False(t, rc.OptimizeHeartbeatInterval)
	require.Nil(t, rc.Clock)
}

func TestRaftConfig_DynatuneUsesMillisecondTicks(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		tickMs                uint
		electionTicks         int
		wantHeartbeat, wantEl int
	}{
		{"etcd defaults 100/1000", 100, 10, 100, 1000},
		{"50 ms heartbeat, 1 s election", 50, 20, 50, 1000},
		{"10 ms heartbeat, 500 ms election", 10, 50, 10, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dynatuneServerConfig(t)
			cfg.TickMs = tc.tickMs
			cfg.ElectionTicks = tc.electionTicks
			rc, _ := raftConfig(cfg, 1, raft.NewMemoryStorage())
			require.True(t, rc.Dynatune)
			require.Equal(t, tc.wantHeartbeat, rc.HeartbeatTick)
			require.Equal(t, tc.wantEl, rc.ElectionTick)
		})
	}
}

func TestRaftConfig_DynatuneTunablesAreCopied(t *testing.T) {
	rc, _ := raftConfig(dynatuneServerConfig(t), 1, raft.NewMemoryStorage())
	require.Equal(t, 500, rc.MaxElectionMetricsCapacity)
	require.Equal(t, 7, rc.MinElectionMetricsCapacity)
	require.Equal(t, 2, rc.ElectionSafetyFactor)
	require.Equal(t, 0.999, rc.HeartbeatReachabilityGoal)
	require.Equal(t, int64(9), rc.K)
	require.True(t, rc.OptimizeHeartbeatInterval)
}

func TestRaftConfig_DynatuneUnsetTunablesStayZeroForRaftToDefault(t *testing.T) {
	cfg := config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 100, ElectionTicks: 10, ExperimentalDynatune: true}
	rc, _ := raftConfig(cfg, 1, raft.NewMemoryStorage())
	require.True(t, rc.Dynatune)
	require.Zero(t, rc.MaxElectionMetricsCapacity)
	require.Zero(t, rc.ElectionSafetyFactor)
	// ...and raft accepts that and fills its own defaults.
	_, err := raft.NewRawNode(rc)
	require.NoError(t, err)
}

func TestRaftConfig_BothModesAreAcceptedByRaft(t *testing.T) {
	stock := config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 100, ElectionTicks: 10}
	for name, cfg := range map[string]config.ServerConfig{"stock": stock, "dynatune": dynatuneServerConfig(t)} {
		t.Run(name, func(t *testing.T) {
			rc, _ := raftConfig(cfg, 1, raft.NewMemoryStorage())
			_, err := raft.NewRawNode(rc)
			require.NoError(t, err)
		})
	}
}

func TestRaftConfig_DynatuneWithHeirRaftIsRefused(t *testing.T) {
	// Defence in depth: embed.Config.Validate already rejects this.
	cfg := dynatuneServerConfig(t)
	cfg.ExperimentalHeirElection = true
	require.Panics(t, func() { raftConfig(cfg, 1, raft.NewMemoryStorage()) })
}
