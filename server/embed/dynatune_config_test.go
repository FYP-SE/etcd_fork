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

package embed

import (
	"flag"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// Dynatune flags (task D1): master switch --experimental-dynatune, tunables
// under the reference implementation's own flag names so the reference and
// this port take the same flag list.

func parseFlags(t *testing.T, args ...string) *Config {
	t.Helper()
	cfg := NewConfig()
	fs := flag.NewFlagSet("etcd", flag.ContinueOnError)
	cfg.AddFlags(fs)
	require.NoError(t, fs.Parse(args))
	return cfg
}

func TestDynatuneFlags_DefaultToOff(t *testing.T) {
	cfg := NewConfig()
	require.False(t, cfg.ExperimentalDynatune)
	require.Zero(t, cfg.MaxElectionMetricsCapacity)
	require.Zero(t, cfg.MinElectionMetricsCapacity)
	require.Zero(t, cfg.ElectionSafetyFactor)
	require.Zero(t, cfg.HeartbeatReachabilityGoal)
	require.Zero(t, cfg.HeartbeatK)
	require.False(t, cfg.OptimizeHeartbeatInterval)
	require.Empty(t, cfg.ListenPeerUdpUrl)
	require.NoError(t, cfg.Validate(), "a stock configuration must stay valid")
}

func TestDynatuneFlags_Parse(t *testing.T) {
	cfg := parseFlags(t,
		"--experimental-dynatune",
		"--max-election-metrics-capacity=500",
		"--min-election-metrics-capacity=7",
		"--election-safety-factor=2",
		"--heartbeat-reachability-goal=0.999",
		"--heartbeat-k=9",
		"--optimize-heartbeat-interval",
		"--listen-peer-udp-url=127.0.0.2:2391",
	)
	require.True(t, cfg.ExperimentalDynatune)
	require.Equal(t, 500, cfg.MaxElectionMetricsCapacity)
	require.Equal(t, 7, cfg.MinElectionMetricsCapacity)
	require.Equal(t, 2, cfg.ElectionSafetyFactor)
	require.Equal(t, 0.999, cfg.HeartbeatReachabilityGoal)
	require.Equal(t, int64(9), cfg.HeartbeatK)
	require.True(t, cfg.OptimizeHeartbeatInterval)
	require.Equal(t, "127.0.0.2:2391", cfg.ListenPeerUdpUrl)
	require.NoError(t, cfg.Validate())
}

func TestDynatuneFlags_ConfigFileKeys(t *testing.T) {
	yc := struct {
		Enable   bool    `json:"experimental-dynatune"`
		Max      int     `json:"max-election-metrics-capacity"`
		Min      int     `json:"min-election-metrics-capacity"`
		Factor   int     `json:"election-safety-factor"`
		Goal     float64 `json:"heartbeat-reachability-goal"`
		K        int64   `json:"heartbeat-k"`
		Optimize bool    `json:"optimize-heartbeat-interval"`
		UDP      string  `json:"listen-peer-udp-url"`
	}{true, 500, 7, 2, 0.999, 9, true, "127.0.0.2:2391"}
	b, err := yaml.Marshal(&yc)
	require.NoError(t, err)
	tmpfile := mustCreateCfgFile(t, b)
	defer os.Remove(tmpfile.Name())

	cfg, err := ConfigFromFile(tmpfile.Name())
	require.NoError(t, err)
	require.True(t, cfg.ExperimentalDynatune)
	require.Equal(t, 500, cfg.MaxElectionMetricsCapacity)
	require.Equal(t, 7, cfg.MinElectionMetricsCapacity)
	require.Equal(t, 2, cfg.ElectionSafetyFactor)
	require.Equal(t, 0.999, cfg.HeartbeatReachabilityGoal)
	require.Equal(t, int64(9), cfg.HeartbeatK)
	require.True(t, cfg.OptimizeHeartbeatInterval)
	require.Equal(t, "127.0.0.2:2391", cfg.ListenPeerUdpUrl)
}

func TestDynatuneValidate_AcceptsDefaultsAndExplicitValues(t *testing.T) {
	cfg := NewConfig()
	cfg.ExperimentalDynatune = true
	require.NoError(t, cfg.Validate(), "master switch alone: raft supplies the defaults")

	cfg = NewConfig()
	cfg.ExperimentalDynatune = true
	cfg.MaxElectionMetricsCapacity = 1000
	cfg.MinElectionMetricsCapacity = 10
	cfg.ElectionSafetyFactor = 3
	cfg.HeartbeatReachabilityGoal = 0.99
	cfg.OptimizeHeartbeatInterval = true
	cfg.ListenPeerUdpUrl = "0.0.0.0:2381"
	require.NoError(t, cfg.Validate())
}

func TestDynatuneValidate_RejectsBadValuesWhenEnabled(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*Config)
	}{
		{"negative max window", func(c *Config) { c.MaxElectionMetricsCapacity = -1 }},
		{"negative min window", func(c *Config) { c.MinElectionMetricsCapacity = -1 }},
		{"min >= max window", func(c *Config) { c.MaxElectionMetricsCapacity = 5; c.MinElectionMetricsCapacity = 5 }},
		{"negative safety factor", func(c *Config) { c.ElectionSafetyFactor = -1 }},
		{"goal == 1 when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = 1 }},
		{"goal > 1 when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = 1.5 }},
		{"negative goal when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = -0.1 }},
		{"NaN goal when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = math.NaN() }},
		{"negative K when not optimising", func(c *Config) { c.HeartbeatK = -1 }},
		{"UDP address without a port", func(c *Config) { c.ListenPeerUdpUrl = "0.0.0.0" }},
		{"UDP address that is not host:port", func(c *Config) { c.ListenPeerUdpUrl = "http://0.0.0.0:2381" }},
		{"UDP port out of range", func(c *Config) { c.ListenPeerUdpUrl = "0.0.0.0:99999" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.ExperimentalDynatune = true
			c.mod(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}

func TestDynatuneValidate_OnlyChecksTheTunableThatIsUsed(t *testing.T) {
	cfg := NewConfig()
	cfg.ExperimentalDynatune = true
	cfg.HeartbeatReachabilityGoal = 1.5 // unused: not optimising
	require.NoError(t, cfg.Validate())

	cfg = NewConfig()
	cfg.ExperimentalDynatune = true
	cfg.OptimizeHeartbeatInterval = true
	cfg.HeartbeatK = -1 // unused: optimising
	require.NoError(t, cfg.Validate())
}

// A tunable given without the master switch would silently run stock etcd
// while the operator believes Dynatune is on -- the very failure the
// reference's reflection-based plumbing allows -- so it is an error.
func TestDynatuneValidate_RejectsTunablesWithoutTheMasterSwitch(t *testing.T) {
	cases := map[string]func(*Config){
		"max-election-metrics-capacity": func(c *Config) { c.MaxElectionMetricsCapacity = 10 },
		"min-election-metrics-capacity": func(c *Config) { c.MinElectionMetricsCapacity = 10 },
		"election-safety-factor":        func(c *Config) { c.ElectionSafetyFactor = 2 },
		"heartbeat-reachability-goal":   func(c *Config) { c.HeartbeatReachabilityGoal = 0.9 },
		"heartbeat-k":                   func(c *Config) { c.HeartbeatK = 5 },
		"optimize-heartbeat-interval":   func(c *Config) { c.OptimizeHeartbeatInterval = true },
		"listen-peer-udp-url":           func(c *Config) { c.ListenPeerUdpUrl = "0.0.0.0:2381" },
	}
	for flagName, set := range cases {
		t.Run(flagName, func(t *testing.T) {
			cfg := NewConfig()
			set(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.ErrorContains(t, err, "--"+flagName)
			require.ErrorContains(t, err, "--experimental-dynatune")
		})
	}
}

// The combined Dynatune + HeirRaft arm is out of scope: HeirRaft piggybacks
// its heir/stability fields on heartbeats, which Dynatune sends over UDP.
func TestDynatuneValidate_RejectsHeirRaftFlags(t *testing.T) {
	cases := map[string]func(*Config){
		"experimental-heir-election":     func(c *Config) { c.ExperimentalHeirElection = true },
		"experimental-heir-log-priority": func(c *Config) { c.ExperimentalHeirLogPriority = true },
		"experimental-heir-lease":        func(c *Config) { c.ExperimentalHeirLease = true },
		"experimental-graceful-handover": func(c *Config) { c.ExperimentalGracefulHandover = true },
		"experimental-heir-config":       func(c *Config) { c.ExperimentalHeirConfig = "heir-timeout=4" },
	}
	for flagName, set := range cases {
		t.Run(flagName, func(t *testing.T) {
			cfg := NewConfig()
			cfg.ExperimentalDynatune = true
			set(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.ErrorContains(t, err, "--"+flagName)
			require.ErrorContains(t, err, "--experimental-dynatune")
		})
	}
}
