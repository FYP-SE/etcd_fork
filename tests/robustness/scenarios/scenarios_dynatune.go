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

package scenarios

import (
	"testing"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/failpoint"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
)

// withDynatuneEnabled turns Dynatune on with exactly the tunables of the
// evaluation arm (decision Q6: the authors' defaults; see ARM_FLAGS in
// etcd_baseline_experiments/testbed/gen-compose.py). As for HeirRaft, setting
// the embed.Config fields is enough: the e2e framework turns every field that
// differs from the default into a command-line flag. The per-member UDP port
// is added by EtcdServerProcessConfig when ExperimentalDynatune is set.
func withDynatuneEnabled(c *e2e.EtcdProcessClusterConfig) {
	c.ServerConfig.ExperimentalDynatune = true
	c.ServerConfig.MaxElectionMetricsCapacity = 1000
	c.ServerConfig.MinElectionMetricsCapacity = 10
	c.ServerConfig.ElectionSafetyFactor = 3
	c.ServerConfig.HeartbeatReachabilityGoal = 0.99
	c.ServerConfig.OptimizeHeartbeatInterval = true
}

// DynatuneFailover (F4): run the robustness suite (linearizability and watch
// guarantees) against a Dynatune-enabled cluster under repeated crash and
// restart of a random member, next to the same cluster without Dynatune.
// failpoint.KillFailpoint (SIGKILL and restart) does not need a
// failpoint-instrumented build, as in HeirRaftFailover.
func DynatuneFailover(_ *testing.T) []TestScenario {
	arms := []struct {
		name string
		opts []e2e.EPClusterOption
	}{
		{"Stock", nil},
		{"Dynatune", []e2e.EPClusterOption{withDynatuneEnabled}},
	}
	var out []TestScenario
	for _, arm := range arms {
		opts := append([]e2e.EPClusterOption{e2e.WithClusterSize(3)}, arm.opts...)
		out = append(out, TestScenario{
			Name:      "DynatuneFailover/" + arm.name + "/ClusterOfSize3",
			Failpoint: failpoint.KillFailpoint,
			Traffic:   traffic.EtcdPut,
			Profile: traffic.Profile{
				KeyValue:   &traffic.KeyValueHigh,
				Watch:      &traffic.WatchDefault,
				Compaction: &traffic.CompactionDefault,
			},
			Cluster: *e2e.NewConfig(opts...),
		})
	}
	return out
}
