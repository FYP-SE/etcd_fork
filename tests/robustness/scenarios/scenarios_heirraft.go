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

// withHeirRaftEnabled turns on all HeirRaft experimental flags
// (T5.2). embed.Config.AddFlags already registers
// --experimental-heir-election/-graceful-handover/-log-priority, and the
// e2e framework's EtcdServerProcessConfig diffs cfg.ServerConfig against
// embed.NewConfig()'s defaults field-by-field to build the process's CLI
// args (see values()/EtcdServerProcessConfig in framework/e2e/cluster.go) --
// so setting these struct fields is sufficient to get the flags onto the
// spawned etcd process, no new EPClusterOption plumbing required.
func withHeirRaftEnabled(c *e2e.EtcdProcessClusterConfig) {
	c.ServerConfig.ExperimentalHeirElection = true
	c.ServerConfig.ExperimentalGracefulHandover = true
	c.ServerConfig.ExperimentalHeirLogPriority = true
	c.ServerConfig.ExperimentalHeirLease = true // DESIGN_UPDATE.md D2
}

// HeirRaftFailover is T6.3: run the robustness (linearizability/watch
// guarantee) suite against a HeirRaft-enabled build under repeated
// failover injection. Reuses failpoint.KillFailpoint (SIGKILL + restart of
// a random member) rather than a gofail-instrumented failpoint -- Kill
// doesn't gate on GoFailEnabled (see Regression's Issue13766 scenario),
// so this needs no failpoints-enabled build, sidestepping this repo's
// known `make` breakage on paths containing spaces (see root CLAUDE.md).
func HeirRaftFailover(_ *testing.T) []TestScenario {
	return []TestScenario{
		{
			Name:      "HeirRaftFailover/ClusterOfSize3",
			Failpoint: failpoint.KillFailpoint,
			Traffic:   traffic.EtcdPut,
			Profile: traffic.Profile{
				KeyValue:   &traffic.KeyValueHigh,
				Watch:      &traffic.WatchDefault,
				Compaction: &traffic.CompactionDefault,
			},
			Cluster: *e2e.NewConfig(
				e2e.WithClusterSize(3),
				withHeirRaftEnabled,
			),
		},
	}
}
