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

package robustness

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/scenarios"
)

// TestRobustnessDynatune (F4): the robustness checks against a cluster with
// Dynatune on, and without it for comparison, under crash and restart of
// random members (see scenarios.DynatuneFailover).
func TestRobustnessDynatune(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, s := range scenarios.DynatuneFailover(t) {
		t.Run(s.Name, func(t *testing.T) {
			lg := zaptest.NewLogger(t)
			s.Cluster.Logger = lg
			ctx := t.Context()
			c, err := e2e.NewEtcdProcessCluster(ctx, t, e2e.WithConfig(&s.Cluster))
			require.NoError(t, err)
			defer forcestopCluster(c)
			testRobustness(ctx, t, lg, s, c)
		})
	}
}
