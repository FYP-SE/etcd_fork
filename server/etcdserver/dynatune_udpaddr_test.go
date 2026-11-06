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

	"go.etcd.io/etcd/server/v3/config"
)

// Task D1b wiring: the transport only listens for UDP heartbeats under
// --experimental-dynatune, on --listen-peer-udp-url or the default 0.0.0.0:2381.
func TestUDPListenAddr(t *testing.T) {
	require.Empty(t, udpListenAddr(config.ServerConfig{}), "off: no UDP listener")
	require.Empty(t, udpListenAddr(config.ServerConfig{ListenPeerUDPAddr: "0.0.0.0:2381"}),
		"off: an address without the master switch is ignored (embed.Validate rejects it earlier)")
	require.Equal(t, "0.0.0.0:2381", udpListenAddr(config.ServerConfig{ExperimentalDynatune: true}))
	require.Equal(t, "10.0.0.1:2391",
		udpListenAddr(config.ServerConfig{ExperimentalDynatune: true, ListenPeerUDPAddr: "10.0.0.1:2391"}))
}
