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

package rafthttp

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/client/pkg/v3/types"
)

// F4 / option A: where a peer's UDP listener is. The destination port is the
// peer's peer-URL port plus the local member's (UDP port - peer-URL port), so
// members that share one host (the robustness harness) can each have their own
// UDP port, while the standard layout (peer 2380 and UDP 2381 everywhere, one
// host per member) gives exactly the old rule: the same UDP port for everyone.

func mustURLs(t *testing.T, ss ...string) types.URLs {
	t.Helper()
	us := make(types.URLs, 0, len(ss))
	for _, s := range ss {
		u, err := url.Parse(s)
		require.NoError(t, err)
		us = append(us, *u)
	}
	return us
}

func TestUDPPortOffset(t *testing.T) {
	for _, tc := range []struct {
		name    string
		local   types.URLs
		udpPort string
		want    int
		ok      bool
	}{
		{"standard layout", mustURLs(t, "http://10.0.0.1:2380"), "2381", 1, true},
		{"harness member", mustURLs(t, "http://127.0.0.1:20010"), "20011", 1, true},
		{"same port, different protocol", mustURLs(t, "http://10.0.0.1:2380"), "2380", 0, true},
		{"udp below peer port", mustURLs(t, "http://10.0.0.1:2380"), "2000", -380, true},
		{"no local peer url", nil, "2381", 0, false},
		{"peer url without port", mustURLs(t, "http://10.0.0.1"), "2381", 0, false},
		{"udp port not a number", mustURLs(t, "http://10.0.0.1:2380"), "x", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := udpPortOffset(tc.local, tc.udpPort)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestPeerUDPPort(t *testing.T) {
	std := mustURLs(t, "http://10.0.0.2:2380")
	for _, tc := range []struct {
		name     string
		peer     types.URLs
		offset   int
		hasOff   bool
		fallback string
		want     string
	}{
		// standard layout: every member is on 2380 with UDP on 2381, so the
		// result equals the fallback (the local UDP port): the old behaviour
		{"standard layout equals the old rule", std, 1, true, "2381", "2381"},
		{"harness: each member its own port", mustURLs(t, "http://127.0.0.1:20020"), 1, true, "20011", "20021"},
		{"no offset known: old rule", mustURLs(t, "http://127.0.0.1:20020"), 0, false, "2381", "2381"},
		{"peer url without port: old rule", mustURLs(t, "http://10.0.0.2"), 1, true, "2381", "2381"},
		{"no peer url: old rule", nil, 1, true, "2381", "2381"},
		{"result out of range: old rule", mustURLs(t, "http://10.0.0.2:65535"), 5, true, "2381", "2381"},
		{"result below 1: old rule", mustURLs(t, "http://10.0.0.2:100"), -200, true, "2381", "2381"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, peerUDPPort(tc.peer, tc.offset, tc.hasOff, tc.fallback))
		})
	}
}
