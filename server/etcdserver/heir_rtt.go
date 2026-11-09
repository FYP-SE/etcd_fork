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
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

// heirRTTProberInterval is the stream-prober interval used with
// --experimental-heir-rtt-rank (default etcd: 4 s). With the prober's EWMA
// weight of 1/8, 500 ms gives an estimate that follows a change within ~4 s.
// Cost: one small HTTP GET per peer every 500 ms.
const heirRTTProberInterval = 500 * time.Millisecond

// peerRTTSource is the raft.PeerRTTProvider behind
// --experimental-heir-rtt-rank (DESIGN_UPDATE.md E8). The raft config is
// built before the transport exists, so the transport is attached later by
// NewServer; until then every peer reports no estimate, and raft ranks
// candidates by score alone.
type peerRTTSource struct {
	tr atomic.Pointer[rafthttp.Transport]
}

func (p *peerRTTSource) attach(tr *rafthttp.Transport) { p.tr.Store(tr) }

func (p *peerRTTSource) PeerRTT(id uint64) (time.Duration, bool) {
	tr := p.tr.Load()
	if tr == nil {
		return 0, false
	}
	return tr.PeerRTT(types.ID(id))
}
