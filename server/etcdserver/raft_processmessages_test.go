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

	"go.etcd.io/raft/v3/raftpb"
)

func msgApp(to, lastIndex uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgApp.Enum(),
		To:      &to,
		Entries: []*raftpb.Entry{{Index: &lastIndex}},
	}
}

func msgAppResp(to, index uint64) *raftpb.Message {
	return &raftpb.Message{Type: raftpb.MsgAppResp.Enum(), To: &to, Index: &index}
}

// TestProcessMessagesKeepsOrder: processMessages must hand messages to the
// transport in raft's order. Several MsgApps for one follower in one Ready
// (common under write load) must stay in ascending index order; sent in
// reverse, the follower rejects each one that does not extend its log and
// the leader has to probe and resend (RESEARCH_LOG 2026-11-17). Released
// etcd (v3.6.x) keeps the order: it marks dropped messages with To = 0
// instead of rebuilding the slice.
func TestProcessMessagesKeepsOrder(t *testing.T) {
	r := newRaftNode(raftNodeConfig{
		lg:          zaptest.NewLogger(t),
		heartbeat:   100 * time.Millisecond,
		isIDRemoved: func(id uint64) bool { return id == 9 },
	})
	in := []*raftpb.Message{
		msgApp(2, 10),
		msgApp(3, 10),
		msgAppResp(1, 4), // older MsgAppResp: dropped, only the latest is sent
		msgApp(2, 11),
		msgApp(9, 11), // removed member: dropped
		msgApp(3, 11),
		msgApp(2, 12),
		msgAppResp(1, 5), // latest MsgAppResp: kept
	}
	type key struct {
		typ   raftpb.MessageType
		to    uint64
		index uint64
	}
	k := func(m *raftpb.Message) key {
		idx := m.GetIndex()
		if n := len(m.Entries); n > 0 {
			idx = m.Entries[n-1].GetIndex()
		}
		return key{m.GetType(), m.GetTo(), idx}
	}
	want := []key{
		{raftpb.MsgApp, 2, 10},
		{raftpb.MsgApp, 3, 10},
		{raftpb.MsgApp, 2, 11},
		{raftpb.MsgApp, 3, 11},
		{raftpb.MsgApp, 2, 12},
		{raftpb.MsgAppResp, 1, 5},
	}
	out := r.processMessages(in)
	if len(out) != len(want) {
		t.Fatalf("got %d messages, want %d", len(out), len(want))
	}
	for i, m := range out {
		if got := k(m); got != want[i] {
			t.Errorf("message %d = %+v, want %+v", i, got, want[i])
		}
	}
}
