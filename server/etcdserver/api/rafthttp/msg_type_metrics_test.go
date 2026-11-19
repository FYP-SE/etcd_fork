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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// TestRecordMessageSent checks the per-type message counters used by the
// final evaluation's overhead analysis: one count and proto.Size bytes per
// message, labelled by raft message type.
func TestRecordMessageSent(t *testing.T) {
	m := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), To: new(uint64(2)), From: new(uint64(1)), Term: new(uint64(5))}
	label := raftpb.MsgHeartbeat.String()

	count0 := testutil.ToFloat64(raftMessagesSent.WithLabelValues(label))
	bytes0 := testutil.ToFloat64(raftMessageBytesSent.WithLabelValues(label))

	recordMessageSent(m)
	recordMessageSent(m)

	if got := testutil.ToFloat64(raftMessagesSent.WithLabelValues(label)) - count0; got != 2 {
		t.Fatalf("count delta = %v, want 2", got)
	}
	want := float64(2 * proto.Size(m))
	if got := testutil.ToFloat64(raftMessageBytesSent.WithLabelValues(label)) - bytes0; got != want {
		t.Fatalf("bytes delta = %v, want %v", got, want)
	}
}
