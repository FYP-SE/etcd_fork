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
	"bytes"
	"strings"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type countingWriter struct {
	bytes.Buffer
	writes []int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes = append(w.writes, len(p))
	return w.Buffer.Write(p)
}

func TestLastEntryIndex(t *testing.T) {
	idx := func(i uint64) *uint64 { return &i }
	cases := []struct {
		m    *raftpb.Message
		want uint64
	}{
		{&raftpb.Message{Type: raftpb.MsgApp.Enum(), Entries: []*raftpb.Entry{{Index: idx(7)}, {Index: idx(8)}}}, 8},
		{&raftpb.Message{Type: raftpb.MsgApp.Enum()}, 0},                                                // empty append (probe/commit update)
		{&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum()}, 0},                                          // not an append
		{&raftpb.Message{Type: raftpb.MsgAppResp.Enum(), Entries: []*raftpb.Entry{{Index: idx(3)}}}, 0}, // wrong type
	}
	for i, c := range cases {
		if got := lastEntryIndex(c.m); got != c.want {
			t.Errorf("#%d: lastEntryIndex = %d, want %d", i, got, c.want)
		}
	}
}

func TestTraceSinkLineFormat(t *testing.T) {
	var w countingWriter
	s := newTraceSink(&w)
	s.record('q', 0xabc, 42, time.Unix(0, 1234567890))
	s.record('f', 0xabc, 0, time.Unix(0, 1)) // index 0: not traced
	s.flush()
	if got, want := w.String(), "HEIRTRACE,q,abc,42,1234567890\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestTraceSinkChunksStayBelowPipeBuf(t *testing.T) {
	var w countingWriter
	s := newTraceSink(&w)
	for i := 1; i <= 1000; i++ {
		s.record('f', 0xffffffffffffffff, uint64(i), time.Unix(1794780350, 123456789))
	}
	s.flush()
	for _, n := range w.writes {
		if n > traceChunk || n > 4096 {
			t.Fatalf("write of %d bytes exceeds chunk/PIPE_BUF", n)
		}
	}
	// every write ends on a line boundary, so no line is ever split
	if lines := strings.Count(w.String(), "\n"); lines != 1000 {
		t.Fatalf("lines = %d, want 1000", lines)
	}
	off := 0
	for _, n := range w.writes {
		off += n
		if w.String()[off-1] != '\n' {
			t.Fatalf("write ending at %d does not end with a newline", off)
		}
	}
}

func TestNilTraceSinkIsNoop(t *testing.T) {
	var s *traceSink
	s.record('q', 1, 1, time.Now()) // must not panic
	s.flush()
}
