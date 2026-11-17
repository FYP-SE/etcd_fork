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
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
)

// HeirRaft diagnostics (RESEARCH_LOG 2026-11-17): measures per-peer send
// skew on the leader. When ETCD_HEIRRAFT_SEND_TRACE=1, every MsgApp that
// carries entries is recorded when peer.send queues it for a peer ("q") and
// when that peer's stream writer takes it off its queue ("d"), and when the
// stream writer has flushed it to that peer's connection ("f"):
//
//	HEIRTRACE,<stage>,<peer id hex>,<last entry index>,<unix ns>
//
// Lines go to stderr in chunks of at most traceChunk bytes, so `docker logs`
// keeps them whole (a pipe write <= PIPE_BUF is atomic and is not
// interleaved with zap's lines). Off by default; debug use only, never in an
// evaluation arm.
const (
	traceChunk     = 3584 // < PIPE_BUF (4096)
	traceFlushTick = 100 * time.Millisecond
)

var sendTrace = newSendTraceFromEnv()

func newSendTraceFromEnv() *traceSink {
	if os.Getenv("ETCD_HEIRRAFT_SEND_TRACE") != "1" {
		return nil
	}
	s := newTraceSink(os.Stderr)
	go func() {
		for range time.Tick(traceFlushTick) {
			s.flush()
		}
	}()
	return s
}

type traceSink struct {
	mu  sync.Mutex
	out io.Writer
	buf []byte
}

func newTraceSink(out io.Writer) *traceSink {
	return &traceSink{out: out, buf: make([]byte, 0, traceChunk)}
}

// lastEntryIndex is the index of the last entry m carries, or 0 if m is not
// a MsgApp with entries (those are not traced).
func lastEntryIndex(m *raftpb.Message) uint64 {
	if m.GetType() != raftpb.MsgApp || len(m.Entries) == 0 {
		return 0
	}
	return m.Entries[len(m.Entries)-1].GetIndex()
}

// record appends one line; nil-safe so call sites need no check.
func (s *traceSink) record(stage byte, peer types.ID, index uint64, now time.Time) {
	if s == nil || index == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	line := make([]byte, 0, 64)
	line = append(line, "HEIRTRACE,"...)
	line = append(line, stage, ',')
	line = strconv.AppendUint(line, uint64(peer), 16)
	line = append(line, ',')
	line = strconv.AppendUint(line, index, 10)
	line = append(line, ',')
	line = strconv.AppendInt(line, now.UnixNano(), 10)
	line = append(line, '\n')
	if len(s.buf)+len(line) > traceChunk {
		s.flushLocked()
	}
	s.buf = append(s.buf, line...)
}

func (s *traceSink) flush() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushLocked()
}

func (s *traceSink) flushLocked() {
	if len(s.buf) == 0 {
		return
	}
	_, _ = s.out.Write(s.buf)
	s.buf = s.buf[:0]
}
