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
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xiang90/probing"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/types"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3/raftpb"
)

// Task D1b: UDP heartbeat transport for Dynatune. When Transport.UDPListenAddr
// is set, MsgHeartbeat and MsgHeartbeatResp to a known peer travel by UDP;
// everything else stays on the TCP peer path. The tests use real sockets on
// the loopback aliases 127.0.0.2 / 127.0.0.3 / 127.0.0.4 sharing one port,
// which is how members of a real cluster share the UDP port on different hosts.

const loopback2, loopback3, loopback4 = "127.0.0.2", "127.0.0.3", "127.0.0.4"

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", loopback2+":0")
	require.NoError(t, err)
	defer pc.Close()
	return pc.LocalAddr().(*net.UDPAddr).Port
}

func peerURLs(t *testing.T, ip string) types.URLs {
	t.Helper()
	u, err := types.NewURLs([]string{fmt.Sprintf("http://%s:2380", ip)})
	require.NoError(t, err)
	return u
}

func udpHeartbeatMsg(from, to uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:              raftpb.MsgHeartbeat.Enum(),
		From:              new(from),
		To:                new(to),
		Term:              new(uint64(3)),
		Commit:            new(uint64(7)),
		Context:           []byte("ctx"),
		Rtt:               new(int64(55_000_000)),
		SendTime:          new(int64(1_700_000_000_123_456_789)),
		SequenceId:        new(int64(42)),
		HeartbeatInterval: new(int64(31)),
	}
}

type udpMember struct {
	tr   *Transport
	raft *fakeRaft
	recv chan *raftpb.Message
	peer *fakePeer // the TCP-path peer this member has for the other one
}

type udpPair struct {
	a, b *udpMember // member 1 on 127.0.0.2, member 2 on 127.0.0.3
	port int
}

func newUDPMember(t *testing.T, id, remote types.ID, ip string, port int) *udpMember {
	t.Helper()
	recv := make(chan *raftpb.Message, 64)
	fr := &fakeRaft{recvc: recv}
	fp := newFakePeer()
	tr := &Transport{
		Logger:        zaptest.NewLogger(t),
		ID:            id,
		Raft:          fr,
		ServerStats:   stats.NewServerStats("", ""),
		peers:         map[types.ID]Peer{remote: fp},
		UDPListenAddr: net.JoinHostPort(ip, strconv.Itoa(port)),
	}
	require.NoError(t, tr.startUDPHeartbeat())
	t.Cleanup(tr.stopUDPHeartbeat)
	return &udpMember{tr: tr, raft: fr, recv: recv, peer: fp}
}

func newUDPPair(t *testing.T) *udpPair {
	t.Helper()
	port := freeUDPPort(t)
	p := &udpPair{port: port}
	p.a = newUDPMember(t, 1, 2, loopback2, port)
	p.b = newUDPMember(t, 2, 1, loopback3, port)
	p.a.tr.udp.setPeerURLs(2, peerURLs(t, loopback3))
	p.b.tr.udp.setPeerURLs(1, peerURLs(t, loopback2))
	return p
}

func recvWithin(t *testing.T, ch <-chan *raftpb.Message) *raftpb.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("no message within 2 s")
		return nil
	}
}

func requireNothingWithin(t *testing.T, ch <-chan *raftpb.Message, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message: %v", m)
	case <-time.After(d):
	}
}

// ---------------------------------------------------------------- off by default

func TestUDP_OffByDefaultHeartbeatsUseTheTCPPeerPath(t *testing.T) {
	peer := newFakePeer()
	tr := &Transport{
		ServerStats: stats.NewServerStats("", ""),
		peers:       map[types.ID]Peer{2: peer},
	}
	require.NoError(t, tr.startUDPHeartbeat())
	require.Nil(t, tr.udp, "no UDP state unless UDPListenAddr is set")
	tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	require.Len(t, peer.msgs, 1)
	tr.stopUDPHeartbeat() // must be a no-op
}

// ---------------------------------------------------------------- lifecycle

func TestUDP_ListenerBindsOnStartAndStopFreesThePort(t *testing.T) {
	port := freeUDPPort(t)
	addr := net.JoinHostPort(loopback2, strconv.Itoa(port))
	m := newUDPMember(t, 1, 2, loopback2, port)

	_, err := net.ListenPacket("udp", addr)
	require.Error(t, err, "the port is held while the transport runs")

	m.tr.stopUDPHeartbeat()
	m.tr.stopUDPHeartbeat() // idempotent
	pc, err := net.ListenPacket("udp", addr)
	require.NoError(t, err, "Stop must release the port")
	pc.Close()
}

func TestUDP_SendAfterStopDoesNotPanic(t *testing.T) {
	p := newUDPPair(t)
	p.a.tr.stopUDPHeartbeat()
	require.NotPanics(t, func() { p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)}) })
}

// ---------------------------------------------------------------- routing

func TestUDP_HeartbeatsAndResponsesGoByUDPNotTCP(t *testing.T) {
	p := newUDPPair(t)

	hb := udpHeartbeatMsg(1, 2)
	resp := &raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3))}
	p.a.tr.Send([]*raftpb.Message{hb})
	p.b.tr.Send([]*raftpb.Message{resp})

	got := recvWithin(t, p.b.recv)
	require.Equal(t, raftpb.MsgHeartbeat, got.GetType())
	got = recvWithin(t, p.a.recv)
	require.Equal(t, raftpb.MsgHeartbeatResp, got.GetType())

	require.Empty(t, p.a.peer.msgs, "nothing on the TCP peer path")
	require.Empty(t, p.b.peer.msgs, "nothing on the TCP peer path")
}

func TestUDP_OtherMessageTypesStayOnTCP(t *testing.T) {
	p := newUDPPair(t)
	msgs := []*raftpb.Message{
		{Type: raftpb.MsgApp.Enum(), To: new(uint64(2))},
		{Type: raftpb.MsgProp.Enum(), To: new(uint64(2))},
		{Type: raftpb.MsgVote.Enum(), To: new(uint64(2))},
		{Type: raftpb.MsgPreVote.Enum(), To: new(uint64(2))},
		{Type: raftpb.MsgAppResp.Enum(), To: new(uint64(2))},
	}
	p.a.tr.Send(msgs)
	require.Len(t, p.a.peer.msgs, len(msgs))
	requireNothingWithin(t, p.b.recv, 100*time.Millisecond)
}

func TestUDP_AllFieldsSurviveTheTrip(t *testing.T) {
	p := newUDPPair(t)
	want := udpHeartbeatMsg(1, 2)
	p.a.tr.Send([]*raftpb.Message{want})
	got := recvWithin(t, p.b.recv)
	require.True(t, proto.Equal(want, got), "sent %v, got %v", want, got)
}

func TestUDP_HeartbeatTooBigForOneDatagramGoesByTCP(t *testing.T) {
	p := newUDPPair(t)
	hb := udpHeartbeatMsg(1, 2)
	hb.Context = make([]byte, 2000)
	p.a.tr.Send([]*raftpb.Message{hb})
	require.Len(t, p.a.peer.msgs, 1)
	requireNothingWithin(t, p.b.recv, 100*time.Millisecond)
}

// ---------------------------------------------------------------- sockets

func TestUDP_OneSocketIsReusedPerPeer(t *testing.T) {
	p := newUDPPair(t)
	var dials int32
	p.a.tr.udp.dial = func(addr string) (net.Conn, error) {
		atomic.AddInt32(&dials, 1)
		return net.Dial("udp", addr)
	}
	for i := 0; i < 50; i++ {
		p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	}
	for i := 0; i < 50; i++ {
		recvWithin(t, p.b.recv)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&dials))
}

type countingConn struct {
	net.Conn
	closed *int32
}

func (c countingConn) Close() error { atomic.AddInt32(c.closed, 1); return c.Conn.Close() }

func TestUDP_UpdatingThePeerRetargetsTheSocketAndRemovingItClosesIt(t *testing.T) {
	p := newUDPPair(t)
	var dials, closed int32
	p.a.tr.udp.dial = func(addr string) (net.Conn, error) {
		atomic.AddInt32(&dials, 1)
		c, err := net.Dial("udp", addr)
		return countingConn{c, &closed}, err
	}
	p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	recvWithin(t, p.b.recv)

	// the peer moves to 127.0.0.4
	third, err := net.ListenPacket("udp", net.JoinHostPort(loopback4, strconv.Itoa(p.port)))
	require.NoError(t, err)
	defer third.Close()
	p.a.tr.udp.setPeerURLs(2, peerURLs(t, loopback4))
	require.Equal(t, int32(1), atomic.LoadInt32(&closed), "the old socket is closed")
	p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	require.NoError(t, third.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 2048)
	n, _, err := third.ReadFrom(buf)
	require.NoError(t, err, "the heartbeat must reach the new address")
	got := &raftpb.Message{}
	require.NoError(t, proto.Unmarshal(buf[:n], got))
	require.Equal(t, raftpb.MsgHeartbeat, got.GetType())
	requireNothingWithin(t, p.b.recv, 100*time.Millisecond)
	require.Equal(t, int32(2), atomic.LoadInt32(&dials))

	p.a.tr.udp.removePeer(2)
	require.Equal(t, int32(2), atomic.LoadInt32(&closed), "removing the peer closes its socket")
}

func TestUDP_SetupFailureFallsBackToTCP(t *testing.T) {
	p := newUDPPair(t)
	p.a.tr.udp.dial = func(string) (net.Conn, error) { return nil, fmt.Errorf("no route") }
	p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	require.Len(t, p.a.peer.msgs, 1, "a misconfigured UDP path must not silence heartbeats")
	requireNothingWithin(t, p.b.recv, 100*time.Millisecond)
}

func TestUDP_PeerWithoutAKnownHostFallsBackToTCP(t *testing.T) {
	m := newUDPMember(t, 1, 2, loopback2, freeUDPPort(t)) // no setPeerURLs
	m.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	require.Len(t, m.peer.msgs, 1)
}

// ---------------------------------------------------------------- Cut / Mend

func TestUDP_CutPeerDropsUDPHeartbeatsAndMendRestoresThem(t *testing.T) {
	p := newUDPPair(t)
	p.a.tr.CutPeer(2)
	p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	requireNothingWithin(t, p.b.recv, 150*time.Millisecond)
	require.Empty(t, p.a.peer.msgs, "a cut peer must not be reached by the TCP fallback either")

	p.a.tr.MendPeer(2)
	p.a.tr.Send([]*raftpb.Message{udpHeartbeatMsg(1, 2)})
	recvWithin(t, p.b.recv)
}

// ---------------------------------------------------------------- receiver validation

func TestUDP_ReceiverDropsInvalidDatagramsAndKeepsWorking(t *testing.T) {
	p := newUDPPair(t)
	// member 3 is a peer but has been removed from the cluster
	// The receive goroutine is already running: set these under the transport
	// lock, which it takes before reading them, so the writes are ordered
	// before its reads (the datagram path alone gives the race detector no edge).
	p.b.tr.mu.Lock()
	p.b.tr.peers[3] = newFakePeer()
	p.b.raft.removedID = 3
	p.b.tr.mu.Unlock()

	raw, err := net.Dial("udp", net.JoinHostPort(loopback3, strconv.Itoa(p.port)))
	require.NoError(t, err)
	defer raw.Close()
	send := func(m *raftpb.Message) {
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		_, err = raw.Write(b)
		require.NoError(t, err)
	}

	send(udpHeartbeatMsg(9, 2)) // sender is not a peer
	send(udpHeartbeatMsg(1, 5)) // addressed to another member
	send(udpHeartbeatMsg(3, 2)) // sender was removed from the cluster
	app := udpHeartbeatMsg(1, 2)
	app.Type = raftpb.MsgApp.Enum() // not a heartbeat type
	send(app)
	_, err = raw.Write([]byte("this is not a protobuf message \xff\xfe\xfd"))
	require.NoError(t, err)
	_, err = raw.Write(nil) // empty datagram
	require.NoError(t, err)
	// ordering on one loopback socket is preserved: the sentinel comes last
	sentinel := udpHeartbeatMsg(1, 2)
	sentinel.SequenceId = new(int64(999))
	send(sentinel)

	got := recvWithin(t, p.b.recv)
	require.Equal(t, int64(999), got.GetSequenceId(), "only the valid datagram is delivered")
	requireNothingWithin(t, p.b.recv, 150*time.Millisecond)
}

// ---------------------------------------------------------------- wiring into the real peer methods

func TestUDP_AddUpdateRemovePeerKeepTheUDPPeerTableInSync(t *testing.T) {
	port := freeUDPPort(t)
	tr := &Transport{
		Logger:         zaptest.NewLogger(t),
		ID:             1,
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		streamRt:       &roundTripperRecorder{},
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
		UDPListenAddr:  net.JoinHostPort(loopback2, strconv.Itoa(port)),
	}
	require.NoError(t, tr.startUDPHeartbeat())
	defer tr.Stop()

	tr.AddPeer(2, []string{"http://" + loopback3 + ":2380"})
	require.Equal(t, loopback3, tr.udp.hostFor(2))

	tr.UpdatePeer(2, []string{"http://" + loopback4 + ":2380"})
	require.Equal(t, loopback4, tr.udp.hostFor(2))

	tr.RemovePeer(2)
	require.Empty(t, tr.udp.hostFor(2))
}

func TestUDP_StopReleasesTheListener(t *testing.T) {
	port := freeUDPPort(t)
	addr := net.JoinHostPort(loopback2, strconv.Itoa(port))
	tr := &Transport{
		Logger:         zaptest.NewLogger(t),
		ID:             1,
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		streamRt:       &roundTripperRecorder{},
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
		UDPListenAddr:  addr,
	}
	require.NoError(t, tr.startUDPHeartbeat())
	tr.Stop()
	pc, err := net.ListenPacket("udp", addr)
	require.NoError(t, err, "Transport.Stop must stop the UDP listener")
	pc.Close()
}

// ---------------------------------------------------------------- host parsing

func TestUDP_PeerHostIsTakenFromTheFirstSortedURL(t *testing.T) {
	for _, tc := range []struct {
		urls []string
		want string
	}{
		{[]string{"http://10.0.0.5:2380"}, "10.0.0.5"},
		{[]string{"https://[fe80::1]:2380"}, "fe80::1"},
		// types.NewURLs sorts its input, so "first" is the lexicographically
		// first URL -- the same on every member for the same URL list.
		{[]string{"http://etcd-2.example.com:2380", "http://10.0.0.9:2380"}, "10.0.0.9"},
		{[]string{"http://localhost:2380"}, "localhost"},
	} {
		u, err := types.NewURLs(tc.urls)
		require.NoError(t, err)
		require.Equal(t, tc.want, peerUDPHost(u), "%v", tc.urls)
	}
	require.Empty(t, peerUDPHost(nil))
}
