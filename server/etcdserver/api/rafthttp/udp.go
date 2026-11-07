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
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
)

// UDP heartbeats for Dynatune (task D1b).
//
// Dynatune estimates packet loss from gaps in heartbeat sequence numbers and
// round-trip time from heartbeat round trips. Over TCP a lost heartbeat is
// retransmitted, so loss would be invisible and RTT would absorb the
// retransmission delay; the heartbeats therefore travel by UDP, exactly as in
// the reference implementation (distsys-lab/dynatune). This is enabled only
// when Transport.UDPListenAddr is set (--experimental-dynatune); otherwise no
// socket exists and Transport behaves as stock.
//
// Differences from the reference, all deliberate:
//   - gated by a flag (the reference is always on);
//   - the destination port is not a hard-coded constant: it is the peer's
//     peer-URL port plus the local member's (UDP listen port - peer-URL port).
//     With the usual layout (peer 2380 and UDP 2381 on every member, one host
//     per member) that is the same UDP port for everyone, exactly as in the
//     reference; members that share a host (the robustness harness) can each
//     use their own pair of ports. Whenever either port cannot be read the
//     local UDP listen port is used for every peer (the reference's rule);
//   - one socket is reused per peer (the reference dials and closes one per
//     heartbeat);
//   - received datagrams are validated (the reference accepts anything): they
//     must decode, be a heartbeat or heartbeat response, be addressed to this
//     member, and come from a current, non-removed peer;
//   - if the UDP path cannot be set up for a peer (no known host, dial
//     failure) or the message does not fit one datagram, the heartbeat goes by
//     TCP instead of vanishing; a datagram that is merely lost is never
//     retried;
//   - CutPeer/MendPeer also cut and restore the UDP path;
//   - the listener and every socket are closed on Stop.
//
// Known limitation, shared with the reference: UDP heartbeats are neither
// TLS-protected nor authenticated.

const (
	// udpMaxDatagram: a message whose encoding is larger goes by TCP. Well
	// below the usual 1500-byte Ethernet MTU so datagrams are never fragmented.
	udpMaxDatagram = 1400
	udpReadBuffer  = 64 * 1024
)

type udpHeartbeat struct {
	lg   *zap.Logger
	t    *Transport
	conn net.PacketConn
	port string // the local listen port; the destination port when no offset applies

	// offset is (local UDP port - local peer-URL port) when both are known; a
	// peer's UDP port is its peer-URL port plus offset (see peerUDPPort).
	offset    int
	hasOffset bool

	// dial opens the per-peer socket; replaceable in tests.
	dial func(addr string) (net.Conn, error)

	mu     sync.Mutex
	hosts  map[types.ID]string
	ports  map[types.ID]string // per-peer destination port
	socks  map[types.ID]net.Conn
	paused map[types.ID]bool
	closed bool
	wg     sync.WaitGroup
}

func isUDPHeartbeat(m *raftpb.Message) bool {
	return m.GetType() == raftpb.MsgHeartbeat || m.GetType() == raftpb.MsgHeartbeatResp
}

// peerUDPHost is the host used to reach a peer's UDP listener: the host of the
// first peer URL.
func peerUDPHost(urls types.URLs) string {
	if len(urls) == 0 {
		return ""
	}
	return urls[0].Hostname()
}

// udpPortOffset is the local UDP listen port minus the port of the local first
// peer URL; ok is false when either is missing or not a number.
func udpPortOffset(local types.URLs, udpPort string) (int, bool) {
	if len(local) == 0 {
		return 0, false
	}
	pp, err := strconv.Atoi(local[0].Port())
	if err != nil {
		return 0, false
	}
	up, err := strconv.Atoi(udpPort)
	if err != nil {
		return 0, false
	}
	return up - pp, true
}

// peerUDPPort is the port of a peer's UDP listener: its first peer URL's port
// plus offset. With no usable offset, peer port or result in 1..65535 it is
// fallback (the local UDP port, the reference's rule).
func peerUDPPort(peer types.URLs, offset int, hasOffset bool, fallback string) string {
	if !hasOffset || len(peer) == 0 {
		return fallback
	}
	pp, err := strconv.Atoi(peer[0].Port())
	if err != nil {
		return fallback
	}
	if p := pp + offset; p >= 1 && p <= 65535 {
		return strconv.Itoa(p)
	}
	return fallback
}

// startUDPHeartbeat opens the UDP listener if Transport.UDPListenAddr is set.
func (t *Transport) startUDPHeartbeat() error {
	if t.UDPListenAddr == "" || t.udp != nil {
		return nil
	}
	pc, err := net.ListenPacket("udp", t.UDPListenAddr)
	if err != nil {
		return err
	}
	_, port, err := net.SplitHostPort(pc.LocalAddr().String())
	if err != nil {
		pc.Close()
		return err
	}
	lg := t.Logger
	if lg == nil {
		lg = zap.NewNop()
	}
	offset, hasOffset := udpPortOffset(t.URLs, port)
	u := &udpHeartbeat{
		lg:        lg,
		t:         t,
		conn:      pc,
		port:      port,
		offset:    offset,
		hasOffset: hasOffset,
		dial:      func(addr string) (net.Conn, error) { return net.Dial("udp", addr) },
		hosts:     make(map[types.ID]string),
		ports:     make(map[types.ID]string),
		socks:     make(map[types.ID]net.Conn),
		paused:    make(map[types.ID]bool),
	}
	t.udp = u
	u.wg.Add(1)
	go u.readLoop()
	lg.Info("listening for UDP heartbeats", zap.String("address", pc.LocalAddr().String()))
	return nil
}

// stopUDPHeartbeat closes the listener and all sockets. Safe to call more than
// once and when UDP was never started. It must not be called with t.mu held:
// it waits for the read loop, which takes t.mu to validate senders.
func (t *Transport) stopUDPHeartbeat() {
	if t.udp != nil {
		t.udp.close()
	}
}

func (u *udpHeartbeat) close() {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	u.closed = true
	for id, c := range u.socks {
		c.Close()
		delete(u.socks, id)
	}
	u.mu.Unlock()
	u.conn.Close()
	u.wg.Wait()
}

func (u *udpHeartbeat) readLoop() {
	defer u.wg.Done()
	buf := make([]byte, udpReadBuffer)
	for {
		n, _, err := u.conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			u.mu.Lock()
			closed := u.closed
			u.mu.Unlock()
			if closed {
				return
			}
			u.lg.Debug("UDP heartbeat read error", zap.Error(err))
			time.Sleep(10 * time.Millisecond) // do not spin on a persistent error
			continue
		}
		u.handle(buf[:n])
	}
}

// handle validates one datagram and passes a valid heartbeat to raft.
func (u *udpHeartbeat) handle(data []byte) {
	m := &raftpb.Message{}
	if err := proto.Unmarshal(data, m); err != nil || !isUDPHeartbeat(m) {
		return
	}
	if m.GetTo() != uint64(u.t.ID) {
		return
	}
	u.t.mu.RLock()
	_, known := u.t.peers[types.ID(m.GetFrom())]
	u.t.mu.RUnlock()
	if !known || u.t.Raft.IsIDRemoved(m.GetFrom()) {
		return
	}
	if err := u.t.Raft.Process(context.Background(), m); err != nil {
		u.lg.Debug("failed to process UDP heartbeat", zap.Error(err))
	}
}

// send transmits m to peer `to` by UDP. It returns true if the message is
// handled -- written, lost on the wire, or deliberately dropped for a cut peer
// -- and false if the UDP path is unavailable for this message (closed, no
// known host, socket setup failed, too big), in which case the caller sends it
// over TCP. Safe on a nil receiver.
func (u *udpHeartbeat) send(to types.ID, m *raftpb.Message) bool {
	if u == nil {
		return false
	}
	data, err := proto.Marshal(m)
	if err != nil || len(data) > udpMaxDatagram {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return false
	}
	if u.paused[to] {
		return true
	}
	host := u.hosts[to]
	if host == "" {
		return false
	}
	conn := u.socks[to]
	if conn == nil {
		port := u.ports[to]
		if port == "" {
			port = u.port
		}
		c, err := u.dial(net.JoinHostPort(host, port))
		if err != nil {
			u.lg.Warn("cannot open UDP heartbeat socket; using TCP",
				zap.String("remote-peer-id", to.String()), zap.String("host", host), zap.Error(err))
			return false
		}
		u.socks[to] = c
		conn = c
	}
	if _, err := conn.Write(data); err != nil {
		// e.g. ICMP "port unreachable" surfacing on a connected socket: the
		// heartbeat is lost, like any other; reopen the socket next time.
		u.lg.Debug("UDP heartbeat write failed", zap.String("remote-peer-id", to.String()), zap.Error(err))
		conn.Close()
		delete(u.socks, to)
	}
	return true
}

// setPeerURLs records where a peer's UDP listener is; a changed host closes the
// peer's socket so the next heartbeat reaches the new address. Safe on nil.
func (u *udpHeartbeat) setPeerURLs(id types.ID, urls types.URLs) {
	if u == nil {
		return
	}
	host := peerUDPHost(urls)
	port := peerUDPPort(urls, u.offset, u.hasOffset, u.port)
	u.mu.Lock()
	defer u.mu.Unlock()
	if old := u.hosts[id]; old != host || u.ports[id] != port {
		if c := u.socks[id]; c != nil {
			c.Close()
			delete(u.socks, id)
		}
	}
	if host == "" {
		delete(u.hosts, id)
		delete(u.ports, id)
		return
	}
	u.hosts[id] = host
	u.ports[id] = port
}

// removePeer forgets a peer and closes its socket. Safe on nil.
func (u *udpHeartbeat) removePeer(id types.ID) {
	if u == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if c := u.socks[id]; c != nil {
		c.Close()
		delete(u.socks, id)
	}
	delete(u.hosts, id)
	delete(u.ports, id)
	delete(u.paused, id)
}

// hostFor returns the recorded UDP host of a peer ("" if unknown). Safe on nil.
func (u *udpHeartbeat) hostFor(id types.ID) string {
	if u == nil {
		return ""
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hosts[id]
}

func (u *udpHeartbeat) cut(id types.ID) {
	if u == nil {
		return
	}
	u.mu.Lock()
	u.paused[id] = true
	u.mu.Unlock()
}

func (u *udpHeartbeat) mend(id types.ID) {
	if u == nil {
		return
	}
	u.mu.Lock()
	delete(u.paused, id)
	u.mu.Unlock()
}
