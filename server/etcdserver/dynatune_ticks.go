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
	"time"

	"go.etcd.io/etcd/server/v3/config"
)

// Tick units (task D1c).
//
// Stock etcd ticks raft every --heartbeat-interval (ServerConfig.TickMs), and
// raft's HeartbeatTick/ElectionTick count those ticks. Dynatune computes its
// timeouts in milliseconds and stores them straight into raft's tick-counted
// timers, so with --experimental-dynatune raft is ticked once per millisecond
// and raftConfig expresses HeartbeatTick/ElectionTick in milliseconds.
// ServerConfig.TickMs/ElectionTicks keep their meaning (wall-clock based
// durations such as leases and request timeouts derive from them); only code
// that converts between raft ticks and time goes through these helpers.

// raftTickInterval is the wall-clock time between two raft ticks.
func raftTickInterval(cfg config.ServerConfig) time.Duration {
	if cfg.ExperimentalDynatune {
		return time.Millisecond
	}
	return time.Duration(cfg.TickMs) * time.Millisecond
}

// raftTicks converts a count of stock ticks (each TickMs long) into raft
// ticks: unchanged for stock, multiplied by TickMs when raft ticks every ms.
func raftTicks(cfg config.ServerConfig, stockTicks int) int {
	if cfg.ExperimentalDynatune {
		return stockTicks * int(cfg.TickMs)
	}
	return stockTicks
}

// forwardTicks is the number of raft ticks the startup fast-forward
// (adjustTicks) advances: ElectionTicks-minusStockTicks stock ticks' worth of
// time, in raft ticks.
func forwardTicks(cfg config.ServerConfig, minusStockTicks int) int {
	return raftTicks(cfg, cfg.ElectionTicks-minusStockTicks)
}

// udpListenAddr is the address the peer transport listens on for Dynatune's UDP
// heartbeats: empty (no UDP at all) unless --experimental-dynatune is set, then
// --listen-peer-udp-url or the default 0.0.0.0:2381.
func udpListenAddr(cfg config.ServerConfig) string {
	if !cfg.ExperimentalDynatune {
		return ""
	}
	if cfg.ListenPeerUDPAddr != "" {
		return cfg.ListenPeerUDPAddr
	}
	return "0.0.0.0:2381"
}

// ticksToDuration is the wall-clock time n raft ticks take.
func ticksToDuration(cfg config.ServerConfig, n int) time.Duration {
	return time.Duration(n) * raftTickInterval(cfg)
}
