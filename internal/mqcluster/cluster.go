package mqcluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"mq/internal/broker"
	"mq/internal/raft"
	"mq/internal/wal"
)

type Node struct {
	Broker *broker.Broker
	Raft   *raft.Node
}

func Open(dir string, peers []string, selfIndex int, raftToken string, heartbeat time.Duration) (*Node, error) {
	if len(peers) == 0 {
		return nil, errors.New("raft peers are required")
	}
	if selfIndex < 0 || selfIndex >= len(peers) {
		return nil, fmt.Errorf("raft self index %d is outside peers", selfIndex)
	}
	if raftToken == "" {
		return nil, errors.New("raft token must not be empty")
	}
	cfg := raft.Config{
		Self:             selfIndex,
		Tick:             heartbeat,
		HeartbeatTicks:   1,
		ElectionMinTicks: 10,
		ElectionMaxTicks: 20,
	}
	for i, peerURL := range peers {
		parsed, err := url.Parse(peerURL)
		if err != nil {
			return nil, fmt.Errorf("raft peer %d has invalid URL %q: %w", i, peerURL, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("raft peer %d URL must use http or https, got %q", i, peerURL)
		}
		if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("raft peer %d URL %q must not contain user info, query, or fragment", i, peerURL)
		}
		if parsed.Scheme == "http" && !isLoopback(parsed.Hostname()) {
			return nil, fmt.Errorf("raft peer %d over plain HTTP is only allowed to loopback, got %q", i, peerURL)
		}
		for j := 0; j < i; j++ {
			if peers[j] == peerURL {
				return nil, fmt.Errorf("raft peer %d is a duplicate of peer %d", i, j)
			}
		}
	}
	cfgStorage, err := raft.OpenFileStorage(dir)
	if err != nil {
		return nil, fmt.Errorf("open raft storage: %w", err)
	}
	cfg.Storage = cfgStorage
	b := broker.New()
	peersForRaft := make([]raft.Peer, len(peers))
	for i, url := range peers {
		if i == selfIndex {
			continue
		}
		peersForRaft[i] = raft.HTTPPeer{
			BaseURL: url,
			Token:   raftToken,
			Client:  &http.Client{Timeout: 5 * time.Second},
		}
	}
	node, err := raft.New(cfg, peersForRaft, broker.NewRaftFSM(b))
	if err != nil {
		return nil, err
	}
	b.SetPropose(func(entries []wal.Entry) error {
		payload, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		if _, err := node.Propose(payload); err != nil {
			return err
		}
		return nil
	})
	if err := node.Start(); err != nil {
		_ = cfgStorage.Close()
		return nil, err
	}
	return &Node{Broker: b, Raft: node}, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (n *Node) Role() raft.Role {
	return n.Raft.StateRole()
}

func (n *Node) CurrentTerm() uint64 {
	return n.Raft.CurrentTerm()
}

func (n *Node) Close() error {
	if n.Raft != nil {
		n.Raft.Close()
	}
	if n.Broker != nil {
		return n.Broker.Close()
	}
	return nil
}
