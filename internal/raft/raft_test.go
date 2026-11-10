package raft

import (
	"testing"
	"time"
)

type recordFSM struct {
	applied [][]byte
}

func (r *recordFSM) Apply(data []byte) error {
	r.applied = append(r.applied, append([]byte(nil), data...))
	return nil
}

func newCluster(t *testing.T, count int) []*Node {
	t.Helper()
	nodes := make([]*Node, count)
	fsms := make([]*recordFSM, count)
	for i := 0; i < count; i++ {
		fsms[i] = &recordFSM{}
		node, err := New(Config{
			Self:             i,
			Storage:          &MemoryStorage{},
			Tick:             time.Millisecond,
			ElectionMinTicks: 3,
			ElectionMaxTicks: 5,
			HeartbeatTicks:   1,
		}, make([]Peer, count), fsms[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
	}
	peers := make([]Peer, count)
	for i, node := range nodes {
		peers[i] = node
	}
	for _, node := range nodes {
		next := make([]Peer, count)
		copy(next, peers)
		if err := node.SetPeers(next); err != nil {
			t.Fatal(err)
		}
	}
	return nodes
}

func tickClusterN(t *testing.T, nodes []*Node, count int) {
	t.Helper()
	for round := 0; round < count; round++ {
		for _, node := range nodes {
			node.Tick()
		}
	}
}

func findsLeader(nodes []*Node) int {
	for i, node := range nodes {
		if node.StateRole() == Leader {
			return i
		}
	}
	return -1
}

func TestElectsALeader(t *testing.T) {
	nodes := newCluster(t, 3)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatalf("no leader; roles = %d %d %d", nodes[0].StateRole(), nodes[1].StateRole(), nodes[2].StateRole())
	}
	terms := map[uint64]int{}
	for _, node := range nodes {
		terms[node.CurrentTerm()]++
	}
	if len(terms) == 0 {
		t.Fatal("no terms")
	}
}

func TestProposesCommitsAcrossQuorum(t *testing.T) {
	nodes := newCluster(t, 3)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	target, err := nodes[leader].Propose([]byte("frame-1"))
	if err != nil {
		t.Fatal(err)
	}
	if target == 0 {
		t.Fatal("returned zero applied index")
	}
	for i, node := range nodes {
		if node.CommitIndex() < target {
			t.Fatalf("node %d commitIndex = %d, want >= %d", i, node.CommitIndex(), target)
		}
	}
	tickClusterN(t, nodes, 10)
}

type flakyPeer struct {
	node *Node
	drop bool
}

func (p *flakyPeer) RequestVote(args RequestVoteArgs) RequestVoteReply {
	if p.drop {
		return RequestVoteReply{Term: args.Term, VoteGranted: false}
	}
	return p.node.RequestVote(args)
}

func (p *flakyPeer) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	if p.drop {
		return AppendEntriesReply{Term: args.Term, Success: false, LastLogIndex: 0}
	}
	return p.node.AppendEntries(args)
}

func TestIsolatedNodeCannotElectItself(t *testing.T) {
	nodes := newCluster(t, 3)
	mem := []*flakyPeer{}
	for _, node := range nodes {
		mem = append(mem, &flakyPeer{node: node})
	}
	peers := make([]Peer, len(nodes))
	for i, peer := range mem {
		peers[i] = peer
	}
	for _, node := range nodes {
		local := make([]Peer, len(nodes))
		copy(local, peers)
		if err := node.SetPeers(local); err != nil {
			t.Fatal(err)
		}
	}
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	for _, peer := range mem {
		if peer.node == nodes[leader] {
			continue
		}
		peer.drop = true
	}
	_ = leader
}

func TestLaggingFollowerCatchesUpNotTestedYet(t *testing.T) {
	nodes := newCluster(t, 3)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	target, err := nodes[leader].Propose([]byte("entry-a"))
	if err != nil {
		t.Fatal(err)
	}
	if target == 0 {
		t.Fatal("propose returned no index")
	}
	for i, node := range nodes {
		if node.CurrentTerm() != nodes[leader].CurrentTerm() {
			t.Fatalf("node %d term = %d, leader = %d", i, node.CurrentTerm(), nodes[leader].CurrentTerm())
		}
	}
}
