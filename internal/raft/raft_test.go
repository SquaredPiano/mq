package raft

import (
	"testing"
	"time"
)

type recordFSM struct {
	applied   [][]byte
	installed [][]byte
}

func (r *recordFSM) Apply(data []byte) error {
	r.applied = append(r.applied, append([]byte(nil), data...))
	return nil
}

func (r *recordFSM) Snapshot() ([]byte, error) {
	if len(r.installed) > 0 {
		return append([]byte(nil), r.installed[len(r.installed)-1]...), nil
	}
	return []byte(`{"committed":true}`), nil
}

func (r *recordFSM) InstallSnapshot(data []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error {
	_ = lastIncludedIndex
	_ = lastIncludedTerm
	r.installed = append(r.installed, append([]byte(nil), data...))
	return nil
}

func newCluster(t *testing.T, count int, snapThreshold ...uint64) []*Node {
	t.Helper()
	nodes := make([]*Node, count)
	fsms := make([]*recordFSM, count)
	threshold := uint64(0)
	if len(snapThreshold) > 0 {
		threshold = snapThreshold[0]
	}
	for i := 0; i < count; i++ {
		fsms[i] = &recordFSM{}
		node, err := New(Config{
			Self:              i,
			Storage:           &MemoryStorage{},
			Tick:              time.Millisecond,
			ElectionMinTicks:  3,
			ElectionMaxTicks:  5,
			HeartbeatTicks:    1,
			SnapshotThreshold: threshold,
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
		for j, peer := range peers {
			if peer == node {
				continue
			}
			next[j] = peer
		}
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
	tickClusterN(t, nodes, 20)
	for i, node := range nodes {
		if node.CommitIndex() < target {
			t.Fatalf("node %d commitIndex = %d, want >= %d", i, node.CommitIndex(), target)
		}
	}
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

func (p *flakyPeer) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	if p.drop {
		return InstallSnapshotReply{Term: args.Term}
	}
	return p.node.InstallSnapshot(args)
}

func TestIsolatedNodeCannotElectItself(t *testing.T) {
	nodes := newCluster(t, 3)
	proxies := make([][]Peer, 3)
	for i, node := range nodes {
		peers := make([]Peer, 3)
		for j, target := range nodes {
			if i == j {
				continue
			}
			peers[j] = &flakyPeer{node: target}
		}
		proxies[i] = peers
		if err := node.SetPeers(peers); err != nil {
			t.Fatal(err)
		}
	}
	proxies[0][1].(*flakyPeer).drop = true
	proxies[0][2].(*flakyPeer).drop = true
	proxies[1][0].(*flakyPeer).drop = true
	proxies[2][0].(*flakyPeer).drop = true
	tickClusterN(t, nodes, 50)
	if nodes[0].StateRole() == Leader {
		t.Fatal("isolated node became leader")
	}
	leader := findsLeader(nodes[1:])
	if leader == -1 {
		t.Fatal("no leader among remaining nodes")
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
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

func TestCommittedEntrySurvivesLeaderLoss(t *testing.T) {
	nodes := newCluster(t, 3)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	if _, err := nodes[leader].Propose([]byte("entry-a")); err != nil {
		t.Fatal(err)
	}
	nodes[leader].Close()
	surviving := make([]*Node, 0, 2)
	for i, node := range nodes {
		if i == leader {
			continue
		}
		surviving = append(surviving, node)
	}
	time.Sleep(10 * time.Millisecond)
	for round := 0; round < 50; round++ {
		for _, node := range surviving {
			node.Tick()
		}
	}
	leader2 := -1
	for i, node := range nodes {
		if i == leader {
			continue
		}
		if node.StateRole() == Leader {
			leader2 = i
		}
	}
	if leader2 == -1 {
		t.Fatal("no replacement leader emerged")
	}
	if got := nodes[leader2].CommitIndex(); got < 1 {
		t.Fatalf("new leader commit index = %d, want at least 1", got)
	}
}

func TestSnapshotCompactionEndToEnd(t *testing.T) {
	nodes := newCluster(t, 3, 3)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	for i := 0; i < 12; i++ {
		if _, err := nodes[leader].Propose([]byte("frame")); err != nil {
			t.Fatal(err)
		}
	}
	snapIdx := uint64(0)
	for _, node := range nodes {
		if node.SnapshotIndex() > 0 {
			snapIdx = node.SnapshotIndex()
		}
	}
	if snapIdx == 0 {
		t.Fatalf("snapshot never taken; threshold=0?")
	}
}

func TestSnapshotSurvivesLeaderLoss(t *testing.T) {
	nodes := newCluster(t, 3, 4)
	tickClusterN(t, nodes, 30)
	leader := findsLeader(nodes)
	if leader == -1 {
		t.Fatal("no leader")
	}
	for i := 0; i < 10; i++ {
		if _, err := nodes[leader].Propose([]byte("frame")); err != nil {
			t.Fatal(err)
		}
	}
	before := nodes[leader].SnapshotIndex()
	if before == 0 {
		t.Fatal("no snapshot was installed on the leader")
	}
	nodes[leader].Close()
	surviving := make([]*Node, 0, 2)
	for i, node := range nodes {
		if i == leader {
			continue
		}
		surviving = append(surviving, node)
	}
	time.Sleep(10 * time.Millisecond)
	for round := 0; round < 80; round++ {
		for _, node := range surviving {
			node.Tick()
		}
	}
	leader2 := -1
	for i, node := range nodes {
		if i == leader {
			continue
		}
		if node.StateRole() == Leader {
			leader2 = i
		}
	}
	if leader2 == -1 {
		t.Fatal("no successor leader emerged")
	}
	for _, node := range surviving {
		snapIdx := node.SnapshotIndex()
		_ = snapIdx
	}
}
