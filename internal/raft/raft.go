package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

var ErrNotLeader = errors.New("not the leader")

var ErrLeaderLost = errors.New("leader lost quorum before the entry committed")

type Entry struct {
	Term    uint64
	Command []byte
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term         uint64
	Success      bool
	MatchIndex   uint64
	LastLogIndex uint64
}

type Peer interface {
	RequestVote(args RequestVoteArgs) RequestVoteReply
	AppendEntries(args AppendEntriesArgs) AppendEntriesReply
}

type FSM interface {
	Apply([]byte) error
}

type Storage interface {
	LoadMeta() (uint64, int, error)
	SaveMeta(term uint64, votedFor int) error
	Load() ([]Entry, error)
	Append(entry Entry, index uint64) error
	TruncateFrom(index uint64) error
	Close() error
}

type Config struct {
	Self             int
	Storage          Storage
	Tick             time.Duration
	ElectionMinTicks uint64
	ElectionMaxTicks uint64
	HeartbeatTicks   uint64
	Random           *rand.Rand
}

type Node struct {
	cfg     Config
	peers   []Peer
	fsm     FSM
	storage Storage

	mu          sync.Mutex
	log         []Entry
	term        uint64
	votedFor    int
	role        Role
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64

	stop        chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
	ticks       uint64
	lastHeard   uint64
	electionAt  uint64
	heartbeatAt uint64
	wake        chan struct{}
}

func New(cfg Config, peers []Peer, fsm FSM) (*Node, error) {
	if cfg.Storage == nil {
		return nil, errors.New("raft storage is required")
	}
	if cfg.Self < 0 || cfg.Self >= len(peers) {
		return nil, fmt.Errorf("raft self index %d is outside the peer list", cfg.Self)
	}
	if peers[cfg.Self] != nil {
		return nil, errors.New("raft self must not have a peer endpoint")
	}
	if fsm == nil {
		return nil, errors.New("raft FSM is required")
	}
	if cfg.ElectionMinTicks == 0 {
		cfg.ElectionMinTicks = 10
	}
	if cfg.ElectionMaxTicks == 0 {
		cfg.ElectionMaxTicks = cfg.ElectionMinTicks * 2
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.Random == nil {
		cfg.Random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	term, votedFor, err := cfg.Storage.LoadMeta()
	if err != nil {
		return nil, err
	}
	entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		peers:       peers,
		fsm:         fsm,
		storage:     cfg.Storage,
		term:        term,
		votedFor:    votedFor,
		log:         entries,
		nextIndex:   make([]uint64, len(peers)),
		matchIndex:  make([]uint64, len(peers)),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		heartbeatAt: 0,
		wake:        make(chan struct{}, 1),
	}
	n.resetElectionDeadlineLocked()
	n.wake = make(chan struct{}, 1)
	return n, nil
}

func (n *Node) Start() error {
	go n.run()
	return nil
}

func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.Tick()
		case <-n.wake:
		}
	}
}

func (n *Node) Close() {
	n.stopOnce.Do(func() {
		close(n.stop)
		<-n.done
		n.mu.Lock()
		_ = n.storage.Close()
		n.mu.Unlock()
	})
}

func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ticks++
	switch n.role {
	case Leader:
		if n.ticks >= n.heartbeatAt {
			n.heartbeatAt = n.ticks + n.cfg.HeartbeatTicks
			n.broadcastAppendEntriesLocked()
		}
	default:
		if n.ticks >= n.electionAt {
			n.startElectionLocked()
		}
	}
	n.applyCommittedLocked()
}

func (n *Node) resetElectionDeadlineLocked() {
	span := n.cfg.ElectionMaxTicks - n.cfg.ElectionMinTicks + 1
	if span == 0 {
		span = 1
	}
	n.electionAt = n.ticks + n.cfg.ElectionMinTicks + uint64(n.cfg.Random.Int63n(int64(span)))
}

func (n *Node) stepdownLocked(term uint64) {
	if term > n.term {
		n.term = term
		n.votedFor = -1
		n.storage.SaveMeta(n.term, n.votedFor)
	}
	n.role = Follower
	n.resetElectionDeadlineLocked()
}

func (n *Node) lastIndexLocked() uint64 {
	return uint64(len(n.log))
}

func (n *Node) lastTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) resetElectionAtLocked() {
	n.lastHeard = n.ticks
	n.resetElectionDeadlineLocked()
}

func (n *Node) logTermAtLocked(index uint64) uint64 {
	if index == 0 || index > uint64(len(n.log)) {
		return 0
	}
	return n.log[index-1].Term
}

func (n *Node) quorum() uint64 {
	return uint64(len(n.peers)/2 + 1)
}

func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term > n.term {
		n.stepdownLocked(args.Term)
	}
	if args.Term < n.term {
		return RequestVoteReply{Term: n.term, VoteGranted: false}
	}
	upToDate := args.LastLogTerm > n.lastTermLocked() || (args.LastLogTerm == n.lastTermLocked() && args.LastLogIndex >= n.lastIndexLocked())
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		_ = n.storage.SaveMeta(n.term, n.votedFor)
		return RequestVoteReply{Term: n.term, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.term, VoteGranted: false}
}

func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.term {
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	n.role = Follower
	n.stepdownLocked(args.Term)
	n.resetElectionDeadlineLocked()
	prevIdx, prevTerm := args.PrevLogIndex, args.PrevLogTerm
	if prevIdx > n.lastIndexLocked() {
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	if prevIdx > 0 && n.logTermAtLocked(prevIdx) != prevTerm {
		n.truncateFromLocked(prevIdx)
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	inserting := false
	leaderIndex := prevIdx + 1
	for offset, entry := range args.Entries {
		idx := prevIdx + 1 + uint64(offset)
		if idx <= n.lastIndexLocked() {
			if n.logTermAtLocked(idx) != entry.Term {
				n.truncateFromLocked(idx)
				inserting = true
				n.appendEntryLocked(entry)
				leaderIndex = idx
			}
		} else {
			inserting = true
			n.appendEntryLocked(entry)
			leaderIndex = idx
		}
	}
	_ = inserting
	_ = leaderIndex
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = args.LeaderCommit
		if n.commitIndex > n.lastIndexLocked() {
			n.commitIndex = n.lastIndexLocked()
		}
	}
	n.applyCommittedLocked()
	return AppendEntriesReply{Term: n.term, Success: true, MatchIndex: prevIdx + uint64(len(args.Entries))}
}

func (n *Node) broadcastAppendEntriesLocked() {
	if n.role != Leader {
		return
	}
	argsList := make([]AppendEntriesArgs, len(n.peers))
	for i, peer := range n.peers {
		if peer == nil || i == n.cfg.Self {
			continue
		}
		next := n.nextIndex[i]
		if next == 0 {
			next = n.lastIndexLocked() + 1
		}
		prev := next - 1
		prevTerm := n.logTermAtLocked(prev)
		var entries []Entry
		if next-1 < n.lastIndexLocked() {
			entries = append([]Entry(nil), n.log[next-1:]...)
		}
		argsList[i] = AppendEntriesArgs{
			Term:         n.term,
			LeaderID:     n.cfg.Self,
			PrevLogIndex: prev,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}
	}
	n.mu.Unlock()
	type result struct {
		index int
		reply AppendEntriesReply
	}
	results := make(chan result, len(n.peers))
	active := 0
	for i, peer := range n.peers {
		if peer == nil || i == n.cfg.Self {
			continue
		}
		active++
		go func(index int, peer Peer, args AppendEntriesArgs) {
			defer func() {
				if r := recover(); r != nil {
					results <- result{index: index, reply: AppendEntriesReply{Term: args.Term, Success: false}}
				}
			}()
			results <- result{index: index, reply: peer.AppendEntries(args)}
		}(i, peer, argsList[i])
	}
	for received := 0; received < active; received++ {
		got := <-results
		i := got.index
		args := argsList[i]
		n.mu.Lock()
		if got.reply.Term > n.term {
			n.stepdownLocked(got.reply.Term)
			n.mu.Unlock()
			continue
		}
		if !got.reply.Success {
			if next := n.nextIndex[i]; next > 1 {
				n.nextIndex[i] = next - 1
			}
			n.mu.Unlock()
			continue
		}
		n.matchIndex[i] = args.PrevLogIndex + uint64(len(args.Entries))
		n.nextIndex[i] = n.matchIndex[i] + 1
		advanced := false
		c := n.lastIndexLocked()
		v := n.commitIndex
		for ; c > v; c-- {
			if n.logTermAtLocked(c) != n.term {
				continue
			}
			matchCount := uint64(1)
			for j, otherPeer := range n.peers {
				if otherPeer == nil || j == n.cfg.Self {
					continue
				}
				if n.matchIndex[j] >= c {
					matchCount++
				}
			}
			if matchCount >= n.quorum() {
				v = c
				advanced = true
				break
			}
		}
		if advanced {
			n.commitIndex = v
		}
		n.applyCommittedLocked()
		n.mu.Unlock()
	}
	n.mu.Lock()
}

func (n *Node) startElectionLocked() {
	n.term++
	n.votedFor = n.cfg.Self
	if err := n.storage.SaveMeta(n.term, n.votedFor); err != nil {
		_ = err
	}
	n.role = Candidate
	n.resetElectionDeadlineLocked()
	argsList := make([]RequestVoteArgs, len(n.peers))
	for i, peer := range n.peers {
		if peer == nil || i == n.cfg.Self {
			continue
		}
		argsList[i] = RequestVoteArgs{
			Term:         n.term,
			CandidateID:  n.cfg.Self,
			LastLogIndex: n.lastIndexLocked(),
			LastLogTerm:  n.lastTermLocked(),
		}
	}
	n.mu.Unlock()
	votes := uint64(1)
	for i, peer := range n.peers {
		if peer == nil || i == n.cfg.Self {
			continue
		}
		args := argsList[i]
		reply := peer.RequestVote(args)
		n.mu.Lock()
		if reply.Term > n.term {
			n.stepdownLocked(reply.Term)
			n.mu.Unlock()
			return
		}
		if n.role == Candidate && n.term == args.Term && reply.VoteGranted {
			votes++
			if votes >= n.quorum() {
				n.becomeLeaderLocked()
				return
			}
		}
		n.mu.Unlock()
	}
	n.mu.Lock()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	for i := range n.peers {
		if i == n.cfg.Self {
			continue
		}
		n.matchIndex[i] = 0
		n.nextIndex[i] = n.lastIndexLocked() + 1
	}
	n.heartbeatAt = n.ticks
	n.broadcastAppendEntriesLocked()
}

func (n *Node) appendEntryLocked(entry Entry) {
	_ = n.storage.Append(entry, uint64(len(n.log))+1)
	n.log = append(n.log, entry)
}

func (n *Node) truncateFromLocked(index uint64) {
	if uint64(len(n.log)) < index {
		return
	}
	_ = n.storage.TruncateFrom(index - 1)
	n.log = n.log[:index-1]
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if err := n.fsm.Apply(n.log[n.lastApplied-1].Command); err != nil {
			_ = err
		}
	}
}

func (n *Node) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	entry := Entry{Term: n.term, Command: command}
	if err := n.storage.Append(entry, uint64(len(n.log))+1); err != nil {
		n.mu.Unlock()
		return 0, err
	}
	n.log = append(n.log, entry)
	target := uint64(len(n.log))
	n.broadcastAppendEntriesLocked()
	n.mu.Unlock()
	for {
		select {
		case <-n.wake:
		default:
			n.Tick()
			time.Sleep(200 * time.Microsecond)
		}
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return 0, ErrLeaderLost
		}
		if n.commitIndex >= target {
			n.mu.Unlock()
			return target, nil
		}
		n.mu.Unlock()
	}
}

func (n *Node) Status() (role Role, term uint64, commitIndex uint64, lastIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.term, n.commitIndex, n.lastIndexLocked()
}

func (n *Node) SetPeers(peers []Peer) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(peers) != len(n.peers) {
		return fmt.Errorf("peer list must contain %d entries", len(n.peers))
	}
	n.peers = peers
	return nil
}

func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIndexLocked()
}

func (n *Node) LastLogTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastTermLocked()
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

func (n *Node) StateRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}
