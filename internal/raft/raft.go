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
var ErrLeaderTimeout = errors.New("timed out waiting for the entry to commit")

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

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
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
	InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

type FSM interface {
	Apply([]byte) error
	Snapshot() ([]byte, error)
	InstallSnapshot(data []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error
}

type Storage interface {
	LoadMeta() (uint64, int, error)
	SaveMeta(term uint64, votedFor int) error
	Load() ([]Entry, error)
	Append(entry Entry, index uint64) error
	TruncateFrom(index uint64) error
	TruncatePrefix(count uint64) error
	LoadSnapshot() (*SnapshotRecord, error)
	WriteSnapshot(record SnapshotRecord) error
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

	SnapshotThreshold uint64
}

type Node struct {
	cfg     Config
	peers   []Peer
	fsm     FSM
	started bool
	storage Storage

	mu            sync.Mutex
	log           []Entry
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte
	term          uint64
	votedFor      int
	role          Role
	commitIndex   uint64
	lastApplied   uint64
	nextIndex     []uint64
	matchIndex    []uint64

	stop        chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
	fatalErr    error
	ticks       uint64
	electionAt  uint64
	heartbeatAt uint64
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
	}
	n.loadSnapshotLocked()
	n.resetElectionDeadlineLocked()
	return n, nil
}

func (n *Node) loadSnapshotLocked() {
	snapshot, err := n.storage.LoadSnapshot()
	if err != nil || snapshot == nil {
		return
	}
	n.snapshotIndex = snapshot.Index
	n.snapshotTerm = snapshot.Term
	n.snapshotData = snapshot.Data
	n.commitIndex = snapshot.Index
	n.lastApplied = snapshot.Index
	if err := n.fsm.InstallSnapshot(snapshot.Data, snapshot.Index, snapshot.Term); err != nil {
		n.haltLocked(err)
		return
	}
	for i := range n.peers {
		if n.peers[i] == nil || i == n.cfg.Self {
			continue
		}
		n.nextIndex[i] = snapshot.Index + 1
	}
}

func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return nil
	}
	n.started = true
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

		}
	}
}

func (n *Node) Close() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		started := n.started
		n.mu.Unlock()
		if started {
			close(n.stop)
			<-n.done
		}
		n.mu.Lock()
		_ = n.storage.Close()
		n.mu.Unlock()
	})
}

func (n *Node) haltLocked(err error) {
	if n.fatalErr == nil {
		n.fatalErr = err
	}
}

var ErrFatal = errors.New("raft node halted after a fatal error")

func (n *Node) FatalErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fatalErr
}

func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fatalErr != nil {
		return
	}
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
		if err := n.storage.SaveMeta(term, -1); err != nil {
			n.haltLocked(err)
			return
		}
		n.term = term
		n.votedFor = -1
	}
	n.role = Follower
	n.resetElectionDeadlineLocked()
}

func (n *Node) lastIndexLocked() uint64 {
	return n.snapshotIndex + uint64(len(n.log))
}

func (n *Node) lastTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) logTermAtLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	if index > n.snapshotIndex && index <= n.lastIndexLocked() {
		return n.log[index-n.snapshotIndex-1].Term
	}
	return 0
}

func (n *Node) entryAtLocked(index uint64) (Entry, bool) {
	if index == 0 || index <= n.snapshotIndex {
		return Entry{}, false
	}
	pos := index - n.snapshotIndex - 1
	if pos >= uint64(len(n.log)) {
		return Entry{}, false
	}
	return n.log[pos], true
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (n *Node) quorum() uint64 {
	return uint64(len(n.peers)/2 + 1)
}

func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fatalErr != nil {
		return RequestVoteReply{}
	}
	if args.Term > n.term {
		if err := n.storage.SaveMeta(args.Term, -1); err != nil {
			n.haltLocked(err)
			return RequestVoteReply{}
		}
		n.stepdownLocked(args.Term)
	}
	if args.Term < n.term {
		return RequestVoteReply{Term: n.term, VoteGranted: false}
	}
	upToDate := args.LastLogTerm > n.lastTermLocked() || (args.LastLogTerm == n.lastTermLocked() && args.LastLogIndex >= n.lastIndexLocked())
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		if err := n.storage.SaveMeta(n.term, args.CandidateID); err != nil {
			n.haltLocked(err)
			return RequestVoteReply{}
		}
		n.votedFor = args.CandidateID
		n.resetElectionDeadlineLocked()
		return RequestVoteReply{Term: n.term, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.term, VoteGranted: false}
}

func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fatalErr != nil {
		return AppendEntriesReply{}
	}
	if args.Term < n.term {
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	n.role = Follower
	if args.Term > n.term {
		if err := n.storage.SaveMeta(args.Term, -1); err != nil {
			n.haltLocked(err)
			return AppendEntriesReply{}
		}
		n.term = args.Term
	}
	n.resetElectionDeadlineLocked()
	prevIdx, prevTerm := args.PrevLogIndex, args.PrevLogTerm
	if prevIdx > n.lastIndexLocked() {
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	if prevIdx > 0 && n.logTermAtLocked(prevIdx) != prevTerm {
		if prevIdx <= n.commitIndex {
			n.haltLocked(errors.New("leader wants to overwrite a committed entry"))
			return AppendEntriesReply{}
		}
		if err := n.truncateFromLocked(prevIdx); err != nil {
			n.haltLocked(err)
			return AppendEntriesReply{}
		}
		return AppendEntriesReply{Term: n.term, Success: false, LastLogIndex: n.lastIndexLocked()}
	}
	for offset, entry := range args.Entries {
		idx := prevIdx + 1 + uint64(offset)
		if idx <= n.lastIndexLocked() {
			if n.logTermAtLocked(idx) != entry.Term {
				if idx <= n.commitIndex {
					n.haltLocked(errors.New("leader wants to overwrite a committed entry"))
					return AppendEntriesReply{}
				}
				if err := n.truncateFromLocked(idx); err != nil {
					n.haltLocked(err)
					return AppendEntriesReply{}
				}
				if err := n.appendEntryLocked(entry); err != nil {
					n.haltLocked(err)
					return AppendEntriesReply{}
				}
			}
			continue
		}
		if err := n.appendEntryLocked(entry); err != nil {
			n.haltLocked(err)
			return AppendEntriesReply{}
		}
	}
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = args.LeaderCommit
		if n.commitIndex > n.lastIndexLocked() {
			n.commitIndex = n.lastIndexLocked()
		}
	}
	n.applyCommittedLocked()
	return AppendEntriesReply{Term: n.term, Success: true, MatchIndex: prevIdx + uint64(len(args.Entries))}
}

func (n *Node) sendSnapshotLocked(i int, peer Peer) {
	if n.snapshotIndex == 0 {
		return
	}
	n.mu.Unlock()
	args := InstallSnapshotArgs{
		Term:              n.term,
		LeaderID:          n.cfg.Self,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              append([]byte(nil), n.snapshotData...),
	}
	reply := peer.InstallSnapshot(args)
	n.mu.Lock()
	if reply.Term > n.term {
		n.stepdownLocked(reply.Term)
		return
	}
	n.matchIndex[i] = n.snapshotIndex
	n.nextIndex[i] = n.snapshotIndex + 1
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
		if next <= n.snapshotIndex && n.snapshotIndex > 0 {
			n.sendSnapshotLocked(i, peer)
			continue
		}
		prev := next - 1
		prevTerm := n.logTermAtLocked(prev)
		var entries []Entry
		if next-1 < n.lastIndexLocked() {
			last := n.lastIndexLocked()
			if last-(next-1) > 64 {
				last = next - 1 + 64
			}
			start := next - n.snapshotIndex - 1
			end := last - n.snapshotIndex
			entries = append([]Entry(nil), n.log[start:end]...)
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
				next = min(next, got.reply.LastLogIndex+1)
				if next > 1 {
					n.nextIndex[i] = next - 1
				}
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
	if err := n.storage.SaveMeta(n.term, n.cfg.Self); err != nil {
		n.haltLocked(err)
		return
	}
	n.votedFor = n.cfg.Self
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
	n.matchIndex = make([]uint64, len(n.peers))
	n.nextIndex = make([]uint64, len(n.peers))
	for i := range n.peers {
		if i == n.cfg.Self {
			continue
		}
		n.matchIndex[i] = 0
		n.nextIndex[i] = n.lastIndexLocked() + 1
	}
	noop := Entry{Term: n.term}
	if err := n.storage.Append(noop, uint64(len(n.log))+1); err != nil {
		n.haltLocked(err)
		return
	}
	n.log = append(n.log, noop)
	n.heartbeatAt = n.ticks
	n.broadcastAppendEntriesLocked()
}

func (n *Node) appendEntryLocked(entry Entry) error {
	if err := n.storage.Append(entry, uint64(len(n.log))+1); err != nil {
		return err
	}
	n.log = append(n.log, entry)
	return nil
}

func (n *Node) truncateFromLocked(index uint64) error {
	if index == 0 || index-1 >= uint64(len(n.log)) {
		return nil
	}
	if err := n.storage.TruncateFrom(index - 1); err != nil {
		return err
	}
	n.log = n.log[:index-1]
	return nil
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, ok := n.entryAtLocked(next)
		if !ok {
			return
		}
		if err := n.fsm.Apply(entry.Command); err != nil {
			n.haltLocked(err)
			return
		}
		n.lastApplied = next
		n.maybeSnapshotLocked()
	}
}

func (n *Node) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	if n.fatalErr != nil {
		n.mu.Unlock()
		return 0, ErrFatal
	}
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	entry := Entry{Term: n.term, Command: append([]byte(nil), command...)}
	if err := n.storage.Append(entry, uint64(len(n.log))+1); err != nil {
		n.haltLocked(err)
		n.mu.Unlock()
		return 0, err
	}
	n.log = append(n.log, entry)
	target := uint64(len(n.log))
	n.broadcastAppendEntriesLocked()
	n.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n.mu.Lock()
		if n.fatalErr != nil {
			defer n.mu.Unlock()
			return 0, ErrFatal
		}
		if n.role != Leader {
			n.mu.Unlock()
			return 0, ErrLeaderLost
		}
		if n.lastApplied >= target {
			n.mu.Unlock()
			return target, nil
		}
		n.mu.Unlock()
		if time.Now().After(deadline) {
			return 0, ErrLeaderTimeout
		}
		time.Sleep(time.Millisecond)
	}
}

func (n *Node) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fatalErr != nil {
		return InstallSnapshotReply{}
	}
	if args.Term < n.term {
		return InstallSnapshotReply{Term: n.term}
	}
	n.role = Follower
	if args.Term > n.term {
		if err := n.storage.SaveMeta(args.Term, -1); err != nil {
			n.haltLocked(err)
			return InstallSnapshotReply{}
		}
		n.term = args.Term
	}
	n.resetElectionDeadlineLocked()
	if args.LastIncludedIndex <= n.snapshotIndex {
		return InstallSnapshotReply{Term: n.term}
	}
	rec := SnapshotRecord{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Data: append([]byte(nil), args.Data...)}
	if err := n.storage.WriteSnapshot(rec); err != nil {
		n.haltLocked(err)
		return InstallSnapshotReply{}
	}
	truncateCount := rec.Index - n.snapshotIndex
	if truncateCount > 0 {
		if err := n.storage.TruncatePrefix(truncateCount); err != nil {
			n.haltLocked(err)
			return InstallSnapshotReply{}
		}
		n.log = n.log[truncateCount:]
	}
	n.snapshotIndex = rec.Index
	n.snapshotTerm = rec.Term
	n.snapshotData = rec.Data
	n.commitIndex = rec.Index
	n.lastApplied = rec.Index
	for j := range n.peers {
		if j == n.cfg.Self {
			continue
		}
		n.matchIndex[j] = rec.Index
		n.nextIndex[j] = rec.Index + 1
	}
	if err := n.fsm.InstallSnapshot(rec.Data, rec.Index, rec.Term); err != nil {
		n.haltLocked(err)
		return InstallSnapshotReply{}
	}
	return InstallSnapshotReply{Term: n.term}
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
	if peers[n.cfg.Self] != nil {
		return errors.New("raft self slot must be nil")
	}
	for i, peer := range peers {
		if i == n.cfg.Self {
			continue
		}
		if peer == nil {
			return fmt.Errorf("raft peer %d is nil", i)
		}
	}
	n.peers = peers
	n.matchIndex = make([]uint64, len(peers))
	n.nextIndex = make([]uint64, len(peers))
	if n.role == Leader {
		n.role = Follower
		n.resetElectionDeadlineLocked()
	}
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

func (n *Node) maybeSnapshotLocked() {
	threshold := n.cfg.SnapshotThreshold
	if threshold == 0 || n.lastApplied <= n.snapshotIndex {
		return
	}
	if n.lastApplied-n.snapshotIndex < threshold {
		return
	}
	target := n.lastApplied
	term, ok := n.entryTermLocked(target)
	if !ok {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.haltLocked(err)
		return
	}
	rec := SnapshotRecord{Index: target, Term: term, Data: data}
	if err := n.storage.WriteSnapshot(rec); err != nil {
		n.haltLocked(err)
		return
	}
	m := target - n.snapshotIndex
	if m > 0 {
		if err := n.storage.TruncatePrefix(m); err != nil {
			n.haltLocked(err)
			return
		}
		n.log = n.log[m:]
	}
	n.snapshotIndex = target
	n.snapshotTerm = term
	n.snapshotData = data
}

func (n *Node) entryTermLocked(index uint64) (uint64, bool) {
	if index == 0 || index > n.lastIndexLocked() {
		return 0, false
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm, true
	}
	pos := index - n.snapshotIndex - 1
	return n.log[pos].Term, true
}

func (n *Node) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotIndex
}
