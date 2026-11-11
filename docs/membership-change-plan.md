# Raft membership changes — design reference

## Current state

Cluster membership is set at process start (`-raft-peers` + `-raft-self`).
There is no automatic reconfiguration. The `cluster` role cannot add or
remove members without restarting all three processes.

## Why one-at-a-time

Single-server changes are safe without full joint consensus because every
committed config overlaps the next by construction (all changes committed
under the old voter set before taking effect; flip the voter at apply time).

## Proposal command envelope

Type prefix before the JSON body: `mq-raft-membership:`

```go
type MembershipCommand struct {
    Version uint16           `json:"version"`
    Kind    MemberChangeKind `json:"kind"`  // 1 = add, 2 = promote, 3 = remove
    Spec    PeerSpec         `json:"spec"`
}
type PeerSpec struct {
    Index int    `json:"index"`           // stable membership slot; never reused
    URL   string `json:"url,omitempty"`   // empty for in-process tests
}
```

Broker FSM (`internal/broker/raft_fsm.go`) unmarshals commands as plain
`[]wal.Entry`, so the prefix byte is
needed because JSON two-byte sniffing is brittle.

## New Node state

```go
type Config struct {
    // existing fields unchanged
    PeerFactory func(PeerSpec) Peer // required for membershipAdd
}
type Node struct {
    // peers, nextIndex, matchIndex, voter []bool added alongside
    voter    []bool
    selfSpec PeerSpec
}
```

## Apply-time activation

`applyCommittedLocked` decodes and validates the magic prefix before the
FSM. Apply steps:

- AddNode: grow `peers`, `nextIndex`, `matchIndex`, `voter` by one slot with
  `matchIndex=0`. Existing slots kept; `peers[self]=nil`. Use
  `PeerFactory(spec)` to turn slot into a live `Peer`.
  `voter=false` — learner until promoted.
- PromoteNode: `voter[slot]=true`. Require leader has observed
  `matchIndex[slot] >= lastIndexLocked()` at proposal time.
- RemoveNode: `voter[slot]=false`, `peers[slot]=nil` — tombstoned, never
  reused (indexes stable for log compatibility). Removing yourself exits
  the node (`haltLocked(ErrMemberRemoved)`).

## Quorum with voters only

`quorum()` becomes `count(voter)/2+1`.
`AppendEntries`/`RequestVote`/`broadcastAppendEntriesLocked`/`startElectionLocked`
count only voters (skip voter=false).

## Apply on restart

`New()` replays the full log; for each membership magic entry it rebuilds
`peers`/`voter` in order. Baseline = open-time peer list. The log is
authoritative; membership loss = log loss.

## Safety rules

- Single-server changes only (no joint consensus in this design; avoid it,
  stale configs are quieted as soon as the entry committed).
- No membership entries may duplicate in flight; `ProposeMembership` is
  rejected if an uncommitted membership entry exists (`ErrMembershipPending`).
- MemberAddNode requires `PeerFactory`; single-node mode passes nil and
  proposals fail fast.
- Membership entries are never truncated/snapshoted away.
- Learner nodes cannot be required for quorum until promoted.

## Order of work if picked up

1. Types + decoder in raft.go (the envelope + MembershipCommand kind).
2. Config/selfSpec + initializer path in Node.New.
3. applyCommittedLocked hook with prefix-check.
4. Broadcasting learner role (no voters, no votes).
5. Admin endpoint `/raft/add-node` and `/raft/remove-node` wired through
   mqcluster + httpapi with the replication token.
6. Tests: learner cannot block writes; promote-committed becomes voter;
   losing a voter still commits; restart replays membership; oversize
   proposal returns ErrMembershipPending.
