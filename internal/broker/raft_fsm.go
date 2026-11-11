package broker
import (
	"encoding/json"
	"fmt"

	"mq/internal/wal"
)

type RaftFSM struct {
	broker *Broker
}

func NewRaftFSM(b *Broker) *RaftFSM {
	return &RaftFSM{broker: b}
}

type raftSnapshotEnvelope struct {
	Version1            uint16 `json:"version"`
	LastIncludedIndex   uint64 `json:"last_included_index"`
	LastIncludedTerm    uint64 `json:"last_included_term"`
	Topics              []topicSnapshot   `json:"topics,omitempty"`
	Groups              []groupSnapshot   `json:"groups,omitempty"`
	Requests            []requestSnapshot `json:"requests,omitempty"`
}

const raftSnapshotVersion = uint16(1)

func (r *RaftFSM) Snapshot() ([]byte, error) {
	snapshot := r.broker.captureSnapshot(0, 0)
	envelope := raftSnapshotEnvelope{
		Version1:          raftSnapshotVersion,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		Topics:            snapshot.Topics,
		Groups:            snapshot.Groups,
		Requests:          snapshot.Requests,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode raft snapshot: %w", err)
	}
	return data, nil
}

func (r *RaftFSM) InstallSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	var envelope raftSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode raft snapshot: %w", err)
	}
	if envelope.Version1 != raftSnapshotVersion {
		return fmt.Errorf("unsupported raft snapshot version %d", envelope.Version1)
	}
	snapshot := brokerSnapshot{Topics: envelope.Topics, Groups: envelope.Groups, Requests: envelope.Requests}
	state, err := buildSnapshotState(snapshot)
	if err != nil {
		return fmt.Errorf("raft snapshot does not decode to broker state: %w", err)
	}
	b := r.broker
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	b.mu.Lock()
	b.topics = state.topics
	b.committed = state.committed
	b.requests = state.requests
	b.unavailable = nil
	b.mu.Unlock()
	return nil
}

func (r *RaftFSM) Apply(command []byte) error {
	if len(command) == 0 {
		return nil
	}
	var entries []wal.Entry
	if err := json.Unmarshal(command, &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := r.broker.applyEntry(entry); err != nil {
			return err
		}
	}
	return nil
}
