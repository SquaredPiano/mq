package broker

import (
	"encoding/json"

	"mq/internal/wal"
)

type RaftFSM struct {
	broker *Broker
}

func NewRaftFSM(b *Broker) *RaftFSM {
	return &RaftFSM{broker: b}
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
