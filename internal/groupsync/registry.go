package groupsync

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	MemberIDLength = 16
	MemberTimeout  = 20 * time.Second
)

type Member struct {
	ID       string    `json:"member_id"`
	Group    string    `json:"group"`
	Topic    string    `json:"topic"`
	Assigned []int     `json:"partitions"`
	Expires  time.Time `json:"expires"`
}

type Registry struct {
	mu      sync.Mutex
	members map[string]*Member
}

var ErrUnknownMember = errors.New("unknown member")

func New() *Registry {
	return &Registry{members: make(map[string]*Member)}
}

func key(group, topic, id string) string {
	return group + "/" + topic + "/" + id
}

func roundRobinAssign(members []*Member, partitions uint64) {
	n := len(members)
	if n == 0 {
		return
	}
	for i, m := range members {
		m.Assigned = nil
		for p := uint64(i); p < partitions; p += uint64(n) {
			m.Assigned = append(m.Assigned, int(p))
		}
	}
}

func (r *Registry) Join(group, topic string, partitions uint8) (Member, error) {
	r.checkTopic(topic, partitions)
	if err := r.checkGroup(group); err != nil {
		return Member{}, err
	}
	id, err := freshID()
	if err != nil {
		return Member{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapLocked()
	current := r.membersLocked(group, topic)
	m := &Member{
		ID:       id,
		Group:    group,
		Topic:    topic,
		Assigned: nil,
		Expires:  time.Now().Add(MemberTimeout),
	}
	r.members[key(group, topic, id)] = m
	current = append(current, m)
	roundRobinAssign(current, uint64(partitions))
	for _, m := range current {
		if m.ID == id {
			return Member{ID: m.ID, Group: m.Group, Topic: m.Topic, Assigned: append([]int(nil), m.Assigned...), Expires: m.Expires}, nil
		}
	}
	return Member{}, errors.New("assigned member not in snapshot")
}

func (r *Registry) Heartbeat(group, topic, memberID string, partitions uint8) (Member, error) {
	r.checkTopic(topic, partitions)
	if err := r.checkGroup(group); err != nil {
		return Member{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapLocked()
	m, ok := r.members[key(group, topic, memberID)]
	if !ok {
		return Member{}, ErrUnknownMember
	}
	m.Expires = time.Now().Add(MemberTimeout)
	current := r.membersLocked(group, topic)
	roundRobinAssign(current, uint64(partitions))
	return Member{ID: m.ID, Group: m.Group, Topic: m.Topic, Assigned: append([]int(nil), m.Assigned...), Expires: m.Expires}, nil
}

func (r *Registry) Leave(group, topic, memberID string, partitions uint8) error {
	r.checkTopic(topic, partitions)
	if err := r.checkGroup(group); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapLocked()
	k := key(group, topic, memberID)
	m, ok := r.members[k]
	if !ok {
		return ErrUnknownMember
	}
	delete(r.members, k)
	roundRobinAssign(r.membersLocked(group, topic), uint64(partitions))
	_ = m
	return nil
}

func (r *Registry) List(group, topic string) []Member {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapLocked()
	current := r.membersLocked(group, topic)
	out := make([]Member, 0, len(current))
	for _, m := range current {
		out = append(out, Member{ID: m.ID, Group: m.Group, Topic: m.Topic, Assigned: append([]int(nil), m.Assigned...), Expires: m.Expires})
	}
	return out
}

func (r *Registry) checkTopic(string, uint8) {}
func (r *Registry) checkGroup(string) error  { return nil }

func (r *Registry) reapLocked() {
	now := time.Now()
	for k, m := range r.members {
		if !m.Expires.After(now) {
			delete(r.members, k)
		}
	}
}

func (r *Registry) membersLocked(group, topic string) []*Member {
	members := make([]*Member, 0, 4)
	for _, m := range r.members {
		if m.Group == group && m.Topic == topic {
			members = append(members, m)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return members
}

func freshID() (string, error) {
	raw := make([]byte, MemberIDLength/2)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate member id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
