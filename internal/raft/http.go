package raft

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPPeer struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

func (p HTTPPeer) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	body, err := json.Marshal(args)
	if err != nil {
		return InstallSnapshotReply{}
	}
	reply, err := p.post("/raft/install-snapshot", body)
	if err != nil {
		return InstallSnapshotReply{}
	}
	var out InstallSnapshotReply
	if err := json.Unmarshal(reply, &out); err != nil {
		return InstallSnapshotReply{}
	}
	return out
}

func (p HTTPPeer) RequestVote(args RequestVoteArgs) RequestVoteReply {
	body, err := json.Marshal(args)
	if err != nil {
		return RequestVoteReply{Term: args.Term, VoteGranted: false}
	}
	reply, err := p.post("/raft/request-vote", body)
	if err != nil {
		return RequestVoteReply{Term: args.Term, VoteGranted: false}
	}
	var out RequestVoteReply
	if err := json.Unmarshal(reply, &out); err != nil {
		return RequestVoteReply{Term: args.Term, VoteGranted: false}
	}
	return out
}

func (p HTTPPeer) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	body, err := json.Marshal(args)
	if err != nil {
		return AppendEntriesReply{Term: args.Term, Success: false}
	}
	reply, err := p.post("/raft/append-entries", body)
	if err != nil {
		return AppendEntriesReply{Term: args.Term, Success: false}
	}
	var out AppendEntriesReply
	if err := json.Unmarshal(reply, &out); err != nil {
		return AppendEntriesReply{Term: args.Term, Success: false}
	}
	return out
}

func (p HTTPPeer) post(path string, body []byte) ([]byte, error) {
	if p.Client == nil {
		p.Client = &http.Client{Timeout: 5 * time.Second}
	}
	request, err := http.NewRequest(http.MethodPost, p.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Raft-Token", p.Token)
	response, err := p.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("raft peer returned status %d: %s", response.StatusCode, string(data))
	}
	return data, nil
}

func HTTPHandler(node *Node, token string) http.Handler {
	if node == nil {
		panic("raft node is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(token)) != 1 {
			http.Error(w, "invalid raft token", http.StatusUnauthorized)
			return
		}
		var args RequestVoteArgs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&args); err != nil {
			http.Error(w, "could not decode request vote args", http.StatusBadRequest)
			return
		}
		reply := node.RequestVote(args)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/install-snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(token)) != 1 {
			http.Error(w, "invalid raft token", http.StatusUnauthorized)
			return
		}
		var args InstallSnapshotArgs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&args); err != nil {
			http.Error(w, "could not decode install snapshot args", http.StatusBadRequest)
			return
		}
		reply := node.InstallSnapshot(args)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(token)) != 1 {
			http.Error(w, "invalid raft token", http.StatusUnauthorized)
			return
		}
		var args AppendEntriesArgs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&args); err != nil {
			http.Error(w, "could not decode append entries args", http.StatusBadRequest)
			return
		}
		reply := node.AppendEntries(args)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/role", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(token)) != 1 {
			http.Error(w, "invalid raft token", http.StatusUnauthorized)
			return
		}
		role, term, commitIndex, lastIndex := node.Status()
		writeJSON(w, map[string]any{
			"role":         role,
			"term":         term,
			"commit_index": commitIndex,
			"last_index":   lastIndex,
		})
	})
	mux.HandleFunc("/raft/propose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(token)) != 1 {
			http.Error(w, "invalid raft token", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
		if err != nil {
			http.Error(w, "could not read command", http.StatusBadRequest)
			return
		}
		index, err := node.Propose(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]any{"index": index})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
