package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	pprof "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mq/internal/broker"
	"mq/internal/httpapi"
	"mq/internal/mqcluster"
	"mq/internal/raft"
	"mq/internal/replication"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP(S) listen address")
	dataDir := flag.String("data", "./mq-data", "directory for broker data")
	groupCommitWindow := flag.Duration(
		"group-commit-window", broker.DefaultGroupCommitWindow,
		"maximum wait to batch concurrent publishes before syncing; zero disables the wait",
	)
	walSegmentBytes := flag.Int64(
		"wal-segment-bytes", broker.DefaultWALSegmentBytes,
		"maximum size of a WAL segment (minimum 4 MiB)",
	)
	partitionCount := flag.Int(
		"partitions", 1,
		"number of partitions each topic is sharded into at start",
	)
	retentionMaxAge := flag.Duration(
		"retention-max-age", 0,
		"delete records older than this age (e.g. 24h); zero disables time-based retention",
	)
	snapshotInterval := flag.Int64(
		"snapshot-interval-bytes", 64<<20,
		"bytes of undeliverable WAL growth after the shared snapshot before the leader takes another checkpoint; zero stops automatic checkpointing",
	)
	role := flag.String("role", "standalone", "node role: standalone, leader, or follower")
	replicaURL := flag.String("replica", "", "follower base URL for a leader")
	replicationToken := flag.String("replication-token", os.Getenv("MQ_REPLICATION_TOKEN"), "shared leader/follower token; may also be set with MQ_REPLICATION_TOKEN")
	clientToken := flag.String("client-token", os.Getenv("MQ_CLIENT_TOKEN"), "Bearer token for public API routes; may also be set with MQ_CLIENT_TOKEN")
	adminToken := flag.String("admin-token", os.Getenv("MQ_ADMIN_TOKEN"), "Bearer token for topic retention changes; may also be set with MQ_ADMIN_TOKEN")
	leaderEpoch := flag.Uint64("epoch", 0, "positive, monotonically increasing leader epoch")
	raftPeers := flag.String("raft-peers", "", "comma-separated HTTP base URLs of every raft node in cluster mode (self included, in order)")
	raftSelf := flag.Int("raft-self", -1, "index into -raft-peers for this node in cluster mode")
	raftLearner := flag.Bool("raft-learner", false, "start this cluster node as a non-voting learner (for add-node); promote after joining")
	raftToken := flag.String("raft-token", os.Getenv("MQ_RAFT_TOKEN"), "token for raft RPCs; may also be set with MQ_RAFT_TOKEN")
	tlsCert := flag.String("tls-cert", "", "TLS certificate PEM file; enables HTTPS")
	tlsKey := flag.String("tls-key", "", "TLS private key PEM file; enables HTTPS")
	flag.Parse()
	if (*tlsCert == "") != (*tlsKey == "") {
		return errors.New("-tls-cert and -tls-key must be supplied together")
	}
	if *tlsCert == "" && !isLoopbackAddress(*address) {
		return errors.New("non-loopback HTTP listeners require -tls-cert and -tls-key")
	}
	if *clientToken == "" && !isLoopbackAddress(*address) {
		return errors.New("non-loopback listeners require -client-token or MQ_CLIENT_TOKEN")
	}

	store, err := broker.OpenWithOptions(*dataDir, broker.OpenOptions{
		GroupCommitWindow: *groupCommitWindow,
		WALSegmentBytes:   *walSegmentBytes,
		PartitionCount:    *partitionCount,
		MaxAge:            *retentionMaxAge,
	})
	if err != nil {
		return fmt.Errorf("open broker: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close broker: %w", err))
		}
	}()

	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var replicationWorkerDone chan struct{}
	defer func() {
		stop()
		if replicationWorkerDone != nil {
			<-replicationWorkerDone
		}
	}()

	var handler http.Handler
	var replicator *replication.Client
	var cluster *mqcluster.Node
	switch *role {
	case "standalone":
		if *replicaURL != "" || *replicationToken != "" || *leaderEpoch != 0 {
			return errors.New("standalone role cannot use replica settings")
		}
		handler, err = httpapi.NewAuthenticatedHandlerWithAdmin(store, *clientToken, *adminToken)
		if err != nil {
			return err
		}
	case "leader":
		if *replicaURL == "" {
			return errors.New("leader role requires -replica")
		}
		if *leaderEpoch == 0 {
			return errors.New("leader role requires a positive -epoch")
		}
		replicator, err = replication.New(store, *replicaURL, *replicationToken, *leaderEpoch)
		if err != nil {
			return err
		}
		replicator.SetSnapshotInterval(*snapshotInterval)

		store.SetReplicator(func() error {
			return replicator.SyncContext(shutdown)
		})
		handler, err = httpapi.NewAuthenticatedLeaderHandlerWithAdmin(store, *replicationToken, *clientToken, *adminToken, func() (any, error) {
			return replicator.Status()
		})
		if err != nil {
			return err
		}
	case "follower":
		if *replicaURL != "" || *leaderEpoch != 0 {
			return errors.New("follower role cannot use -replica or -epoch")
		}
		var err error
		handler, err = httpapi.NewAuthenticatedFollowerHandlerWithAdmin(store, *replicationToken, *clientToken, *adminToken)
		if err != nil {
			return err
		}
	case "cluster":
		if *raftPeers == "" || *raftSelf < 0 || *raftToken == "" {
			return errors.New("cluster role requires -raft-peers, -raft-self, and -raft-token")
		}
		urls := strings.Split(*raftPeers, ",")
		if *raftSelf >= len(urls) {
			return errors.New("-raft-self is outside -raft-peers")
		}
		var err error
		cluster, err = mqcluster.OpenWithPartitions(*dataDir, urls, *raftSelf, *raftToken, 50*time.Millisecond, *partitionCount, *raftLearner, *retentionMaxAge)
		if err != nil {
			return fmt.Errorf("open cluster node: %w", err)
		}
		defer func() {
			if err := cluster.Close(); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("close cluster: %w", err))
			}
			cluster = nil
		}()
		localIndex := *raftSelf
		localPeers := strings.Split(*raftPeers, ",")
		resolver := func(scope string) (string, bool) {
			_ = scope
			if cluster.Raft.StateRole() == raft.Leader {
				return localPeers[localIndex], true
			}
			id := cluster.Raft.LeaderID()
			if id >= 0 && id < len(localPeers) {
				return localPeers[id], true
			}
			return "", false
		}
		selfURL := localPeers[localIndex]
		base, err := httpapi.NewAuthenticatedHandlerWithLeader(cluster.Broker, *clientToken, *adminToken, selfURL, resolver)
		if err != nil {
			return err
		}
		mux := http.NewServeMux()
		mux.Handle("/", base)
		mux.Handle("/raft/", raft.HTTPHandler(cluster.Raft, *raftToken))
		mux.Handle("/v1/admin/cluster/members", clusterAdminHandler(cluster.Raft, *adminToken))
		mux.Handle("/v1/admin/cluster/add-node", clusterAdminHandler(cluster.Raft, *adminToken))
		mux.Handle("/v1/admin/cluster/promote-node", clusterAdminHandler(cluster.Raft, *adminToken))
		mux.Handle("/v1/admin/cluster/remove-node", clusterAdminHandler(cluster.Raft, *adminToken))
		handler = mux
	default:
		return fmt.Errorf("unknown role %q (use standalone, leader, follower, or cluster)", *role)
	}
	if replicator != nil {
		replicationWorkerDone = make(chan struct{})
		go func() {
			defer close(replicationWorkerDone)
			replicator.Run(shutdown, time.Second)
		}()
	}

	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	handler = mux

	server := &http.Server{
		Addr:              *address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	serverErrors := make(chan error, 1)
	go func() {
		if *tlsCert != "" {
			log.Printf("listening with HTTPS on %s", server.Addr)
			serverErrors <- server.ListenAndServeTLS(*tlsCert, *tlsKey)
			return
		}
		log.Printf("listening with HTTP on %s", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
	case <-shutdown.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr := server.Shutdown(ctx)
		cancel()
		if shutdownErr != nil {
			log.Printf("graceful shutdown failed: %v", shutdownErr)
			if closeErr := server.Close(); closeErr != nil {
				log.Printf("force close failed: %v", closeErr)
			}
		}
		if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		if shutdownErr != nil {
			return fmt.Errorf("shut down HTTP server: %w", shutdownErr)
		}
	}
	return nil
}

func clusterAdminHandler(node *raft.Node, adminToken string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if adminToken == "" {
			http.Error(w, "admin token must be set for cluster members", http.StatusForbidden)
			return
		}
		provided := r.Header.Get("Authorization")
		credential, found := strings.CutPrefix(provided, "Bearer ")
		if !found || subtle.ConstantTimeCompare([]byte(credential), []byte(adminToken)) != 1 {
			http.Error(w, "invalid admin token", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/admin/cluster/members":
			specs, voter := node.MembershipMembers()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"members": specs,
				"voter":   voter,
			})
			return
		case "/v1/admin/cluster/add-node":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var body struct {
				URL string `json:"url"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil || body.URL == "" {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			spec := raft.PeerSpec{URL: body.URL}
			idx, err := node.AddNode(spec)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "index": idx, "members": nil})
			return
		case "/v1/admin/cluster/remove-node":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var body struct {
				Index int `json:"index"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil || body.Index < 0 {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			spec := raft.PeerSpec{Index: body.Index}
			idx, err := node.RemoveNode(spec)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "index": idx})
			return
		case "/v1/admin/cluster/promote-node":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var body struct {
				Index int `json:"index"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil || body.Index < 0 {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			spec := raft.PeerSpec{Index: body.Index}
			idx, err := node.PromoteNode(spec)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "index": idx})
			return
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
