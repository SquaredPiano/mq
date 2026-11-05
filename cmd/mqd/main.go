package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mq/internal/broker"
	"mq/internal/httpapi"
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
	default:
		return fmt.Errorf("unknown role %q (use standalone, leader, or follower)", *role)
	}
	if replicator != nil {
		replicationWorkerDone = make(chan struct{})
		go func() {
			defer close(replicationWorkerDone)
			replicator.Run(shutdown, time.Second)
		}()
	}

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
