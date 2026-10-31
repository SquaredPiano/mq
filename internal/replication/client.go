package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mq/internal/broker"
)

const maxChunkBytes = 4 << 20
const maxChunksPerSync = 1

var ErrCatchupIncomplete = errors.New("replication catch-up remains")

type Status struct {
	Epoch                 uint64  `json:"epoch"`
	LocalWALBytes         int64   `json:"local_wal_bytes"`
	LastConfirmedWALBytes int64   `json:"last_confirmed_wal_bytes"`
	UnconfirmedWALBytes   int64   `json:"unconfirmed_wal_bytes"`
	FollowerKnown         bool    `json:"follower_known"`
	InSync                bool    `json:"in_sync"`
	Syncing               bool    `json:"syncing"`
	SnapshotGeneration    uint64  `json:"snapshot_generation"`
	SnapshotWALOffset     int64   `json:"snapshot_wal_offset"`
	PublishSyncGroups     uint64  `json:"publish_sync_groups"`
	PublishWALFrames      uint64  `json:"publish_wal_frames"`
	PublishFramesPerSync  float64 `json:"publish_frames_per_sync"`
	LastAttempt           string  `json:"last_attempt,omitempty"`
	LastSuccess           string  `json:"last_success,omitempty"`
	LastError             string  `json:"last_error,omitempty"`
}

type followerStatus struct {
	Offset int64  `json:"offset"`
	Hash   string `json:"hash"`
	Epoch  uint64 `json:"epoch"`
}

type Client struct {
	broker  *broker.Broker
	baseURL string
	token   string
	epoch   uint64
	http    *http.Client

	mu                  sync.Mutex
	statusMu            sync.RWMutex
	nextOffset          int64
	known               bool
	lastConfirmedOffset int64
	followerKnown       bool
	lastAttempt         time.Time
	lastSuccess         time.Time
	lastError           string
	syncing             bool

	snapshotInterval atomic.Int64
	sharedGeneration uint64
	sharedOffset     int64
}

func (c *Client) SetSnapshotInterval(size int64) {
	if size > 0 {
		c.snapshotInterval.Store(size)
	}
}

func New(source *broker.Broker, followerURL, token string, epoch uint64) (*Client, error) {
	parsed, err := url.Parse(followerURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("invalid follower URL %q", followerURL)
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return nil, errors.New("replication over HTTP is allowed only to a loopback follower; use HTTPS for remote followers")
	}
	if source == nil {
		return nil, fmt.Errorf("replication source broker is required")
	}
	if token == "" {
		return nil, fmt.Errorf("replication token must not be empty")
	}
	if epoch == 0 {
		return nil, fmt.Errorf("leader epoch must be positive")
	}
	return &Client{
		broker:  source,
		baseURL: strings.TrimRight(followerURL, "/"),
		token:   token,
		epoch:   epoch,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Client) Sync() error {
	return c.SyncContext(context.Background())
}

func (c *Client) SyncContext(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	started := time.Now().UTC()
	c.statusMu.Lock()
	c.lastAttempt = started
	c.syncing = true
	c.statusMu.Unlock()

	err := c.syncLocked(ctx)
	c.statusMu.Lock()
	c.syncing = false
	c.lastConfirmedOffset = c.nextOffset
	if errors.Is(err, ErrCatchupIncomplete) {
		c.lastError = ""
		c.lastSuccess = time.Now().UTC()
	} else if err != nil {
		c.lastError = err.Error()
		c.known = false
	} else {
		c.lastError = ""
		c.lastSuccess = time.Now().UTC()
	}
	c.followerKnown = c.known
	c.statusMu.Unlock()
	return err
}

func (c *Client) syncLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	localEnd, err := c.broker.WALSize()
	if err != nil {
		return err
	}
	if !c.known {
		if err := c.refresh(ctx, localEnd); err != nil {
			return err
		}
	}
	if c.nextOffset > localEnd {
		c.known = false
		return fmt.Errorf("follower WAL is ahead of primary: follower=%d primary=%d", c.nextOffset, localEnd)
	}

	chunksSent := 0
	for c.nextOffset < localEnd && chunksSent < maxChunksPerSync {
		frames, err := c.broker.ReadWALFramesUntil(c.nextOffset, localEnd, maxChunkBytes)
		if err != nil {
			c.known = false
			return err
		}
		if len(frames) == 0 {
			c.known = false
			return fmt.Errorf("primary WAL has no frame at byte %d", c.nextOffset)
		}
		if err := c.sendFrames(ctx, c.nextOffset, frames); err != nil {
			c.known = false
			return err
		}
		c.nextOffset += int64(len(frames))
		chunksSent++
	}
	if c.nextOffset < localEnd {
		return ErrCatchupIncomplete
	}
	return nil
}

func (c *Client) SyncAll(ctx context.Context) error {
	for {
		err := c.SyncContext(ctx)
		if !errors.Is(err, ErrCatchupIncomplete) {
			return err
		}
	}
}

func (c *Client) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	if ctx.Err() != nil {
		return
	}
	_ = c.SyncAll(ctx)
	_ = c.checkpointCycle(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.SyncAll(ctx)
			_ = c.checkpointCycle(ctx)
		}
	}
}

func (c *Client) checkpointCycle(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	localEnd, err := c.broker.WALSize()
	if err != nil {
		return err
	}
	if err := c.maybeCheckpoint(ctx, localEnd); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.statusMu.Lock()
	c.lastConfirmedOffset = c.nextOffset
	c.statusMu.Unlock()
	return nil
}

func (c *Client) Status() (Status, error) {
	localEnd, err := c.broker.WALSize()
	if err != nil {
		return Status{}, err
	}
	groupStats := c.broker.PublishGroupCommitStats()
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	status := Status{
		Epoch:                 c.epoch,
		LocalWALBytes:         localEnd,
		LastConfirmedWALBytes: c.lastConfirmedOffset,
		FollowerKnown:         c.followerKnown,
		Syncing:               c.syncing,
		PublishSyncGroups:     groupStats.Groups,
		PublishWALFrames:      groupStats.Frames,
		PublishFramesPerSync:  groupStats.FramesPerGroup(),
		LastError:             c.lastError,
	}
	status.UnconfirmedWALBytes = localEnd - c.lastConfirmedOffset
	status.SnapshotGeneration = c.sharedGeneration
	status.SnapshotWALOffset = c.sharedOffset
	status.InSync = status.FollowerKnown && !c.syncing && status.UnconfirmedWALBytes == 0 && status.LastError == ""
	if !c.lastAttempt.IsZero() {
		status.LastAttempt = c.lastAttempt.Format(time.RFC3339Nano)
	}
	if !c.lastSuccess.IsZero() {
		status.LastSuccess = c.lastSuccess.Format(time.RFC3339Nano)
	}
	return status, nil
}

func (c *Client) refresh(ctx context.Context, localEnd int64) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/replication/status", nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Replication-Token", c.token)
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("read follower WAL status: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("follower WAL status returned HTTP %d: %s", response.StatusCode, readErrorBody(response.Body))
	}
	var status followerStatus
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&status); err != nil {
		return fmt.Errorf("decode follower WAL status: %w", err)
	}
	if status.Offset < 0 || status.Offset > localEnd {
		return fmt.Errorf("follower WAL offset %d is outside primary range 0..%d", status.Offset, localEnd)
	}
	if status.Epoch > c.epoch {
		return fmt.Errorf("follower has accepted leader epoch %d; this leader is stale at epoch %d", status.Epoch, c.epoch)
	}
	remoteHash, err := hex.DecodeString(status.Hash)
	if err != nil || len(remoteHash) != 32 {
		return fmt.Errorf("follower returned an invalid WAL prefix hash")
	}
	base := c.broker.WALBase()
	if status.Offset < base {
		return c.installSnapshot(ctx)
	}
	localHash, err := c.broker.WALHashPrefix(status.Offset)
	if err != nil {
		return err
	}
	if !bytes.Equal(localHash[:], remoteHash) {
		return c.installSnapshot(ctx)
	}
	if status.Epoch < c.epoch {
		if err := c.advanceEpoch(ctx, c.epoch, status.Offset, localHash); err != nil {
			return err
		}
	}
	c.nextOffset = status.Offset
	c.known = true
	return nil
}

func (c *Client) installSnapshot(ctx context.Context) error {
	meta, ok := c.broker.SnapshotIdentity()
	if !ok {
		if err := c.createLocalSnapshot(); err != nil {
			return err
		}
	} else {
		if err := uploadSnapshot(ctx, c, meta); err != nil {
			return err
		}
	}
	return c.commitLocalSnapshot(ctx)
}

func (c *Client) advanceEpoch(ctx context.Context, epoch uint64, offset int64, hash [32]byte) error {
	query := url.Values{}
	query.Set("epoch", strconv.FormatUint(epoch, 10))
	query.Set("offset", strconv.FormatInt(offset, 10))
	query.Set("hash", hex.EncodeToString(hash[:]))
	target := c.baseURL + "/internal/replication/epoch?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Replication-Token", c.token)
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("advance follower leader epoch: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("follower rejected leader epoch %d with HTTP %d: %s", epoch, response.StatusCode, readErrorBody(response.Body))
	}
	return nil
}

func (c *Client) sendFrames(ctx context.Context, offset int64, frames []byte) error {
	target := c.baseURL + "/internal/replication/frames?offset=" + strconv.FormatInt(offset, 10)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(frames))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Replication-Token", c.token)
	request.Header.Set("X-Leader-Epoch", strconv.FormatUint(c.epoch, 10))
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("send WAL frames at byte %d: %w", offset, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("follower rejected WAL frames at byte %d with HTTP %d: %s", offset, response.StatusCode, readErrorBody(response.Body))
	}
	return nil
}

func readErrorBody(reader io.Reader) string {
	data, _ := io.ReadAll(io.LimitReader(reader, 4096))
	return strings.TrimSpace(string(data))
}

func (c *Client) createLocalSnapshot() error {
	if _, err := c.broker.CreateSnapshot(); err != nil {
		return fmt.Errorf("create leader snapshot: %w", err)
	}
	return nil
}

func uploadSnapshot(ctx context.Context, client *Client, meta broker.SnapshotFileMeta) error {
	file, _, err := client.broker.ExportSnapshot()
	if err != nil {
		return fmt.Errorf("export leader snapshot: %w", err)
	}
	defer file.Close()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/internal/replication/snapshot", file)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Replication-Token", client.token)
	request.Header.Set("X-Leader-Epoch", strconv.FormatUint(client.epoch, 10))
	request.Header.Set("X-Snapshot-Generation", strconv.FormatUint(meta.Generation, 10))
	request.Header.Set("X-Snapshot-Checksum", hex.EncodeToString(meta.Digest[:]))
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("send leader snapshot for generation %d: %w", meta.Generation, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("follower rejected snapshot generation %d with HTTP %d: %s", meta.Generation, response.StatusCode, readErrorBody(response.Body))
	}
	return nil
}

func (c *Client) commitLocalSnapshot(ctx context.Context) error {
	meta, ok := c.broker.SnapshotIdentity()
	if !ok {
		return errors.New("no reusable snapshot found after checkpoint")
	}
	if err := uploadSnapshot(ctx, c, meta); err != nil {
		return err
	}
	c.statusMu.Lock()
	c.sharedGeneration = meta.Generation
	c.sharedOffset = meta.WALOffset
	c.statusMu.Unlock()
	c.nextOffset = meta.WALOffset
	c.known = true
	if err := c.broker.TrimWALPrefix(meta.WALOffset); err != nil {
		return fmt.Errorf("trim leader WAL after checkpoint: %w", err)
	}
	return nil
}

func (c *Client) maybeCheckpoint(ctx context.Context, localEnd int64) error {
	interval := c.snapshotInterval.Load()
	c.statusMu.RLock()
	sharedOffset := c.sharedOffset
	c.statusMu.RUnlock()
	latest, hasSnapshot := c.broker.SnapshotIdentity()
	if interval > 0 && localEnd-sharedOffset < interval && (!hasSnapshot || latest.WALOffset <= sharedOffset) {
		return nil
	}
	if !hasSnapshot {
		if err := c.createLocalSnapshot(); err != nil {
			return err
		}
	}
	return c.commitLocalSnapshot(ctx)
}
