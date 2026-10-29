package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"mq/internal/broker"
	messageLog "mq/internal/log"
)

const (
	maxRequestBytes = 1 << 20
	maxMessageBytes = 256 << 10
	maxBatchRecords = 32
	maxReplicaBytes = 4 << 20
)

type API struct {
	broker                     *broker.Broker
	readOnly                   bool
	requireRequestID           bool
	clientToken                string
	adminToken                 string
	replicationToken           string
	replicationMetricsProvider func() (any, error)
}

type publishRequest struct {
	Topic     string  `json:"topic"`
	RequestID *string `json:"request_id,omitempty"`
	Key       []byte  `json:"key"`
	Value     []byte  `json:"value"`
}

type publishMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type publishBatchRequest struct {
	Topic     string           `json:"topic"`
	RequestID *string          `json:"request_id,omitempty"`
	Messages  []publishMessage `json:"messages"`
}

type fetchRequest struct {
	Topic  string `json:"topic"`
	Offset uint64 `json:"offset"`
	Limit  int    `json:"limit"`
}

type groupFetchRequest struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
	Limit int    `json:"limit"`
}

type commitRequest struct {
	Group      string  `json:"group"`
	Topic      string  `json:"topic"`
	NextOffset *uint64 `json:"next_offset"`
}

type retentionRequest struct {
	Topic       string  `json:"topic"`
	StartOffset *uint64 `json:"start_offset"`
}

type response struct {
	OK      bool                 `json:"ok"`
	Error   string               `json:"error,omitempty"`
	Offset  *uint64              `json:"offset,omitempty"`
	Offsets []uint64             `json:"offsets,omitempty"`
	Records *[]messageLog.Record `json:"records,omitempty"`
}

type snapshotResponse struct {
	OK         bool   `json:"ok"`
	Generation uint64 `json:"generation"`
	WALOffset  int64  `json:"wal_offset"`
}

func NewHandler(b *broker.Broker) http.Handler {
	return NewAuthenticatedHandler(b, "")
}

func NewAuthenticatedHandler(b *broker.Broker, clientToken string) http.Handler {
	a := &API{broker: b, clientToken: clientToken}
	return a.handler()
}

func NewAuthenticatedHandlerWithAdmin(b *broker.Broker, clientToken, adminToken string) (http.Handler, error) {
	if adminToken != "" && adminToken == clientToken {
		return nil, errors.New("client and admin tokens must be different")
	}
	a := &API{broker: b, clientToken: clientToken, adminToken: adminToken}
	return a.handler(), nil
}

func NewLeaderHandler(b *broker.Broker, replicationToken string, metrics func() (any, error)) (http.Handler, error) {
	return NewAuthenticatedLeaderHandler(b, replicationToken, "", metrics)
}

func NewAuthenticatedLeaderHandler(b *broker.Broker, replicationToken, clientToken string, metrics func() (any, error)) (http.Handler, error) {
	return NewAuthenticatedLeaderHandlerWithAdmin(b, replicationToken, clientToken, "", metrics)
}

func NewAuthenticatedLeaderHandlerWithAdmin(b *broker.Broker, replicationToken, clientToken, adminToken string, metrics func() (any, error)) (http.Handler, error) {
	if replicationToken == "" {
		return nil, errors.New("replication token must not be empty")
	}
	if clientToken != "" && clientToken == replicationToken {
		return nil, errors.New("client and replication tokens must be different")
	}
	if adminToken != "" && (adminToken == clientToken || adminToken == replicationToken) {
		return nil, errors.New("admin token must differ from client and replication tokens")
	}
	if metrics == nil {
		return nil, errors.New("replication metrics provider is required")
	}
	a := &API{
		broker:                     b,
		requireRequestID:           true,
		clientToken:                clientToken,
		adminToken:                 adminToken,
		replicationToken:           replicationToken,
		replicationMetricsProvider: metrics,
	}
	return a.handler(), nil
}

func NewFollowerHandler(b *broker.Broker, replicationToken string) (http.Handler, error) {
	return NewAuthenticatedFollowerHandler(b, replicationToken, "")
}

func NewAuthenticatedFollowerHandler(b *broker.Broker, replicationToken, clientToken string) (http.Handler, error) {
	return NewAuthenticatedFollowerHandlerWithAdmin(b, replicationToken, clientToken, "")
}

func NewAuthenticatedFollowerHandlerWithAdmin(b *broker.Broker, replicationToken, clientToken, adminToken string) (http.Handler, error) {
	if replicationToken == "" {
		return nil, errors.New("replication token must not be empty")
	}
	if clientToken != "" && clientToken == replicationToken {
		return nil, errors.New("client and replication tokens must be different")
	}
	if adminToken != "" && (adminToken == clientToken || adminToken == replicationToken) {
		return nil, errors.New("admin token must differ from client and replication tokens")
	}
	if err := b.SetReadOnly(); err != nil {
		return nil, err
	}
	a := &API{broker: b, readOnly: true, clientToken: clientToken, adminToken: adminToken, replicationToken: replicationToken}
	return a.handler(), nil
}

func (a *API) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/publish", a.authenticateClient(http.HandlerFunc(a.publish)))
	mux.Handle("/v1/publish-batch", a.authenticateClient(http.HandlerFunc(a.publishBatch)))
	mux.Handle("/v1/fetch", a.authenticateClient(http.HandlerFunc(a.fetch)))
	mux.Handle("/v1/groups/fetch", a.authenticateClient(http.HandlerFunc(a.fetchGroup)))
	mux.Handle("/v1/groups/commit", a.authenticateClient(http.HandlerFunc(a.commit)))
	if a.adminToken != "" {
		mux.Handle("/v1/topics/retain", a.authenticateAdmin(http.HandlerFunc(a.retain)))
		mux.Handle("/v1/snapshot", a.authenticateAdmin(http.HandlerFunc(a.snapshot)))
	}
	mux.HandleFunc("/", a.notFound)
	if a.readOnly {
		mux.HandleFunc("/internal/replication/status", a.replicationStatus)
		mux.HandleFunc("/internal/replication/frames", a.replicationFrames)
		mux.HandleFunc("/internal/replication/epoch", a.replicationEpoch)
		mux.HandleFunc("/internal/replication/snapshot", a.replicationSnapshot)
	}
	if a.replicationMetricsProvider != nil {
		mux.HandleFunc("/internal/replication/metrics", a.replicationMetrics)
	}
	return mux
}

func (a *API) authenticateClient(next http.Handler) http.Handler {
	if a.clientToken == "" {
		return next
	}
	want := []byte(a.clientToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Values("Authorization")
		valid := false
		if len(authorization) == 1 {
			scheme, credential, found := strings.Cut(authorization[0], " ")
			valid = found && strings.EqualFold(scheme, "Bearer") && subtle.ConstantTimeCompare([]byte(credential), want) == 1
		}
		if !valid {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "invalid client token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *API) authenticateAdmin(next http.Handler) http.Handler {
	want := []byte(a.adminToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Values("Authorization")
		valid := false
		if len(authorization) == 1 {
			scheme, credential, found := strings.Cut(authorization[0], " ")
			valid = found && strings.EqualFold(scheme, "Bearer") && subtle.ConstantTimeCompare([]byte(credential), want) == 1
		}
		if !valid {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *API) publish(w http.ResponseWriter, r *http.Request) {
	if a.readOnly {
		writeError(w, http.StatusMethodNotAllowed, "follower nodes do not accept client publishes")
		return
	}
	if !requirePost(w, r) {
		return
	}

	var request publishRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if a.requireRequestID && request.RequestID == nil {
		writeError(w, http.StatusBadRequest, "request_id is required for replicated publishes")
		return
	}
	if len(request.Key)+len(request.Value) > maxMessageBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "key and value exceed the 256 KiB message limit")
		return
	}

	var offset uint64
	var err error
	if request.RequestID == nil {
		offset, err = a.broker.Publish(request.Topic, request.Key, request.Value)
	} else {
		offset, err = a.broker.PublishWithID(request.Topic, *request.RequestID, request.Key, request.Value)
	}
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response{OK: true, Offset: &offset})
}

func (a *API) publishBatch(w http.ResponseWriter, r *http.Request) {
	if a.readOnly {
		writeError(w, http.StatusMethodNotAllowed, "follower nodes do not accept client publishes")
		return
	}
	if !requirePost(w, r) {
		return
	}

	var request publishBatchRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if a.requireRequestID && request.RequestID == nil {
		writeError(w, http.StatusBadRequest, "request_id is required for replicated publishes")
		return
	}
	if len(request.Messages) == 0 || len(request.Messages) > maxBatchRecords {
		writeError(w, http.StatusBadRequest, "messages must contain between 1 and 32 records")
		return
	}

	messages := make([]broker.Message, len(request.Messages))
	for i, message := range request.Messages {
		if len(message.Key)+len(message.Value) > maxMessageBytes {
			writeError(w, http.StatusRequestEntityTooLarge, "key and value exceed the 256 KiB message limit")
			return
		}
		messages[i] = broker.Message{Key: message.Key, Value: message.Value}
	}

	var offsets []uint64
	var err error
	if request.RequestID == nil {
		offsets, err = a.broker.PublishBatch(request.Topic, messages)
	} else {
		offsets, err = a.broker.PublishBatchWithID(request.Topic, *request.RequestID, messages)
	}
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response{OK: true, Offsets: offsets})
}

func (a *API) fetch(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}

	var request fetchRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if !validLimit(w, request.Limit) {
		return
	}

	records, err := a.broker.Fetch(request.Topic, request.Offset, request.Limit)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	if records == nil {
		records = []messageLog.Record{}
	}
	writeJSON(w, http.StatusOK, response{OK: true, Records: &records})
}

func (a *API) fetchGroup(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}

	var request groupFetchRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if !validLimit(w, request.Limit) {
		return
	}

	records, err := a.broker.FetchGroup(request.Group, request.Topic, request.Limit)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	if records == nil {
		records = []messageLog.Record{}
	}
	writeJSON(w, http.StatusOK, response{OK: true, Records: &records})
}

func (a *API) commit(w http.ResponseWriter, r *http.Request) {
	if a.readOnly {
		writeError(w, http.StatusMethodNotAllowed, "follower nodes do not accept consumer commits")
		return
	}
	if !requirePost(w, r) {
		return
	}

	var request commitRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.NextOffset == nil {
		writeError(w, http.StatusBadRequest, "next_offset is required")
		return
	}
	if err := a.broker.Commit(request.Group, request.Topic, *request.NextOffset); err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response{OK: true})
}

func (a *API) retain(w http.ResponseWriter, r *http.Request) {
	if a.readOnly {
		writeError(w, http.StatusMethodNotAllowed, "follower nodes do not accept retention changes")
		return
	}
	if !requirePost(w, r) {
		return
	}
	var request retentionRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.StartOffset == nil {
		writeError(w, http.StatusBadRequest, "start_offset is required")
		return
	}
	if err := a.broker.RetainFrom(request.Topic, *request.StartOffset); err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response{OK: true})
}

func (a *API) snapshot(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	info, err := a.broker.CreateSnapshot()
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(snapshotResponse{
		OK: true, Generation: info.Generation, WALOffset: info.WALOffset,
	})
}

type replicationStatusResponse struct {
	Offset int64  `json:"offset"`
	Hash   string `json:"hash"`
	Epoch  uint64 `json:"epoch"`
}

func (a *API) replicationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method must be GET")
		return
	}
	if !a.authorizeReplication(w, r) {
		return
	}
	offset, hash, epoch, err := a.broker.WALReplicationStatus()
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(replicationStatusResponse{Offset: offset, Hash: hex.EncodeToString(hash[:]), Epoch: epoch})
}

func (a *API) replicationMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method must be GET")
		return
	}
	if !a.authorizeReplication(w, r) {
		return
	}
	metrics, err := a.replicationMetricsProvider()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(metrics)
}

func (a *API) replicationFrames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if !a.authorizeReplication(w, r) {
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "offset must be a non-negative WAL byte position")
		return
	}
	epoch, err := strconv.ParseUint(r.Header.Get("X-Leader-Epoch"), 10, 64)
	if err != nil || epoch == 0 {
		writeError(w, http.StatusBadRequest, "X-Leader-Epoch must be a positive integer")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxReplicaBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "replication frame batch exceeds 4 MiB")
		} else {
			writeError(w, http.StatusBadRequest, "could not read replication frames")
		}
		return
	}
	if err := a.broker.ReplicateWALFrames(offset, epoch, data); err != nil {
		if errors.Is(err, broker.ErrBrokerUnavailable) {
			writeBrokerError(w, err)
		} else {
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) replicationEpoch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if !a.authorizeReplication(w, r) {
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil || epoch == 0 {
		writeError(w, http.StatusBadRequest, "epoch must be a positive integer")
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "offset must be a non-negative WAL byte position")
		return
	}
	hash, err := hex.DecodeString(r.URL.Query().Get("hash"))
	if err != nil || len(hash) != 32 {
		writeError(w, http.StatusBadRequest, "hash must be a SHA-256 value")
		return
	}
	var expectedHash [32]byte
	copy(expectedHash[:], hash)
	if err := a.broker.AdvanceLeaderEpoch(epoch, offset, expectedHash); err != nil {
		writeBrokerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) replicationSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if !a.authorizeReplication(w, r) {
		return
	}
	epoch, err := strconv.ParseUint(r.Header.Get("X-Leader-Epoch"), 10, 64)
	if err != nil || epoch == 0 {
		writeError(w, http.StatusBadRequest, "X-Leader-Epoch must be a positive integer")
		return
	}
	generation, err := strconv.ParseUint(r.Header.Get("X-Snapshot-Generation"), 10, 64)
	if err != nil || generation == 0 {
		writeError(w, http.StatusBadRequest, "X-Snapshot-Generation must be a positive integer")
		return
	}
	checksum, err := hex.DecodeString(r.Header.Get("X-Snapshot-Checksum"))
	if err != nil || len(checksum) != sha256.Size {
		writeError(w, http.StatusBadRequest, "X-Snapshot-Checksum must be a SHA-256 value")
		return
	}
	var digest [sha256.Size]byte
	copy(digest[:], checksum)
	r.Body = http.MaxBytesReader(w, r.Body, broker.MaxSnapshotTransferBytes())
	if err := a.broker.InstallReplicaSnapshot(r.Body, generation, digest, epoch); err != nil {
		switch {
		case errors.Is(err, broker.ErrStaleSnapshot) || errors.Is(err, broker.ErrSnapshotRejected):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, broker.ErrNotFollower) || errors.Is(err, broker.ErrLeaderEpochMismatch):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, broker.ErrBrokerClosed) || errors.Is(err, broker.ErrBrokerUnavailable):
			writeBrokerError(w, err)
		default:
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) authorizeReplication(w http.ResponseWriter, r *http.Request) bool {
	provided := r.Header.Get("X-Replication-Token")
	if subtle.ConstantTimeCompare([]byte(provided), []byte(a.replicationToken)) == 1 {
		return true
	}
	writeError(w, http.StatusUnauthorized, "invalid replication token")
	return false
}

func (a *API) notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "endpoint not found")
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost {
		return true
	}
	w.Header().Set("Allow", http.MethodPost)
	writeError(w, http.StatusMethodNotAllowed, "method must be POST")
	return false
}

func validLimit(w http.ResponseWriter, limit int) bool {
	if limit >= 1 && limit <= maxBatchRecords {
		return true
	}
	writeError(w, http.StatusBadRequest, "limit must be between 1 and 32")
	return false
}

func decodeRequest(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeRequestDecodeError(w, err, "invalid JSON request")
		return false
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeRequestDecodeError(w, err, "request must contain one JSON object")
		return false
	}
	return true
}

func writeRequestDecodeError(w http.ResponseWriter, err error, message string) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1 MiB")
		return
	}
	writeError(w, http.StatusBadRequest, message)
}

func writeBrokerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, broker.ErrInvalidTopic), errors.Is(err, broker.ErrInvalidGroup):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, broker.ErrInvalidRequestID):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, broker.ErrReadOnly), errors.Is(err, broker.ErrNotFollower):
		writeError(w, http.StatusMethodNotAllowed, err.Error())
	case errors.Is(err, broker.ErrReplicationUnavailable), errors.Is(err, broker.ErrBrokerUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, broker.ErrTopicNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, broker.ErrOffsetExpired):
		writeError(w, http.StatusGone, err.Error())
	case errors.Is(err, broker.ErrCommitOutOfRange), errors.Is(err, broker.ErrCommitRegression), errors.Is(err, broker.ErrRetentionOutOfRange), errors.Is(err, broker.ErrRetentionRegression), errors.Is(err, broker.ErrIdempotencyConflict), errors.Is(err, broker.ErrStaleLeader), errors.Is(err, broker.ErrLeaderEpochMismatch), errors.Is(err, broker.ErrReplicationStateChanged):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, broker.ErrBatchTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, broker.ErrEmptyBatch):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal broker error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, response{OK: false, Error: message})
}

func writeJSON(w http.ResponseWriter, status int, value response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
