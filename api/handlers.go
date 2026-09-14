package main

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var sha256HexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// createTokenRE matches the client-generated idempotency key: 32 lowercase
// hex characters (16 random bytes), generated for one "start upload" and
// saved locally before the first request.
var createTokenRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

type Server struct {
	db      *sql.DB
	dataDir string
	mu      sync.Mutex // serializes chunk ingestion and assembly per process
	hub     *progressHub
}

func NewServer(db *sql.DB, dataDir string) (*Server, error) {
	for _, d := range []string{filepath.Join(dataDir, "chunks"), filepath.Join(dataDir, "artifacts")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return &Server{db: db, dataDir: dataDir, hub: newProgressHub()}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("POST /api/sessions", s.handleCreateSession)
	mux.HandleFunc("GET /api/sessions/{id}", s.handleGetSession)
	mux.HandleFunc("GET /api/sessions/{id}/events", s.handleProgressEvents)
	mux.HandleFunc("POST /api/sessions/{id}/chunks/{index}", s.handleUploadChunk)
	mux.HandleFunc("POST /api/sessions/{id}/assemble", s.handleAssemble)
	mux.HandleFunc("GET /api/sessions/{id}/download", s.handleDownload)
	return cors(mux)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Chunk-SHA256, Range, Last-Event-ID")
		w.Header().Set("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, Content-Length, Content-Disposition")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// rejectTrailingJSON fails any non-whitespace token left after the first
// decoded JSON value, so bodies like {"...":...}{"...":...} are rejected.
// Trailing whitespace alone is legal per RFC 8259.
func rejectTrailingJSON(dec *json.Decoder) error {
	var extra json.RawMessage
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("invalid trailing content after JSON body")
	}
	return fmt.Errorf("request body must contain exactly one JSON object")
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- session JSON ----

type sessionJSON struct {
	SessionID      string  `json:"session_id"`
	Filename       string  `json:"filename"`
	TotalBytes     int64   `json:"total_bytes"`
	ChunkCount     int64   `json:"chunk_count"`
	ChunkSize      int64   `json:"chunk_size"`
	FileSHA256     string  `json:"file_sha256"`
	Status         string  `json:"status"`
	ReceivedCount  int64   `json:"received_count"`
	ConfirmedBytes int64   `json:"confirmed_bytes"`
	MissingChunks  []int64 `json:"missing_chunks"`
	FinalSHA256    *string `json:"final_sha256"`
	Error          *string `json:"error"`
	ArtifactSource *string `json:"artifact_source"`
}

func (s *Server) sessionResponse(sess *Session) (*sessionJSON, error) {
	chunks, err := s.listChunks(sess.ID)
	if err != nil {
		return nil, err
	}
	present := make(map[int64]bool, len(chunks))
	var confirmed int64
	for _, c := range chunks {
		present[c.Index] = true
		confirmed += c.Size
	}
	missing := make([]int64, 0)
	if sess.Status != StatusCompleted {
		// A completed session (including an artifact-reuse hit, which owns
		// no chunk rows) has nothing missing by definition.
		for i := int64(0); i < sess.ChunkCount; i++ {
			if !present[i] {
				missing = append(missing, i)
			}
		}
	}
	resp := &sessionJSON{
		SessionID:      sess.ID,
		Filename:       sess.Filename,
		TotalBytes:     sess.TotalBytes,
		ChunkCount:     sess.ChunkCount,
		ChunkSize:      ChunkSize,
		FileSHA256:     sess.FileSHA256,
		Status:         sess.Status,
		ReceivedCount:  int64(len(chunks)),
		ConfirmedBytes: confirmed,
		MissingChunks:  missing,
	}
	if sess.FinalSHA256.Valid {
		resp.FinalSHA256 = &sess.FinalSHA256.String
	}
	if sess.Error.Valid {
		resp.Error = &sess.Error.String
	}
	if sess.ArtifactSource.Valid {
		resp.ArtifactSource = &sess.ArtifactSource.String
	}
	return resp, nil
}

// ---- handlers ----

type createSessionReq struct {
	Filename   string `json:"filename"`
	TotalBytes int64  `json:"total_bytes"`
	ChunkCount int64  `json:"chunk_count"`
	FileSHA256 string `json:"file_sha256"`
	// ReuseArtifact declares the client's willingness to reuse an already
	// published artifact of identical content instead of uploading every
	// chunk again. Optional: old clients omit it and always get a plain
	// upload session.
	ReuseArtifact bool `json:"reuse_artifact"`
	// CreateToken is the client-generated idempotency key (32 lowercase hex).
	// The page generates one for each "start upload", saves it locally, and
	// re-sends the same token when the first create response was lost to a
	// network failure/timeout. Optional: old clients omit it and always get a
	// brand-new independent session.
	CreateToken string `json:"create_token"`
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionReq
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	// The body must contain exactly one JSON value: a second object or any
	// other trailing token is rejected (trailing whitespace is allowed).
	if err := rejectTrailingJSON(dec); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Filename = strings.TrimSpace(req.Filename)
	if req.Filename == "" || len(req.Filename) > 255 {
		writeErr(w, http.StatusBadRequest, "filename must be 1..255 characters")
		return
	}
	token := strings.TrimSpace(req.CreateToken)
	if token != "" && !createTokenRE.MatchString(token) {
		writeErr(w, http.StatusBadRequest, "create_token must be 32 lowercase hex characters when present")
		return
	}
	if !sha256HexRE.MatchString(req.FileSHA256) {
		writeErr(w, http.StatusBadRequest, "file_sha256 must be 64 lowercase hex characters")
		return
	}

	// A request carrying a token that is ALREADY known replays (or conflicts
	// against) the original create. Resolve it BEFORE validating that
	// chunk_count matches total_bytes: the retry contract compares the five
	// metadata fields verbatim, and a replayed chunk_count different from the
	// original is a 409 token conflict (not a 400 count error), exactly like a
	// different filename or digest. A brand-new token keeps every strict
	// validation below.
	if token != "" {
		if existing, err := s.sessionByToken(token); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		} else if existing != nil {
			s.resolveKnownToken(w, r, createIntent{
				Filename:      req.Filename,
				TotalBytes:    req.TotalBytes,
				ChunkCount:    req.ChunkCount,
				FileSHA256:    req.FileSHA256,
				ReuseArtifact: req.ReuseArtifact,
				Token:         token,
			}, existing)
			return
		}
	}

	if req.TotalBytes < 1 {
		writeErr(w, http.StatusBadRequest, "total_bytes must be >= 1")
		return
	}
	if req.TotalBytes > MaxTotalBytes {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("total_bytes %d exceeds supported maximum %d", req.TotalBytes, MaxTotalBytes))
		return
	}
	if req.ChunkCount < 1 {
		writeErr(w, http.StatusBadRequest, "chunk_count must be >= 1")
		return
	}
	// Overflow-safe ceil(total_bytes / ChunkSize): adding ChunkSize-1 to a
	// near-MaxInt64 total would wrap negative and accept a bogus negative
	// chunk_count, which describes no valid chunk range.
	wantChunks := req.TotalBytes / ChunkSize
	if req.TotalBytes%ChunkSize != 0 {
		wantChunks++
	}
	if req.ChunkCount != wantChunks {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("chunk_count %d does not match total_bytes (expect %d)", req.ChunkCount, wantChunks))
		return
	}

	in := createIntent{
		Filename:      req.Filename,
		TotalBytes:    req.TotalBytes,
		ChunkCount:    req.ChunkCount,
		FileSHA256:    req.FileSHA256,
		ReuseArtifact: req.ReuseArtifact,
		Token:         token,
	}

	// Look up a reusable artifact before opening the idempotency transaction:
	// the candidate check stats files on the volume and must not hold the
	// SQLite write lock. The outcome is only persisted by the transaction,
	// whose token check is what makes a lost-response retry repeat the very
	// same result.
	var owner *Session
	if req.ReuseArtifact {
		o, err := s.findReusableArtifact(req.FileSHA256, req.TotalBytes)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		owner = o
	}

	// Old clients (no create token): keep the historical behavior exactly —
	// every request creates an independent session.
	if token == "" {
		s.createWithoutToken(w, in, owner)
		return
	}

	id, err := newSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot allocate session id")
		return
	}
	// The token was checked above and found unknown; createWithToken now
	// serializes concurrent same-token inserts in ONE transaction (exactly
	// one wins as 201, the rest replay as 200; a racer carrying the same
	// token with other metadata still gets 409).
	sess, replayed, err := s.createWithToken(r.Context(), in, owner, id)
	if errors.Is(err, ErrCreateTokenConflict) {
		writeErr(w, http.StatusConflict,
			"create token was already used with different metadata; start a new delivery instead of retrying this one")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot persist session")
		return
	}
	if replayed {
		// Lost the race with a concurrent same-token request that committed
		// first: answer with the winner's current snapshot as 200.
		cur, err := s.getSession(sess.ID)
		if err != nil || cur == nil {
			writeErr(w, http.StatusInternalServerError, "cannot reload session")
			return
		}
		resp, err := s.sessionResponse(cur)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if sess.Status != StatusCompleted {
		// Fresh plain upload session: create its chunk directory exactly once.
		// Reuse-hit sessions own no chunks and need none.
		if err := os.MkdirAll(s.chunkDir(sess.ID), 0o755); err != nil {
			writeErr(w, http.StatusInternalServerError, "cannot create chunk directory")
			return
		}
	}
	resp, err := s.sessionResponse(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

// resolveKnownToken answers a create request carrying a token that already
// belongs to a session. Identical metadata replays that session's CURRENT
// snapshot as 200 (no second row, no second chunk directory, reuse-hit
// outcomes preserved); any differing field — filename, total bytes, chunk
// count, digest, or reuse willingness — is a 409 conflict that leaves the
// original session untouched.
func (s *Server) resolveKnownToken(w http.ResponseWriter, r *http.Request, in createIntent, existing *Session) {
	if !existing.matchesIntent(in) {
		writeErr(w, http.StatusConflict,
			"create token was already used with different metadata; start a new delivery instead of retrying this one")
		return
	}
	// Answer with the current snapshot so progress (or a completion) reached
	// while the first response was lost is visible immediately.
	cur, err := s.getSession(existing.ID)
	if err != nil || cur == nil {
		writeErr(w, http.StatusInternalServerError, "cannot reload session")
		return
	}
	resp, err := s.sessionResponse(cur)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// createWithoutToken keeps the historical create behavior for old clients:
// every request inserts an independent upload session (or a completed
// reuse-hit session) with no token.
func (s *Server) createWithoutToken(w http.ResponseWriter, in createIntent, owner *Session) {
	id, err := newSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot allocate session id")
		return
	}
	sess := &Session{
		ID:             id,
		Filename:       in.Filename,
		TotalBytes:     in.TotalBytes,
		ChunkCount:     in.ChunkCount,
		FileSHA256:     in.FileSHA256,
		ReuseRequested: in.ReuseArtifact,
	}
	if owner != nil {
		// Reuse hit: the new session is completed at creation, points at the
		// already published artifact, and needs zero chunk uploads.
		sess.Status = StatusCompleted
		sess.FinalSHA256 = sql.NullString{String: in.FileSHA256, Valid: true}
		sess.ArtifactSource = sql.NullString{String: owner.ID, Valid: true}
		if err := s.createReusedSession(sess, owner.ID); err != nil {
			writeErr(w, http.StatusInternalServerError, "cannot persist session")
			return
		}
		resp, err := s.sessionResponse(sess)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, resp)
		return
	}
	sess.Status = StatusUploading
	if err := s.createSession(sess); err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot persist session")
		return
	}
	if err := os.MkdirAll(s.chunkDir(id), 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot create chunk directory")
		return
	}
	resp, err := s.sessionResponse(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.getSession(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sess == nil {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}
	resp, err := s.sessionResponse(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleUploadChunk(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.getSession(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sess == nil {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}
	if sess.Status != StatusUploading {
		writeErr(w, http.StatusConflict, "session is "+sess.Status)
		return
	}
	index, err := strconv.ParseInt(r.PathValue("index"), 10, 64)
	if err != nil || index < 0 || index >= sess.ChunkCount {
		writeErr(w, http.StatusBadRequest, "chunk index out of range")
		return
	}

	wantLen := ExpectedChunkLen(sess.TotalBytes, sess.ChunkCount, index)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, wantLen+1))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "chunk body too large")
		return
	}
	if int64(len(body)) != wantLen {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("chunk %d must be exactly %d bytes, got %d", index, wantLen, len(body)))
		return
	}

	sum := sha256.Sum256(body)
	actualSHA := hex.EncodeToString(sum[:])
	// Protocol requires lowercase hex: compare the header verbatim instead
	// of normalising case, so an all-uppercase (but otherwise correct)
	// digest is rejected and the chunk stays unconfirmed.
	declared := strings.TrimSpace(r.Header.Get("X-Chunk-SHA256"))
	if !sha256HexRE.MatchString(declared) {
		writeErr(w, http.StatusBadRequest, "X-Chunk-SHA256 header must be 64 lowercase hex characters")
		return
	}
	if declared != actualSHA {
		writeErr(w, http.StatusBadRequest, "chunk content does not match X-Chunk-SHA256 header")
		return
	}

	existing, err := s.findChunk(sess.ID, index)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if existing != nil {
		if existing.SHA256 == actualSHA {
			// Idempotent retry: same index, same digest -> success, not counted twice.
			// No state changes, so the progress sequence does not advance.
			s.respondChunk(w, http.StatusOK, sess, index, actualSHA, wantLen, true)
			return
		}
		// Same index, different content: freeze the session as failed.
		reason := fmt.Sprintf("chunk %d conflict: stored sha256 %s, received %s",
			index, existing.SHA256, actualSHA)
		if _, err := s.failSessionTx(sess.ID, reason); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.emitProgress(sess.ID)
		writeErr(w, http.StatusConflict, reason)
		return
	}

	if err := s.persistChunk(sess.ID, index, body); err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot persist chunk: "+err.Error())
		return
	}
	// The chunk row and the bumped progress sequence commit together:
	// subscribers can never observe a new sequence without the new chunk.
	if _, err := s.confirmChunkTx(sess.ID, index, actualSHA, wantLen); err != nil {
		writeErr(w, http.StatusInternalServerError, "cannot record chunk: "+err.Error())
		return
	}
	s.emitProgress(sess.ID)
	s.respondChunk(w, http.StatusCreated, sess, index, actualSHA, wantLen, false)
}

func (s *Server) respondChunk(w http.ResponseWriter, status int, sess *Session, index int64, sha string, size int64, duplicate bool) {
	count, confirmed, err := s.chunkStats(sess.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, status, map[string]any{
		"session_id":      sess.ID,
		"index":           index,
		"sha256":          sha,
		"size":            size,
		"duplicate":       duplicate,
		"received_count":  count,
		"confirmed_bytes": confirmed,
		"status":          sess.Status,
	})
}

// persistChunk writes the chunk to the data volume atomically (temp file + rename).
func (s *Server) persistChunk(sessionID string, index int64, body []byte) error {
	dir := s.chunkDir(sessionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.chunkPath(sessionID, index))
}

func (s *Server) handleAssemble(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.getSession(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sess == nil {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}
	if sess.Status != StatusUploading {
		writeErr(w, http.StatusConflict, "session is "+sess.Status)
		return
	}

	chunks, err := s.listChunks(sess.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if int64(len(chunks)) != sess.ChunkCount {
		resp, _ := s.sessionResponse(sess)
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":          "cannot assemble: chunks missing",
			"missing_chunks": resp.MissingChunks,
		})
		return
	}

	if err := os.MkdirAll(s.artifactDir(), 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	tmp, err := os.CreateTemp(s.artifactDir(), ".assemble-*")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	tmpName := tmp.Name()

	whole := sha256.New()
	out := io.MultiWriter(tmp, whole)
	for _, c := range chunks {
		if err := appendChunk(out, s.chunkPath(sess.ID, c.Index), c.SHA256); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			reason := fmt.Sprintf("stored chunk %d failed integrity check: %v", c.Index, err)
			s.failWithSession(w, sess, reason)
			return
		}
	}
	finalSHA := hex.EncodeToString(whole.Sum(nil))

	if finalSHA != sess.FileSHA256 {
		tmp.Close()
		os.Remove(tmpName)
		reason := fmt.Sprintf("assembled sha256 %s does not match declared %s", finalSHA, sess.FileSHA256)
		s.failWithSession(w, sess, reason)
		return
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Atomic publish: rename within the same filesystem.
	finalPath := s.artifactPath(sess)
	if err := os.Rename(tmpName, finalPath); err != nil {
		os.Remove(tmpName)
		writeErr(w, http.StatusInternalServerError, "publish failed: "+err.Error())
		return
	}
	if _, err := s.completeSessionTx(sess.ID, finalSHA); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	sess.Status = StatusCompleted
	sess.FinalSHA256 = sql.NullString{String: finalSHA, Valid: true}
	sess.ProgressVersion++
	s.emitProgress(sess.ID)
	resp, err := s.sessionResponse(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// failWithSession freezes the session and answers with the full session state
// so clients can directly observe the failure.
func (s *Server) failWithSession(w http.ResponseWriter, sess *Session, reason string) {
	newSeq, err := s.failSessionTx(sess.ID, reason)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	sess.Status = StatusFailed
	sess.Error = sql.NullString{String: reason, Valid: true}
	sess.ProgressVersion = newSeq
	s.emitProgress(sess.ID)
	resp, err := s.sessionResponse(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusConflict, resp)
}

// appendChunk streams one stored chunk into w while re-verifying its digest.
func appendChunk(w io.Writer, path, wantSHA string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), f); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantSHA {
		return fmt.Errorf("sha256 %s != recorded %s", got, wantSHA)
	}
	return nil
}

// ---- artifact download ----

// handleDownload serves the published artifact of a completed session.
// A plain GET returns the whole file (200); a single, satisfiable Range
// returns 206 so interrupted transfers can resume from a byte offset.
// Multi-range or unsatisfiable requests are rejected with 416 and no body.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	sess, err := s.getSession(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sess == nil {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}
	if sess.Status != StatusCompleted {
		writeErr(w, http.StatusConflict, "session is "+sess.Status+": artifact not published")
		return
	}

	// A reused session owns no artifact file itself: the body is read from
	// the backing session's published artifact, while the response filename
	// still comes from this session's own submission.
	owner, err := s.artifactOwner(sess)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if owner == nil {
		writeErr(w, http.StatusGone, "artifact missing on storage volume")
		return
	}
	f, err := os.Open(s.artifactPath(owner))
	if err != nil {
		if os.IsNotExist(err) {
			// Database says completed but the artifact is gone from the volume.
			writeErr(w, http.StatusGone, "artifact missing on storage volume")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	size := st.Size()

	h := w.Header()
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Disposition", contentDisposition(sess.Filename))

	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	if rangeHeader == "" {
		h.Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = io.Copy(w, f)
		}
		return
	}

	start, length, ok := parseSingleRange(rangeHeader, size)
	if !ok {
		// Unsatisfiable / multi-range / malformed: 416 with no body.
		h.Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.CopyN(w, io.NewSectionReader(f, start, length), length)
}

// parseSingleRange parses exactly one HTTP byte-range spec against the
// artifact size. It returns ok=false for malformed specs, multi-range
// requests (not supported), and ranges that cannot be satisfied.
func parseSingleRange(header string, size int64) (start, length int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes=")
	if !found || spec == "" || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	switch {
	case first == "":
		// Suffix range: the last N bytes.
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, n, true
	case last == "":
		// Open range: from start to the end of the file.
		s, err := strconv.ParseInt(first, 10, 64)
		if err != nil || s < 0 || s >= size {
			return 0, 0, false
		}
		return s, size - s, true
	default:
		s, err1 := strconv.ParseInt(first, 10, 64)
		e, err2 := strconv.ParseInt(last, 10, 64)
		if err1 != nil || err2 != nil || s < 0 || e < s || s >= size {
			return 0, 0, false
		}
		if e >= size {
			e = size - 1
		}
		return s, e - s + 1, true
	}
}

// contentDisposition keeps the original filename for the browser download:
// an ASCII fallback for legacy clients plus the RFC 5987 UTF-8 form.
func contentDisposition(filename string) string {
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`,
		sanitizeFilename(filename), url.PathEscape(filename))
}

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	var sb strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteRune('_')
		}
	}
	if sb.Len() == 0 {
		return "file"
	}
	return sb.String()
}
