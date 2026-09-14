package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// ChunkSize is the fixed block size: exactly 1 MiB for every chunk except the last.
const ChunkSize int64 = 1048576

// MaxTotalBytes bounds a single delivery's declared size. Fixed 1 MiB
// chunking plus the per-session missing-chunk list cannot represent
// petabyte-scale metadata, so near-MaxInt64 totals are rejected at creation.
const MaxTotalBytes int64 = 1 << 40 // 1 TiB (at most 1<<20 chunks)

const (
	StatusUploading = "uploading"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Session struct {
	ID          string
	Filename    string
	TotalBytes  int64
	ChunkCount  int64
	FileSHA256  string
	Status      string
	FinalSHA256 sql.NullString
	Error       sql.NullString
	// ArtifactSource, when set, is the id of the completed session whose
	// published artifact physically backs this one (repeat delivery of the
	// same content). NULL for sessions that uploaded their own chunks.
	ArtifactSource sql.NullString
	// ProgressVersion is the persisted SSE progress sequence. It starts at 1
	// when the session row is created and is incremented inside the very same
	// transactions that confirm a chunk, freeze the session on a conflict, or
	// publish the assembly, so the event stream never misses or reorders an
	// update and the sequence keeps growing across process restarts.
	ProgressVersion int64
	// CreateToken is the client-supplied idempotency key carried by a create
	// request ("start upload" generated it before the first attempt and saved
	// it locally). NULL for old clients. A retry after a lost create response
	// presents the same token: the server answers again with the exact same
	// session snapshot instead of inserting a second row.
	CreateToken sql.NullString
	// ReuseRequested records whether the create request declared the
	// artifact-reuse willingness (reuse_artifact:true). It is persisted
	// separately from the outcome: a retry carrying the same create token
	// must match the willingness of the first attempt even when that attempt
	// missed every reusable candidate and fell back to a plain upload.
	ReuseRequested bool
}

// ChunkMeta is the confirmed-chunk metadata persisted in SQLite.
type ChunkMeta struct {
	Index  int64
	SHA256 string
	Size   int64
}

func openDB(dataDir string) (*sql.DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)",
		filepath.Join(dataDir, "app.db"))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Single writer keeps SQLite free of SQLITE_BUSY under concurrent uploads.
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func migrate(db *sql.DB) error {
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS sessions (
    id            TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    total_bytes   INTEGER NOT NULL,
    chunk_count   INTEGER NOT NULL,
    file_sha256   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'uploading',
    final_sha256  TEXT,
    error         TEXT,
    artifact_source TEXT,
    progress_version INTEGER NOT NULL DEFAULT 1,
    create_token  TEXT,
    reuse_requested INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS chunks (
    session_id   TEXT NOT NULL REFERENCES sessions(id),
    chunk_index  INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (session_id, chunk_index)
);`); err != nil {
		return err
	}
	// Databases created before artifact reuse existed lack the column;
	// add it nullable so existing session rows need no data migration.
	has, err := hasColumn(db, "sessions", "artifact_source")
	if err != nil {
		return err
	}
	if !has {
		if _, err := db.Exec(`ALTER TABLE sessions ADD COLUMN artifact_source TEXT`); err != nil {
			return err
		}
	}
	// Databases created before the SSE progress stream existed lack the
	// sequence column. Existing rows start at 1: a fresh SSE subscription on
	// an old session first receives its current snapshot and only subsequent
	// (new-binary) state changes produce numbered updates.
	has, err = hasColumn(db, "sessions", "progress_version")
	if err != nil {
		return err
	}
	if !has {
		if _, err := db.Exec(`ALTER TABLE sessions ADD COLUMN progress_version INTEGER NOT NULL DEFAULT 1`); err != nil {
			return err
		}
	}
	// Databases created before idempotent session creation existed lack the
	// client create-token column. It is nullable (old sessions and old
	// clients carry NULL) and UNIQUE: SQLite permits multiple NULLs, so the
	// index can be built in place over existing rows. The index is what lets
	// a retried "start upload" resolve the original session atomically even
	// under concurrent requests carrying the same token.
	has, err = hasColumn(db, "sessions", "create_token")
	if err != nil {
		return err
	}
	if !has {
		if _, err := db.Exec(`ALTER TABLE sessions ADD COLUMN create_token TEXT`); err != nil {
			return err
		}
	}
	// reuse_requested accompanies create_token: old rows default to 0 (old
	// clients never declared reuse intent).
	has, err = hasColumn(db, "sessions", "reuse_requested")
	if err != nil {
		return err
	}
	if !has {
		if _, err := db.Exec(`ALTER TABLE sessions ADD COLUMN reuse_requested INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, err := db.Exec(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_create_token ON sessions(create_token) WHERE create_token IS NOT NULL`); err != nil {
		return err
	}
	return nil
}

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, rows.Err()
		}
	}
	return false, rows.Err()
}

// ExpectedChunkLen returns the only valid length for chunk index idx.
// Every chunk except the last is exactly ChunkSize; the last is uniquely
// determined by the total byte count.
func ExpectedChunkLen(totalBytes, chunkCount, idx int64) int64 {
	if idx == chunkCount-1 {
		return totalBytes - ChunkSize*(chunkCount-1)
	}
	return ChunkSize
}

func (s *Server) getSession(id string) (*Session, error) {
	sess, err := scanSession(s.db.QueryRow(
		`SELECT `+sessionColumns+` FROM sessions WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return sess, nil
}

// sessionByToken looks up a session by its client-supplied create token.
// Returns (nil, nil) when no session carries the token.
func (s *Server) sessionByToken(token string) (*Session, error) {
	sess, err := scanSession(s.db.QueryRow(
		`SELECT `+sessionColumns+` FROM sessions WHERE create_token = ?`, token))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *Server) listChunks(sessionID string) ([]ChunkMeta, error) {
	rows, err := s.db.Query(
		`SELECT chunk_index, sha256, size FROM chunks WHERE session_id = ? ORDER BY chunk_index`,
		sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkMeta
	for rows.Next() {
		var c ChunkMeta
		if err := rows.Scan(&c.Index, &c.SHA256, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Server) chunkStats(sessionID string) (count int64, confirmedBytes int64, err error) {
	err = s.db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(size), 0) FROM chunks WHERE session_id = ?`,
		sessionID).Scan(&count, &confirmedBytes)
	return
}

func (s *Server) findChunk(sessionID string, index int64) (*ChunkMeta, error) {
	var c ChunkMeta
	err := s.db.QueryRow(
		`SELECT chunk_index, sha256, size FROM chunks WHERE session_id = ? AND chunk_index = ?`,
		sessionID, index).Scan(&c.Index, &c.SHA256, &c.Size)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// dbtx is satisfied by *sql.DB, *sql.Tx and *sql.Conn, so the session INSERT
// statements run either standalone (old clients, no create token) or inside
// the idempotency transaction opened on a dedicated connection (clients
// carrying a create token).
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertSession(ctx context.Context, q dbtx, sess *Session, token sql.NullString, reuseRequested bool) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO sessions
		    (id, filename, total_bytes, chunk_count, file_sha256, status, progress_version, create_token, reuse_requested)
		 VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		sess.ID, sess.Filename, sess.TotalBytes, sess.ChunkCount, sess.FileSHA256,
		StatusUploading, token, reuseRequested)
	return err
}

// insertReusedSession persists a session that is completed at creation time:
// it owns no chunks and points at the artifact published by sourceID. Its
// progress sequence also starts at 1: the snapshot delivered to a brand-new
// subscription is the very first event, not an update.
func insertReusedSession(ctx context.Context, q dbtx, sess *Session, sourceID string, token sql.NullString, reuseRequested bool) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO sessions
		    (id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256,
		     artifact_source, progress_version, create_token, reuse_requested)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		sess.ID, sess.Filename, sess.TotalBytes, sess.ChunkCount, sess.FileSHA256,
		StatusCompleted, sess.FileSHA256, sourceID, token, reuseRequested)
	return err
}

func (s *Server) createSession(sess *Session) error {
	return insertSession(context.Background(), s.db, sess, sql.NullString{}, false)
}

func (s *Server) createReusedSession(sess *Session, sourceID string) error {
	return insertReusedSession(context.Background(), s.db, sess, sourceID, sql.NullString{}, true)
}

// ErrCreateTokenConflict is returned by the idempotent create path when a
// known create token is presented again alongside different session metadata
// (filename, total bytes, chunk count, digest, or reuse willingness). The
// existing session is left untouched and the caller must start over with a
// fresh token.
var ErrCreateTokenConflict = errors.New("create token was already used with different session metadata")

// createIntent is the validated payload of a session-create request.
type createIntent struct {
	Filename      string
	TotalBytes    int64
	ChunkCount    int64
	FileSHA256    string
	ReuseArtifact bool
	Token         string // empty for old clients that carry no create token
}

// sessionColumns lists every persisted session column in the SELECT order
// used by scanSession.
const sessionColumns = `id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256, error, artifact_source, progress_version, create_token, reuse_requested`

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanSession(row rowScanner) (*Session, error) {
	var sess Session
	err := row.Scan(&sess.ID, &sess.Filename, &sess.TotalBytes, &sess.ChunkCount,
		&sess.FileSHA256, &sess.Status, &sess.FinalSHA256, &sess.Error,
		&sess.ArtifactSource, &sess.ProgressVersion, &sess.CreateToken, &sess.ReuseRequested)
	if err != nil {
		return nil, err
	}
	return &sess, nil
}

// matchesIntent reports whether a session previously created under the same
// token describes exactly the delivery the retry asks for. The reuse
// willingness is compared as persisted (not inferred from the outcome), so a
// token cannot be replayed with a different reuse_artifact declaration.
func (sess *Session) matchesIntent(in createIntent) bool {
	return sess.Filename == in.Filename &&
		sess.TotalBytes == in.TotalBytes &&
		sess.ChunkCount == in.ChunkCount &&
		sess.FileSHA256 == in.FileSHA256 &&
		sess.ReuseRequested == in.ReuseArtifact
}

// createWithToken is the idempotent create path, executed as ONE
// transaction:
//
//   - The token is looked up first. When a session already carries it and the
//     metadata is byte-for-byte the same intent, that original session is
//     returned (replayed=true): no second row and no second chunk directory
//     are produced, and the answer is the session's CURRENT snapshot, so a
//     retry after a lost response also observes chunks confirmed in between
//     (or an artifact-reuse hit already completed at creation).
//   - Same token, different metadata -> ErrCreateTokenConflict; the original
//     session is not touched.
//   - Unknown token -> the new session row is inserted in the same
//     transaction (as a plain upload session, or completed pointing at
//     reuseOwner when a reusable artifact was found before the transaction).
//
// The transaction opens BEGIN IMMEDIATE, taking SQLite's write lock up front:
// two concurrent requests carrying the same brand-new token serialize there,
// and the second one then resolves the row committed by the first instead of
// colliding on the unique index.
func (s *Server) createWithToken(ctx context.Context, in createIntent, reuseOwner *Session, newID string) (sess *Session, replayed bool, err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	existing, qerr := scanSession(conn.QueryRowContext(ctx,
		`SELECT `+sessionColumns+` FROM sessions WHERE create_token = ?`, in.Token))
	if qerr != nil && qerr != sql.ErrNoRows {
		return nil, false, qerr
	}
	if qerr == nil {
		// The first create already committed (its response was lost): replay
		// the original session when the intent matches, otherwise refuse.
		if !existing.matchesIntent(in) {
			return nil, false, ErrCreateTokenConflict
		}
		// Read-only resolution: release the write lock.
		if _, err := conn.ExecContext(ctx, `ROLLBACK`); err != nil {
			return nil, false, err
		}
		committed = true
		return existing, true, nil
	}

	sess = &Session{
		ID:             newID,
		Filename:       in.Filename,
		TotalBytes:     in.TotalBytes,
		ChunkCount:     in.ChunkCount,
		FileSHA256:     in.FileSHA256,
		CreateToken:    sql.NullString{String: in.Token, Valid: true},
		ReuseRequested: in.ReuseArtifact,
	}
	token := sql.NullString{String: in.Token, Valid: true}
	if reuseOwner != nil {
		// Reuse hit decided before the transaction: completed at creation,
		// pointing at the already published artifact, zero chunk uploads.
		sess.Status = StatusCompleted
		sess.FinalSHA256 = sql.NullString{String: in.FileSHA256, Valid: true}
		sess.ArtifactSource = sql.NullString{String: reuseOwner.ID, Valid: true}
		if err := insertReusedSession(ctx, conn, sess, reuseOwner.ID, token, in.ReuseArtifact); err != nil {
			return nil, false, err
		}
	} else {
		sess.Status = StatusUploading
		if err := insertSession(ctx, conn, sess, token, in.ReuseArtifact); err != nil {
			return nil, false, err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, false, err
	}
	committed = true
	return sess, false, nil
}

// artifactOwner resolves the session whose own artifact file physically backs
// sess: reused sessions follow their artifact_source reference (created
// pointing at the ultimate source, so normally a single hop), ordinary
// sessions resolve to themselves. Returns (nil, nil) when the reference is
// dangling, i.e. the backing session row no longer exists.
func (s *Server) artifactOwner(sess *Session) (*Session, error) {
	cur := sess
	for depth := 0; cur.ArtifactSource.Valid; depth++ {
		if depth >= 16 {
			return nil, fmt.Errorf("artifact_source chain too deep at session %s", cur.ID)
		}
		next, err := s.getSession(cur.ArtifactSource.String)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, nil
		}
		cur = next
	}
	return cur, nil
}

// findReusableArtifact looks for a completed session whose published artifact
// can back a new delivery of identical content: the whole-file digest and the
// total byte count must match, and the backing artifact file must still exist
// on the volume with exactly the declared length. Candidates that fail the
// on-disk check (missing or length-abnormal file) are skipped so the caller
// falls back to a normal upload session. Returns the session that owns the
// backing artifact file, or nil when no candidate is usable.
func (s *Server) findReusableArtifact(fileSHA string, totalBytes int64) (*Session, error) {
	rows, err := s.db.Query(
		`SELECT `+sessionColumns+`
		 FROM sessions
		 WHERE status = ? AND file_sha256 = ? AND total_bytes = ?
		 ORDER BY created_at, id`,
		StatusCompleted, fileSHA, totalBytes)
	if err != nil {
		return nil, err
	}
	// Materialize the candidates and close the cursor before the per-candidate
	// checks: artifactOwner issues its own queries and the pool allows a
	// single connection, so checking while iterating would deadlock.
	var cands []Session
	for rows.Next() {
		cand, err := scanSession(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		cands = append(cands, *cand)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i := range cands {
		owner, err := s.artifactOwner(&cands[i])
		if err != nil {
			return nil, err
		}
		if owner == nil {
			continue
		}
		st, err := os.Stat(s.artifactPath(owner))
		if err != nil || st.Size() != totalBytes {
			continue
		}
		return owner, nil
	}
	return nil, nil
}

// progressVersion reads the current SSE progress sequence of a session.
func (s *Server) progressVersion(id string) (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT progress_version FROM sessions WHERE id = ?`, id).Scan(&v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// confirmChunkTx records a freshly confirmed chunk and bumps the SSE progress
// sequence in one transaction, so subscribers either observe both the new
// chunk row and the new sequence number or neither. It returns the new
// sequence.
func (s *Server) confirmChunkTx(sessionID string, index int64, sha256Hex string, size int64) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO chunks (session_id, chunk_index, sha256, size) VALUES (?, ?, ?, ?)`,
		sessionID, index, sha256Hex, size); err != nil {
		return 0, err
	}
	var version int64
	if err := tx.QueryRow(
		`UPDATE sessions
		 SET progress_version = progress_version + 1,
		     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE id = ?
		 RETURNING progress_version`, sessionID).Scan(&version); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return version, nil
}

// failSessionTx freezes the session as failed with the given reason and bumps
// the SSE progress sequence in the same transaction (conflict freeze and
// assembly failure). Returns the new sequence.
func (s *Server) failSessionTx(id, reason string) (int64, error) {
	var version int64
	err := s.db.QueryRow(
		`UPDATE sessions
		 SET status = ?, error = ?,
		     progress_version = progress_version + 1,
		     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE id = ? AND status = ?
		 RETURNING progress_version`,
		StatusFailed, reason, id, StatusUploading).Scan(&version)
	if err == sql.ErrNoRows {
		// Already terminal: its state and sequence are unchanged.
		if existing, gerr := s.getSession(id); gerr == nil && existing != nil {
			return existing.ProgressVersion, nil
		}
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	return version, nil
}

// completeSessionTx publishes the assembly outcome: it marks the session
// completed with its final digest and bumps the SSE progress sequence in the
// same transaction. Returns the new sequence.
func (s *Server) completeSessionTx(id, finalSHA string) (int64, error) {
	var version int64
	err := s.db.QueryRow(
		`UPDATE sessions
		 SET status = ?, final_sha256 = ?,
		     progress_version = progress_version + 1,
		     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE id = ? AND status = ?
		 RETURNING progress_version`,
		StatusCompleted, finalSHA, id, StatusUploading).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

// ---- filesystem layout on the data volume ----

func (s *Server) chunkDir(sessionID string) string {
	return filepath.Join(s.dataDir, "chunks", sessionID)
}

func (s *Server) chunkPath(sessionID string, index int64) string {
	return filepath.Join(s.chunkDir(sessionID), fmt.Sprintf("%08d.chunk", index))
}

func (s *Server) artifactDir() string {
	return filepath.Join(s.dataDir, "artifacts")
}

func (s *Server) artifactPath(sess *Session) string {
	return filepath.Join(s.artifactDir(), sess.ID+"-"+sanitizeFilename(sess.Filename))
}
