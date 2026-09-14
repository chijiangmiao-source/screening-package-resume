package main

import (
	"database/sql"
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
	var sess Session
	err := s.db.QueryRow(
		`SELECT id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256, error, artifact_source, progress_version
		 FROM sessions WHERE id = ?`, id).
		Scan(&sess.ID, &sess.Filename, &sess.TotalBytes, &sess.ChunkCount,
			&sess.FileSHA256, &sess.Status, &sess.FinalSHA256, &sess.Error,
			&sess.ArtifactSource, &sess.ProgressVersion)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sess, nil
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

func (s *Server) createSession(sess *Session) error {
	_, err := s.db.Exec(
		`INSERT INTO sessions (id, filename, total_bytes, chunk_count, file_sha256, status, progress_version)
		 VALUES (?, ?, ?, ?, ?, ?, 1)`,
		sess.ID, sess.Filename, sess.TotalBytes, sess.ChunkCount, sess.FileSHA256, StatusUploading)
	return err
}

// createReusedSession persists a session that is completed at creation time:
// it owns no chunks and points at the artifact published by sourceID. Its
// progress sequence also starts at 1: the snapshot delivered to a brand-new
// subscription is the very first event, not an update.
func (s *Server) createReusedSession(sess *Session, sourceID string) error {
	_, err := s.db.Exec(
		`INSERT INTO sessions (id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256, artifact_source, progress_version)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)`,
		sess.ID, sess.Filename, sess.TotalBytes, sess.ChunkCount, sess.FileSHA256,
		StatusCompleted, sess.FileSHA256, sourceID)
	return err
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
		`SELECT id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256, error, artifact_source, progress_version
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
		var cand Session
		if err := rows.Scan(&cand.ID, &cand.Filename, &cand.TotalBytes, &cand.ChunkCount,
			&cand.FileSHA256, &cand.Status, &cand.FinalSHA256, &cand.Error, &cand.ArtifactSource,
			&cand.ProgressVersion); err != nil {
			rows.Close()
			return nil, err
		}
		cands = append(cands, cand)
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
