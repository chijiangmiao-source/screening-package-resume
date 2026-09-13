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
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS sessions (
    id            TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    total_bytes   INTEGER NOT NULL,
    chunk_count   INTEGER NOT NULL,
    file_sha256   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'uploading',
    final_sha256  TEXT,
    error         TEXT,
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
);`)
	return err
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
		`SELECT id, filename, total_bytes, chunk_count, file_sha256, status, final_sha256, error
		 FROM sessions WHERE id = ?`, id).
		Scan(&sess.ID, &sess.Filename, &sess.TotalBytes, &sess.ChunkCount,
			&sess.FileSHA256, &sess.Status, &sess.FinalSHA256, &sess.Error)
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

func (s *Server) insertChunk(sessionID string, index int64, sha256 string, size int64) error {
	_, err := s.db.Exec(
		`INSERT INTO chunks (session_id, chunk_index, sha256, size) VALUES (?, ?, ?, ?)`,
		sessionID, index, sha256, size)
	return err
}

func (s *Server) createSession(sess *Session) error {
	_, err := s.db.Exec(
		`INSERT INTO sessions (id, filename, total_bytes, chunk_count, file_sha256, status)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		sess.ID, sess.Filename, sess.TotalBytes, sess.ChunkCount, sess.FileSHA256, StatusUploading)
	return err
}

func (s *Server) failSession(id, reason string) error {
	_, err := s.db.Exec(
		`UPDATE sessions SET status = ?, error = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE id = ? AND status = ?`,
		StatusFailed, reason, id, StatusUploading)
	return err
}

func (s *Server) completeSession(id, finalSHA string) error {
	_, err := s.db.Exec(
		`UPDATE sessions SET status = ?, final_sha256 = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE id = ? AND status = ?`,
		StatusCompleted, finalSHA, id, StatusUploading)
	return err
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
