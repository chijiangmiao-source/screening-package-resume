package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// createTokenPayload is one create-session request body in these tests.
type createTokenPayload struct {
	Filename      string `json:"filename"`
	TotalBytes    int64  `json:"total_bytes"`
	ChunkCount    int64  `json:"chunk_count"`
	FileSHA256    string `json:"file_sha256"`
	ReuseArtifact bool   `json:"reuse_artifact,omitempty"`
	CreateToken   string `json:"create_token,omitempty"`
}

func tokenPayload(name string, total int64, fileSHA, token string) createTokenPayload {
	return createTokenPayload{
		Filename:    name,
		TotalBytes:  total,
		ChunkCount:  (total + ChunkSize - 1) / ChunkSize,
		FileSHA256:  fileSHA,
		CreateToken: token,
	}
}

// postCreate sends an arbitrary create payload and returns status, parsed
// body, and the total number of session rows afterwards.
func (e *testEnv) postCreate(p createTokenPayload) (int, sessionJSON, int) {
	e.t.Helper()
	body, _ := json.Marshal(p)
	resp, err := http.Post(e.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out sessionJSON
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &out)
	var n int
	if err := e.server.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return resp.StatusCode, out, n
}

func sessionCount(t *testing.T, env *testEnv) int {
	t.Helper()
	var n int
	if err := env.server.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func chunkDirCount(t *testing.T, env *testEnv) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(env.dataDir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// 幂等创建：同一令牌搭配完全相同的元数据重试，返回同一个会话，不重复
// 建记录、不重复建分块目录；第二次回答的是会话当前快照（200）。
func TestCreateTokenIdempotentRetry(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 777
	file, fileSHA := randomFile(t, total)
	token := "0123456789abcdef0123456789abcdef"

	p := tokenPayload("首映-正片.mov", total, fileSHA, token)
	code, first, n := env.postCreate(p)
	if code != http.StatusCreated || n != 1 {
		t.Fatalf("first create: code=%d rows=%d, want 201/1", code, n)
	}
	if first.Status != StatusUploading {
		t.Fatalf("first status = %s, want uploading", first.Status)
	}
	if chunkDirCount(t, env) != 1 {
		t.Fatal("first create must make exactly one chunk directory")
	}

	// Simulate the other workstation (or a previous attempt whose response
	// was lost) confirming chunk 0 before the retry arrives.
	if code, _, _ := env.uploadChunk(first.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}

	// "Response lost": the client retries with the same token and the same
	// computed metadata — no re-hash, no new token.
	code, retry, n := env.postCreate(p)
	if code != http.StatusOK {
		t.Fatalf("retry create: got %d, want 200", code)
	}
	if retry.SessionID != first.SessionID {
		t.Fatalf("retry created a different session: %s != %s", retry.SessionID, first.SessionID)
	}
	if n != 1 {
		t.Fatalf("retry persisted %d sessions, want 1", n)
	}
	if chunkDirCount(t, env) != 1 {
		t.Fatal("idempotent retry must not create a second chunk directory")
	}
	// The replay is the CURRENT snapshot: chunk 0 is already confirmed.
	if retry.ReceivedCount != 1 || retry.ConfirmedBytes != ChunkSize {
		t.Fatalf("retry snapshot is stale: count=%d bytes=%d, want 1/%d",
			retry.ReceivedCount, retry.ConfirmedBytes, ChunkSize)
	}
	if !equalInts(retry.MissingChunks, []int64{1, 2}) {
		t.Fatalf("retry missing = %v, want [1 2]", retry.MissingChunks)
	}

	// The replayed session is a fully functional upload session.
	if code, _, _ := env.uploadChunk(retry.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("chunk 1: got %d", code)
	}
	if code, _, _ := env.uploadChunk(retry.SessionID, 2, chunkOf(t, file, total, 2)); code != http.StatusCreated {
		t.Fatalf("chunk 2: got %d", code)
	}
	if code, done := env.assemble(retry.SessionID); code != http.StatusOK || done.Status != StatusCompleted {
		t.Fatalf("assemble after replay: code=%d status=%s", code, done.Status)
	}
	code, _, dlBody := env.download(retry.SessionID, "")
	if code != http.StatusOK || !bytes.Equal(dlBody, file) {
		t.Fatalf("download after replay: code=%d bytes=%d", code, len(dlBody))
	}
}

// 复用命中也幂等：首次创建即以 completed 命中成品，响应丢失后的重试返回
// 同一个已完成会话（相同标识、相同来源、相同复用结果），库内只有一条记录。
func TestCreateTokenIdempotentReuseHit(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 777
	file, fileSHA := randomFile(t, total)
	src := env.publish("原盘-正片.mov", file, fileSHA)
	token := "abcdef0123456789abcdef0123456789"

	p := tokenPayload("重映-正片.mov", total, fileSHA, token)
	p.ReuseArtifact = true
	code, first, n := env.postCreate(p)
	if code != http.StatusCreated || first.Status != StatusCompleted {
		t.Fatalf("first reuse create: code=%d status=%s", code, first.Status)
	}
	if first.ArtifactSource == nil || *first.ArtifactSource != src.SessionID {
		t.Fatalf("artifact_source = %v, want %s", first.ArtifactSource, src.SessionID)
	}

	// Response lost -> retry verbatim.
	code, retry, n := env.postCreate(p)
	if code != http.StatusOK {
		t.Fatalf("retry reuse create: got %d, want 200", code)
	}
	if retry.SessionID != first.SessionID {
		t.Fatalf("reuse replay id = %s, want %s", retry.SessionID, first.SessionID)
	}
	if retry.Status != StatusCompleted {
		t.Fatalf("reuse replay status = %s, want completed", retry.Status)
	}
	if retry.ArtifactSource == nil || *retry.ArtifactSource != src.SessionID {
		t.Fatalf("reuse replay source = %v, want %s", retry.ArtifactSource, src.SessionID)
	}
	if retry.ReceivedCount != 0 || retry.ConfirmedBytes != 0 || len(retry.MissingChunks) != 0 {
		t.Fatalf("reuse replay must own no chunks: %+v", retry)
	}
	if n != 2 { // the published source + the one reused session, nothing else
		t.Fatalf("rows after reuse retry = %d, want 2", n)
	}
	// No chunk directory exists for a zero-chunk reuse session, on either attempt.
	if _, err := os.Stat(filepath.Join(env.dataDir, "chunks", first.SessionID)); !os.IsNotExist(err) {
		t.Fatalf("reuse session must own no chunk directory, stat err=%v", err)
	}
	// Download through the (same) new session id still serves the reused bytes.
	code, _, body := env.download(retry.SessionID, "")
	if code != http.StatusOK || !bytes.Equal(body, file) {
		t.Fatalf("reuse replay download: code=%d match=%v", code, bytes.Equal(body, file))
	}
}

// 冲突：同一令牌搭配不同文件名、字节数、摘要或复用意愿时返回 409，
// 原有会话一行都不被改动，且库内不增加记录。
func TestCreateTokenConflictMetadata(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 42
	file, fileSHA := randomFile(t, total)
	_, otherSHA := randomFile(t, total)
	token := "11112222333344445555666677778888"

	base := tokenPayload("same.mov", total, fileSHA, token)
	code, first, n := env.postCreate(base)
	if code != http.StatusCreated || n != 1 {
		t.Fatalf("baseline create: code=%d rows=%d", code, n)
	}

	cases := map[string]func(createTokenPayload) createTokenPayload{
		"different filename": func(p createTokenPayload) createTokenPayload { p.Filename = "renamed.mov"; return p },
		"different bytes": func(p createTokenPayload) createTokenPayload {
			return tokenPayload("same.mov", total+1, otherSHA256(t, total+1), token)
		},
		"different chunk count": func(p createTokenPayload) createTokenPayload {
			// Same total bytes and digest, but an incompatible chunk_count.
			// For a KNOWN token this is a token conflict (409), not the
			// usual chunk_count/total_bytes consistency error (400).
			p.ChunkCount = base.ChunkCount + 1
			return p
		},
		"different digest":  func(p createTokenPayload) createTokenPayload { p.FileSHA256 = otherSHA; return p },
		"reuse willingness": func(p createTokenPayload) createTokenPayload { p.ReuseArtifact = true; return p },
	}
	for name, mutate := range cases {
		p := mutate(base)
		code, conflict, n2 := env.postCreate(p)
		if code != http.StatusConflict {
			t.Fatalf("%s: got %d, want 409", name, code)
		}
		if conflict.SessionID == first.SessionID && conflict.Status != "" {
			t.Fatalf("%s: conflict response must not masquerade as the original session", name)
		}
		if n2 != 1 {
			t.Fatalf("%s: rows changed to %d, conflict must not insert", name, n2)
		}
	}

	// The original session is byte-for-byte untouched and still works.
	st := env.getSession(first.SessionID)
	if st.Status != StatusUploading || st.Filename != "same.mov" ||
		st.TotalBytes != total || st.FileSHA256 != fileSHA || st.ArtifactSource != nil {
		t.Fatalf("original session altered by conflicts: %+v", st)
	}
	if code, _, _ := env.uploadChunk(first.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("original chunk 0 after conflicts: got %d", code)
	}

	// A brand-NEW token still goes through strict validation: an
	// inconsistent chunk_count is 400 there, never a token conflict.
	fresh := tokenPayload("same.mov", total, fileSHA, "22223333444455556666777788889999")
	fresh.ChunkCount = base.ChunkCount + 1
	if code, _, n := env.postCreate(fresh); code != http.StatusBadRequest || n != 1 {
		t.Fatalf("unknown token with bad chunk_count: code=%d rows=%d, want 400/1", code, n)
	}
}

// helper: digest of a fresh random file of a given size.
func otherSHA256(t *testing.T, size int64) string {
	t.Helper()
	_, sha := randomFile(t, size)
	return sha
}

// 复用意愿冲突的另一侧：首次带 reuse_artifact=true 命中并完成，同令牌改带
// reuse_artifact=false 必须 409，已完成会话不受影响、仍可下载。
func TestCreateTokenConflictReuseToggleBothWays(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	src := env.publish("seed.mov", file, fileSHA)
	token := "aaaabbbbccccddddeeeeffff00001111"

	hit := tokenPayload("again.mov", total, fileSHA, token)
	hit.ReuseArtifact = true
	code, first, n := env.postCreate(hit)
	if code != http.StatusCreated || first.Status != StatusCompleted || n != 2 {
		t.Fatalf("reuse-hit create: code=%d status=%s rows=%d", code, first.Status, n)
	}
	if first.ArtifactSource == nil || *first.ArtifactSource != src.SessionID {
		t.Fatalf("artifact_source = %v, want %s", first.ArtifactSource, src.SessionID)
	}

	// Same token, now opting OUT of reuse: conflict, not a fresh upload session.
	declined := hit
	declined.ReuseArtifact = false
	code, _, n = env.postCreate(declined)
	if code != http.StatusConflict || n != 2 {
		t.Fatalf("reuse toggle: code=%d rows=%d, want 409/2", code, n)
	}
	st := env.getSession(first.SessionID)
	if st.Status != StatusCompleted || st.ArtifactSource == nil || *st.ArtifactSource != src.SessionID {
		t.Fatalf("completed reuse session altered by conflict: %+v", st)
	}
	code, _, body := env.download(first.SessionID, "")
	if code != http.StatusOK || !bytes.Equal(body, file) {
		t.Fatalf("reused download after conflict: code=%d match=%v", code, bytes.Equal(body, file))
	}
}

// 不带令牌的旧客户端：两次请求仍是相互独立的新会话，行为与以前一致。
func TestCreateWithoutTokenStaysIndependent(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	env.publish("already-there.mov", file, fileSHA)

	// Old-style body: neither create_token nor reuse_artifact present.
	for i := 0; i < 2; i++ {
		body, _ := json.Marshal(map[string]any{
			"filename":    fmt.Sprintf("legacy-%d.mov", i),
			"total_bytes": total,
			"chunk_count": 2,
			"file_sha256": fileSHA,
		})
		resp, err := http.Post(env.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("legacy create %d: got %d", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	if n := sessionCount(t, env); n != 3 { // published source + two independent sessions
		t.Fatalf("rows = %d, want 3 independent sessions", n)
	}
	if d := chunkDirCount(t, env); d != 3 {
		t.Fatalf("chunk dirs = %d, want 3 (one source + two legacy)", d)
	}
}

// 令牌格式：存在但不是 32 位小写十六进制时一律 400，且不落任何记录。
func TestCreateTokenMalformedRejected(t *testing.T) {
	env := newTestEnv(t)
	sha := sha256Hex([]byte("x"))
	body := func(token string) string {
		return `{"filename":"t.mov","total_bytes":10,"chunk_count":1,"file_sha256":"` + sha +
			`","create_token":"` + token + `"}`
	}
	for _, token := range []string{
		"0123456789abcdef0123456789abcde",   // 31 chars
		"0123456789abcdef0123456789abcdef0", // 33 chars
		"0123456789ABCDEF0123456789ABCDEF",  // uppercase
		"0123456789abcd-f0123456789abcde",   // non-hex
	} {
		code, n := env.rawCreate(body(token), nil)
		if code != http.StatusBadRequest {
			t.Fatalf("token %q: got %d, want 400", token, code)
		}
		if n != 0 {
			t.Fatalf("token %q: persisted %d rows, want 0", token, n)
		}
	}
	// Empty string is the same as omitting the field: old-client path.
	if code, n := env.rawCreate(body(""), nil); code != http.StatusCreated || n != 1 {
		t.Fatalf("empty token: code=%d rows=%d, want 201/1", code, n)
	}
}

// 并发：两个携带同一全新令牌的请求同时到达，只允许一条记录，且两个响应
// 指向同一个会话（一个 201、一个 200，顺序不保证）。
func TestCreateTokenConcurrentSameToken(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 5
	_, fileSHA := randomFile(t, total)
	token := "fedcba9876543210fedcba9876543210"
	p := tokenPayload("concurrent.mov", total, fileSHA, token)
	raw, _ := json.Marshal(p)

	const goroutines = 8
	var wg sync.WaitGroup
	type result struct {
		code int
		id   string
	}
	results := make([]result, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := http.Post(env.srv.URL+"/api/sessions", "application/json", bytes.NewReader(raw))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			var out sessionJSON
			_ = json.NewDecoder(resp.Body).Decode(&out)
			results[i] = result{resp.StatusCode, out.SessionID}
		}(i)
	}
	close(start)
	wg.Wait()

	if n := sessionCount(t, env); n != 1 {
		t.Fatalf("concurrent creates persisted %d rows, want 1", n)
	}
	var id string
	created, replayed := 0, 0
	for i, r := range results {
		if r.code != http.StatusCreated && r.code != http.StatusOK {
			t.Fatalf("goroutine %d: unexpected code %d", i, r.code)
		}
		if r.code == http.StatusCreated {
			created++
		} else {
			replayed++
		}
		if id == "" {
			id = r.id
		} else if r.id != id {
			t.Fatalf("goroutine %d got session %s, want %s", i, r.id, id)
		}
	}
	if created != 1 || replayed != goroutines-1 {
		t.Fatalf("expected exactly one 201 and %d replays, got %d/%d", goroutines-1, created, replayed)
	}
	if chunkDirCount(t, env) != 1 {
		t.Fatalf("concurrent creates left %d chunk dirs, want 1", chunkDirCount(t, env))
	}
}

// 重启：令牌持久化在 SQLite 中，进程重启后同令牌重试仍解析到原会话当前
// 快照（含重启前已确认的分块），随后可继续上传、组装、下载。
func TestCreateTokenSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	total := 2*ChunkSize + 99
	file, fileSHA := randomFile(t, total)
	token := "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
	p := tokenPayload("重启-正片.mov", total, fileSHA, token)
	raw, _ := json.Marshal(p)

	db1, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := NewServer(db1, dataDir)
	srv1 := httptest.NewServer(s1.Handler())
	post := func(base string) (int, sessionJSON) {
		resp, err := http.Post(base+"/api/sessions", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out sessionJSON
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, first := post(srv1.URL)
	if code != http.StatusCreated {
		t.Fatalf("first create: got %d", code)
	}
	// Confirm chunk 0 before the "crash"; the response to the create is
	// considered lost, so the client only knows the token after restart.
	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/sessions/%s/chunks/0", srv1.URL, first.SessionID),
		bytes.NewReader(chunkOf(t, file, total, 0)))
	req.Header.Set("X-Chunk-SHA256", sha256Hex(chunkOf(t, file, total, 0)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	srv1.Close()
	db1.Close()

	// "Restart": a new process over the same volume retries with the token.
	db2, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	s2, _ := NewServer(db2, dataDir)
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()

	code, retry := post(srv2.URL)
	if code != http.StatusOK {
		t.Fatalf("retry after restart: got %d, want 200", code)
	}
	if retry.SessionID != first.SessionID {
		t.Fatalf("after restart token resolved to %s, want %s", retry.SessionID, first.SessionID)
	}
	if retry.ReceivedCount != 1 || retry.ConfirmedBytes != ChunkSize ||
		!equalInts(retry.MissingChunks, []int64{1, 2}) {
		t.Fatalf("post-restart replay snapshot wrong: %+v", retry)
	}
	var n int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM sessions WHERE create_token = ?`, token).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("token rows after restart = %d, want 1", n)
	}

	// Finish the delivery through the restarted process.
	env2 := &testEnv{t: t, srv: srv2, server: s2, dataDir: dataDir}
	for _, idx := range retry.MissingChunks {
		if code, _, _ := env2.uploadChunk(retry.SessionID, idx, chunkOf(t, file, total, idx)); code != http.StatusCreated {
			t.Fatalf("chunk %d after restart: got %d", idx, code)
		}
	}
	if code, done := env2.assemble(retry.SessionID); code != http.StatusOK || done.Status != StatusCompleted {
		t.Fatalf("assemble after restart: code=%d status=%s", code, done.Status)
	}
	code, _, body := env2.download(retry.SessionID, "")
	if code != http.StatusOK || !bytes.Equal(body, file) {
		t.Fatalf("download after restart: code=%d match=%v", code, bytes.Equal(body, file))
	}
}

// 迁移：旧库（没有 create_token / reuse_requested 列）打开即自动补齐，
// 旧记录令牌为空、复用意愿为假，迁移后的库支持令牌幂等创建。
func TestMigrateAddsCreateTokenColumns(t *testing.T) {
	dataDir := t.TempDir()
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", filepath.Join(dataDir, "app.db"))
	raw, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`
CREATE TABLE sessions (
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
INSERT INTO sessions (id, filename, total_bytes, chunk_count, file_sha256, status)
VALUES ('legacy-token', 'old.mov', 10, 1, '` + sha256Hex([]byte("x")) + `', 'uploading');`)
	if err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := openDB(dataDir)
	if err != nil {
		t.Fatalf("openDB over legacy schema: %v", err)
	}
	defer db.Close()
	for _, col := range []string{"create_token", "reuse_requested"} {
		has, err := hasColumn(db, "sessions", col)
		if err != nil || !has {
			t.Fatalf("column %s missing after migration: has=%v err=%v", col, has, err)
		}
	}
	// The unique index exists and tolerates the many NULL tokens of old rows.
	var idx int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_sessions_create_token'`).Scan(&idx)
	if err != nil || idx != 1 {
		t.Fatalf("unique token index missing: idx=%d err=%v", idx, err)
	}

	s, err := NewServer(db, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := s.getSession("legacy-token")
	if err != nil || legacy == nil {
		t.Fatalf("legacy row unreadable: %v", err)
	}
	if legacy.CreateToken.Valid {
		t.Fatal("legacy row must carry NULL create_token")
	}
	if legacy.ReuseRequested {
		t.Fatal("legacy row must default reuse_requested to false")
	}

	// Token-based idempotent creation works on the migrated database.
	token := "99998888777766665555444433332222"
	intent := createIntent{
		Filename: "new.mov", TotalBytes: 10, ChunkCount: 1,
		FileSHA256: sha256Hex([]byte("y")), Token: token,
	}
	sess, replayed, err := s.createWithToken(context.Background(), intent, nil, "new-id-1")
	if err != nil || replayed || sess == nil {
		t.Fatalf("first createWithToken: sess=%v replayed=%v err=%v", sess, replayed, err)
	}
	again, replayed, err := s.createWithToken(context.Background(), intent, nil, "new-id-2")
	if err != nil || !replayed {
		t.Fatalf("replay createWithToken: replayed=%v err=%v", replayed, err)
	}
	if again.ID != "new-id-1" {
		t.Fatalf("replay resolved to %s, want the original new-id-1", again.ID)
	}
}
