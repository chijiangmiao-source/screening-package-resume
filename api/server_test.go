package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type testEnv struct {
	t       *testing.T
	srv     *httptest.Server
	dataDir string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dataDir := t.TempDir()
	db, err := openDB(dataDir)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	s, err := NewServer(db, dataDir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		db.Close()
	})
	return &testEnv{t: t, srv: srv, dataDir: dataDir}
}

func randomFile(t *testing.T, size int64) ([]byte, string) {
	t.Helper()
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf)
	return buf, hex.EncodeToString(sum[:])
}

func chunkOf(t *testing.T, file []byte, total int64, idx int64) []byte {
	t.Helper()
	start := idx * ChunkSize
	end := start + ExpectedChunkLen(total, (total+ChunkSize-1)/ChunkSize, idx)
	return file[start:end]
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (e *testEnv) createSession(name string, total int64, fileSHA string) sessionJSON {
	e.t.Helper()
	count := (total + ChunkSize - 1) / ChunkSize
	body, _ := json.Marshal(map[string]any{
		"filename": name, "total_bytes": total, "chunk_count": count, "file_sha256": fileSHA,
	})
	resp, err := http.Post(e.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("create session: status %d", resp.StatusCode)
	}
	var out sessionJSON
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *testEnv) getSession(id string) sessionJSON {
	e.t.Helper()
	resp, err := http.Get(e.srv.URL + "/api/sessions/" + id)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("get session: status %d", resp.StatusCode)
	}
	var out sessionJSON
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// uploadChunk returns (HTTP status, duplicate flag, parsed error message).
func (e *testEnv) uploadChunk(id string, idx int64, data []byte) (int, bool, string) {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/sessions/%s/chunks/%d", e.srv.URL, id, idx),
		bytes.NewReader(data))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("X-Chunk-SHA256", sha256Hex(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var parsed struct {
		Duplicate bool   `json:"duplicate"`
		Error     string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed.Duplicate, parsed.Error
}

func (e *testEnv) assemble(id string) (int, sessionJSON) {
	e.t.Helper()
	resp, err := http.Post(e.srv.URL+"/api/sessions/"+id+"/assemble", "", nil)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out sessionJSON
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *testEnv) artifactExists(id string) bool {
	entries, err := os.ReadDir(filepath.Join(e.dataDir, "artifacts"))
	if err != nil {
		return false
	}
	for _, en := range entries {
		if len(en.Name()) > len(id) && en.Name()[:len(id)] == id {
			return true
		}
	}
	return false
}

func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 中断：只传一部分分块后查询缺失序号，随后仅补传缺块并完成组装。
func TestInterruptAndResume(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 12345 // 3 chunks, short last chunk
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("film-dcp.mov", total, fileSHA)

	// Upload only chunk 0, then "connection drops".
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}

	st := env.getSession(sess.SessionID)
	if !equalInts(st.MissingChunks, []int64{1, 2}) {
		t.Fatalf("missing = %v, want [1 2]", st.MissingChunks)
	}
	if st.ConfirmedBytes != ChunkSize {
		t.Fatalf("confirmed = %d, want %d", st.ConfirmedBytes, ChunkSize)
	}

	// Resume: upload only the missing chunks.
	for _, idx := range st.MissingChunks {
		if code, _, _ := env.uploadChunk(sess.SessionID, idx, chunkOf(t, file, total, idx)); code != http.StatusCreated {
			t.Fatalf("chunk %d: got %d", idx, code)
		}
	}

	code, done := env.assemble(sess.SessionID)
	if code != http.StatusOK {
		t.Fatalf("assemble: got %d", code)
	}
	if done.Status != StatusCompleted || done.FinalSHA256 == nil || *done.FinalSHA256 != fileSHA {
		t.Fatalf("unexpected completion: %+v", done)
	}
	if !env.artifactExists(sess.SessionID) {
		t.Fatal("artifact not published")
	}
	// Artifact bytes must equal the original file.
	entries, _ := os.ReadDir(filepath.Join(env.dataDir, "artifacts"))
	got, err := os.ReadFile(filepath.Join(env.dataDir, "artifacts", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, file) {
		t.Fatal("artifact content mismatch")
	}
}

// 重复：同序号同摘要重复提交返回成功且不重复计数。
func TestDuplicateChunkIdempotent(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("dup.mov", total, fileSHA)

	code, dup, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0))
	if code != http.StatusCreated || dup {
		t.Fatalf("first upload: code=%d dup=%v", code, dup)
	}
	for i := 0; i < 3; i++ {
		code, dup, _ = env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0))
		if code != http.StatusOK || !dup {
			t.Fatalf("retry %d: code=%d dup=%v", i, code, dup)
		}
	}
	st := env.getSession(sess.SessionID)
	if st.ReceivedCount != 1 || st.ConfirmedBytes != ChunkSize {
		t.Fatalf("count=%d bytes=%d, want 1/%d", st.ReceivedCount, st.ConfirmedBytes, ChunkSize)
	}
}

// 冲突：同序号不同内容立即冻结会话为 failed。
func TestConflictingChunkFailsSession(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("conflict.mov", total, fileSHA)

	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	evil := make([]byte, ChunkSize)
	rand.Read(evil)
	code, _, msg := env.uploadChunk(sess.SessionID, 0, evil)
	if code != http.StatusConflict {
		t.Fatalf("conflict: got %d (%s)", code, msg)
	}
	st := env.getSession(sess.SessionID)
	if st.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	// Frozen: further uploads and assembly are rejected.
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusConflict {
		t.Fatalf("upload after failed: got %d", code)
	}
	if code, _ := env.assemble(sess.SessionID); code != http.StatusConflict {
		t.Fatalf("assemble after failed: got %d", code)
	}
	if env.artifactExists(sess.SessionID) {
		t.Fatal("failed session must not publish an artifact")
	}
}

// 摘要不符：组装后整文件摘要与申报不符则失败且成品不可见。
func TestAssembleDigestMismatchFails(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 100
	file, _ := randomFile(t, total)
	_, wrongSHA := randomFile(t, 32)
	sess := env.createSession("tampered.mov", total, wrongSHA)

	count := (total + ChunkSize - 1) / ChunkSize
	for i := int64(0); i < count; i++ {
		if code, _, _ := env.uploadChunk(sess.SessionID, i, chunkOf(t, file, total, i)); code != http.StatusCreated {
			t.Fatalf("chunk %d: got %d", i, code)
		}
	}
	code, st := env.assemble(sess.SessionID)
	if code != http.StatusConflict {
		t.Fatalf("assemble: got %d", code)
	}
	if st.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	if env.artifactExists(sess.SessionID) {
		t.Fatal("mismatched artifact must not be visible")
	}
}

// 分块长度校验：除最后一块外必须恰为 1 MiB，最后一块长度由总字节数唯一确定。
func TestChunkLengthValidation(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 500
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("len.mov", total, fileSHA)

	// Non-last chunk with wrong length.
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, file[:ChunkSize-1]); code != http.StatusBadRequest {
		t.Fatalf("short non-last chunk: got %d", code)
	}
	// Last chunk must be exactly 500 bytes.
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, file[ChunkSize:ChunkSize+499]); code != http.StatusBadRequest {
		t.Fatalf("short last chunk: got %d", code)
	}
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, file[ChunkSize:]); code != http.StatusCreated {
		t.Fatalf("exact last chunk: got %d", code)
	}
	// Declared header digest that does not match content.
	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/sessions/%s/chunks/0", env.srv.URL, sess.SessionID),
		bytes.NewReader(chunkOf(t, file, total, 0)))
	req.Header.Set("X-Chunk-SHA256", sha256Hex([]byte("not the content")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("lying digest header: got %d", resp.StatusCode)
	}
}

// 会话校验：chunk_count 与 total_bytes 不一致、非小写十六进制摘要均被拒绝。
func TestSessionValidation(t *testing.T) {
	env := newTestEnv(t)
	post := func(payload map[string]any) int {
		body, _ := json.Marshal(payload)
		resp, err := http.Post(env.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	sha := sha256Hex([]byte("x"))
	if c := post(map[string]any{"filename": "a", "total_bytes": 1048577, "chunk_count": 1, "file_sha256": sha}); c != http.StatusBadRequest {
		t.Fatalf("bad chunk_count: got %d", c)
	}
	if c := post(map[string]any{"filename": "a", "total_bytes": 10, "chunk_count": 1, "file_sha256": "ABCDEF"}); c != http.StatusBadRequest {
		t.Fatalf("bad sha: got %d", c)
	}
	if c := post(map[string]any{"filename": "a", "total_bytes": 0, "chunk_count": 0, "file_sha256": sha}); c != http.StatusBadRequest {
		t.Fatalf("zero total: got %d", c)
	}
}

// 重开：进程重启（新 Server 复用同一数据卷）后会话与缺块列表仍在。
func TestStateSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	total := 2*ChunkSize + 1
	file, fileSHA := randomFile(t, total)

	db1, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := NewServer(db1, dataDir)
	srv1 := httptest.NewServer(s1.Handler())

	count := (total + ChunkSize - 1) / ChunkSize
	body, _ := json.Marshal(map[string]any{
		"filename": "restart.mov", "total_bytes": total, "chunk_count": count, "file_sha256": fileSHA,
	})
	resp, err := http.Post(srv1.URL+"/api/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var sess sessionJSON
	json.NewDecoder(resp.Body).Decode(&sess)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/sessions/%s/chunks/0", srv1.URL, sess.SessionID),
		bytes.NewReader(chunkOf(t, file, total, 0)))
	req.Header.Set("X-Chunk-SHA256", sha256Hex(chunkOf(t, file, total, 0)))
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	srv1.Close()
	db1.Close()

	// "Restart": new server over the same data volume.
	db2, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	s2, _ := NewServer(db2, dataDir)
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()

	resp, err = http.Get(srv2.URL + "/api/sessions/" + sess.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st sessionJSON
	json.NewDecoder(resp.Body).Decode(&st)
	if !equalInts(st.MissingChunks, []int64{1, 2}) {
		t.Fatalf("after restart missing = %v, want [1 2]", st.MissingChunks)
	}
	if st.ConfirmedBytes != ChunkSize {
		t.Fatalf("after restart confirmed = %d", st.ConfirmedBytes)
	}
}
