package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// download issues a GET (optionally with a Range header) against the
// artifact endpoint and returns status, headers, and the full body.
func (e *testEnv) download(id, rangeHeader string) (int, http.Header, []byte) {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodGet, e.srv.URL+"/api/sessions/"+id+"/download", nil)
	if err != nil {
		e.t.Fatal(err)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, body
}

// publish runs a full upload+assemble flow and returns the completed session.
func (e *testEnv) publish(name string, file []byte, fileSHA string) sessionJSON {
	e.t.Helper()
	total := int64(len(file))
	sess := e.createSession(name, total, fileSHA)
	count := (total + ChunkSize - 1) / ChunkSize
	for i := int64(0); i < count; i++ {
		if code, _, _ := e.uploadChunk(sess.SessionID, i, chunkOf(e.t, file, total, i)); code != http.StatusCreated {
			e.t.Fatalf("chunk %d: got %d", i, code)
		}
	}
	code, done := e.assemble(sess.SessionID)
	if code != http.StatusOK || done.Status != StatusCompleted {
		e.t.Fatalf("assemble: code=%d status=%s", code, done.Status)
	}
	return done
}

// 完整下载：200、字节级一致、文件名与 Accept-Ranges 头齐全。
func TestDownloadFullArtifact(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 777
	file, fileSHA := randomFile(t, total)
	sess := env.publish("首映-正片.mov", file, fileSHA)

	code, hdr, body := env.download(sess.SessionID, "")
	if code != http.StatusOK {
		t.Fatalf("full download: got %d", code)
	}
	if !bytes.Equal(body, file) {
		t.Fatalf("full download body mismatch: got %d bytes, want %d", len(body), len(file))
	}
	if got := hdr.Get("Content-Length"); got != fmt.Sprintf("%d", total) {
		t.Fatalf("Content-Length = %s, want %d", got, total)
	}
	if got := hdr.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", got)
	}
	if cd := hdr.Get("Content-Disposition"); !strings.Contains(cd, "attachment") ||
		!strings.Contains(cd, "filename*=") {
		t.Fatalf("Content-Disposition missing attachment/filename: %q", cd)
	}
	// 旧会话查询仍可使用。
	st := env.getSession(sess.SessionID)
	if st.Status != StatusCompleted {
		t.Fatalf("session query after download: status = %s", st.Status)
	}
}

// 断点续传：按偏移分段拉取，拼接后与原文件字节级一致。
func TestDownloadRangeResume(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 12345
	file, fileSHA := randomFile(t, total)
	sess := env.publish("resume-reel.mov", file, fileSHA)

	// 第一段：bytes=0-1048575（恰为第一个 1 MiB 分块）。
	code, hdr, part1 := env.download(sess.SessionID, "bytes=0-1048575")
	if code != http.StatusPartialContent {
		t.Fatalf("range part1: got %d", code)
	}
	if got, want := hdr.Get("Content-Range"), fmt.Sprintf("bytes 0-%d/%d", ChunkSize-1, total); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if got := hdr.Get("Content-Length"); got != fmt.Sprintf("%d", ChunkSize) {
		t.Fatalf("Content-Length = %q, want %d", got, ChunkSize)
	}
	if got := hdr.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", got)
	}
	if !bytes.Equal(part1, file[:ChunkSize]) {
		t.Fatal("range part1 bytes mismatch")
	}

	// 中断后从已接收位置继续：bytes=1048576- 直到文件末尾。
	code, hdr, part2 := env.download(sess.SessionID, fmt.Sprintf("bytes=%d-", ChunkSize))
	if code != http.StatusPartialContent {
		t.Fatalf("range part2: got %d", code)
	}
	if got, want := hdr.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", ChunkSize, total-1, total); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(part2, file[ChunkSize:]) {
		t.Fatal("range part2 bytes mismatch")
	}

	// 拼接结果必须与原始文件一致。
	if joined := append(append([]byte{}, part1...), part2...); !bytes.Equal(joined, file) {
		t.Fatal("resumed download reassembly mismatch")
	}

	// 后缀范围：最后 100 字节。
	code, hdr, tail := env.download(sess.SessionID, "bytes=-100")
	if code != http.StatusPartialContent {
		t.Fatalf("suffix range: got %d", code)
	}
	if got, want := hdr.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", total-100, total-1, total); got != want {
		t.Fatalf("suffix Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(tail, file[total-100:]) {
		t.Fatal("suffix range bytes mismatch")
	}

	// 结束位置越界时收敛到文件末尾（合法单段，仍 206）。
	code, hdr, clamped := env.download(sess.SessionID, fmt.Sprintf("bytes=%d-%d", total-10, total+999))
	if code != http.StatusPartialContent {
		t.Fatalf("clamped range: got %d", code)
	}
	if got, want := hdr.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", total-10, total-1, total); got != want {
		t.Fatalf("clamped Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(clamped, file[total-10:]) {
		t.Fatal("clamped range bytes mismatch")
	}
}

// 非法范围：越界、多段、倒置、格式错误一律 416 且无正文。
func TestDownloadInvalidRanges(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 500
	file, fileSHA := randomFile(t, total)
	sess := env.publish("invalid-ranges.mov", file, fileSHA)

	cases := []string{
		fmt.Sprintf("bytes=%d-", total),              // 起点越界
		fmt.Sprintf("bytes=%d-%d", total, total+100), // 整段越界
		"bytes=0-1,3-4",                              // 多段范围不支持
		"bytes=500-499",                              // 起点大于终点
		"bytes=-0",                                   // 后缀长度为零
		"bytes=abc-def",                              // 非数字
		"items=0-10",                                 // 非 bytes 单位
		"bytes=",                                     // 空范围
	}
	for _, tc := range cases {
		code, hdr, body := env.download(sess.SessionID, tc)
		if code != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("range %q: got %d, want 416", tc, code)
		}
		if got, want := hdr.Get("Content-Range"), fmt.Sprintf("bytes */%d", total); got != want {
			t.Fatalf("range %q: Content-Range = %q, want %q", tc, got, want)
		}
		if len(body) != 0 {
			t.Fatalf("range %q: 416 must have no body, got %d bytes", tc, len(body))
		}
	}
}

// 未就绪：上传中与已失败会话请求下载返回 409。
func TestDownloadSessionNotReady(t *testing.T) {
	env := newTestEnv(t)

	// 上传中：只传了部分分块。
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("partial.mov", total, fileSHA)
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	code, _, body := env.download(sess.SessionID, "")
	if code != http.StatusConflict {
		t.Fatalf("uploading session download: got %d", code)
	}
	if !json.Valid(body) {
		t.Fatal("409 response should carry a JSON error")
	}

	// 已失败：冲突冻结后请求下载。
	evil := make([]byte, ChunkSize)
	rand.Read(evil)
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, evil); code != http.StatusConflict {
		t.Fatalf("conflict upload: got %d", code)
	}
	if st := env.getSession(sess.SessionID); st.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	code, _, _ = env.download(sess.SessionID, "")
	if code != http.StatusConflict {
		t.Fatalf("failed session download: got %d", code)
	}
	// Range 请求同样被拒绝。
	code, _, _ = env.download(sess.SessionID, "bytes=0-")
	if code != http.StatusConflict {
		t.Fatalf("failed session ranged download: got %d", code)
	}
}

// 成品缺失：数据库已完成但 artifacts 中文件被移除时返回 410。
func TestDownloadArtifactMissing(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 1
	file, fileSHA := randomFile(t, total)
	sess := env.publish("lost-artifact.mov", file, fileSHA)

	artPath := filepath.Join(env.dataDir, "artifacts",
		sess.SessionID+"-"+sanitizeFilename("lost-artifact.mov"))
	if err := os.Remove(artPath); err != nil {
		t.Fatalf("remove artifact: %v", err)
	}
	code, _, body := env.download(sess.SessionID, "")
	if code != http.StatusGone {
		t.Fatalf("missing artifact: got %d, want 410", code)
	}
	if !json.Valid(body) {
		t.Fatal("410 response should carry a JSON error")
	}
	// 会话查询仍是 completed，仅下载入口报告成品缺失。
	if st := env.getSession(sess.SessionID); st.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", st.Status)
	}
}

// 未知会话：下载与查询一样返回 404。
func TestDownloadUnknownSession(t *testing.T) {
	env := newTestEnv(t)
	code, _, _ := env.download("00000000000000000000000000000000", "")
	if code != http.StatusNotFound {
		t.Fatalf("unknown session download: got %d", code)
	}
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
