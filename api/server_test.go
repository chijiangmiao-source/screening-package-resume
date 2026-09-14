package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testEnv struct {
	t       *testing.T
	srv     *httptest.Server
	server  *Server
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
	return &testEnv{t: t, srv: srv, server: s, dataDir: dataDir}
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

// createSessionRaw POSTs a create request, optionally declaring artifact
// reuse, and returns the HTTP status plus the parsed session.
func (e *testEnv) createSessionRaw(name string, total int64, fileSHA string, reuse bool) (int, sessionJSON) {
	e.t.Helper()
	count := (total + ChunkSize - 1) / ChunkSize
	body, _ := json.Marshal(map[string]any{
		"filename": name, "total_bytes": total, "chunk_count": count,
		"file_sha256": fileSHA, "reuse_artifact": reuse,
	})
	resp, err := http.Post(e.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out sessionJSON
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// 成品复用命中：同内容不同文件名的重复交付在创建时即完成，零分块上传，
// 下载读取被复用的成品而响应文件名取自本次提交。
func TestReuseArtifactHit(t *testing.T) {
	env := newTestEnv(t)
	total := 2*ChunkSize + 777
	file, fileSHA := randomFile(t, total)
	src := env.publish("原盘-正片.mov", file, fileSHA)

	code, reuse := env.createSessionRaw("重映-正片.mov", total, fileSHA, true)
	if code != http.StatusCreated {
		t.Fatalf("reuse create: got %d", code)
	}
	if reuse.Status != StatusCompleted {
		t.Fatalf("reuse status = %s, want completed", reuse.Status)
	}
	if reuse.SessionID == src.SessionID {
		t.Fatal("reuse must create a new session id")
	}
	if reuse.FinalSHA256 == nil || *reuse.FinalSHA256 != fileSHA {
		t.Fatalf("reuse final_sha256 = %v, want %s", reuse.FinalSHA256, fileSHA)
	}
	if reuse.ReceivedCount != 0 || reuse.ConfirmedBytes != 0 {
		t.Fatalf("reuse must upload zero chunks: count=%d bytes=%d", reuse.ReceivedCount, reuse.ConfirmedBytes)
	}
	if len(reuse.MissingChunks) != 0 {
		t.Fatalf("reuse missing = %v, want none", reuse.MissingChunks)
	}
	if reuse.ArtifactSource == nil || *reuse.ArtifactSource != src.SessionID {
		t.Fatalf("artifact_source = %v, want %s", reuse.ArtifactSource, src.SessionID)
	}
	if reuse.Filename != "重映-正片.mov" {
		t.Fatalf("filename = %q", reuse.Filename)
	}

	// Query and download use the NEW session id; the body is the reused
	// artifact while Content-Disposition carries this submission's filename.
	st := env.getSession(reuse.SessionID)
	if st.Status != StatusCompleted || st.ArtifactSource == nil || *st.ArtifactSource != src.SessionID {
		t.Fatalf("query by new id: %+v", st)
	}
	code, hdr, body := env.download(reuse.SessionID, "")
	if code != http.StatusOK {
		t.Fatalf("reuse download: got %d", code)
	}
	if !bytes.Equal(body, file) {
		t.Fatal("reuse download body mismatch")
	}
	cd := hdr.Get("Content-Disposition")
	if !strings.Contains(cd, "attachment") || !strings.Contains(cd, "%E9%87%8D%E6%98%A0") {
		t.Fatalf("Content-Disposition must carry the new filename: %q", cd)
	}
	// Range resume works on the reused artifact too.
	code, _, part := env.download(reuse.SessionID, fmt.Sprintf("bytes=%d-", ChunkSize))
	if code != http.StatusPartialContent || !bytes.Equal(part, file[ChunkSize:]) {
		t.Fatalf("reuse ranged download: code=%d bytes=%d", code, len(part))
	}

	// A third delivery reuses the reuse: the reference must point at the
	// ultimate backing session, keeping the chain one hop deep.
	code, third := env.createSessionRaw("三映-正片.mov", total, fileSHA, true)
	if code != http.StatusCreated || third.Status != StatusCompleted {
		t.Fatalf("third create: code=%d status=%s", code, third.Status)
	}
	if third.ArtifactSource == nil || *third.ArtifactSource != src.SessionID {
		t.Fatalf("third artifact_source = %v, want ultimate source %s", third.ArtifactSource, src.SessionID)
	}
}

// 候选损坏：成品文件长度异常或缺失时退回普通上传会话，不误报完成，
// 退回后的会话可正常走分块上传与组装。
func TestReuseFallbackWhenCandidateBroken(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 42
	file, fileSHA := randomFile(t, total)
	src := env.publish("brittle-source.mov", file, fileSHA)
	artPath := filepath.Join(env.dataDir, "artifacts",
		src.SessionID+"-"+sanitizeFilename("brittle-source.mov"))

	// Length-abnormal candidate: truncated artifact must not be reused.
	if err := os.Truncate(artPath, total/2); err != nil {
		t.Fatal(err)
	}
	code, st := env.createSessionRaw("retry-delivery.mov", total, fileSHA, true)
	if code != http.StatusCreated {
		t.Fatalf("fallback create: got %d", code)
	}
	if st.Status != StatusUploading || st.FinalSHA256 != nil || st.ArtifactSource != nil {
		t.Fatalf("length-abnormal candidate must fall back to plain upload: %+v", st)
	}
	if !equalInts(st.MissingChunks, []int64{0, 1}) {
		t.Fatalf("fallback missing = %v, want [0 1]", st.MissingChunks)
	}

	// Missing candidate: artifact deleted entirely.
	if err := os.Remove(artPath); err != nil {
		t.Fatal(err)
	}
	code, st2 := env.createSessionRaw("retry-delivery-2.mov", total, fileSHA, true)
	if code != http.StatusCreated || st2.Status != StatusUploading || st2.ArtifactSource != nil {
		t.Fatalf("missing candidate must fall back to plain upload: code=%d %+v", code, st2)
	}

	// The fallback session is a fully functional upload session: chunks and
	// assembly behave exactly as before (断点续传不受影响).
	if code, _, _ := env.uploadChunk(st.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("fallback chunk 0: got %d", code)
	}
	q := env.getSession(st.SessionID)
	if !equalInts(q.MissingChunks, []int64{1}) {
		t.Fatalf("fallback missing after chunk 0 = %v, want [1]", q.MissingChunks)
	}
	if code, _, _ := env.uploadChunk(st.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("fallback chunk 1: got %d", code)
	}
	code, done := env.assemble(st.SessionID)
	if code != http.StatusOK || done.Status != StatusCompleted {
		t.Fatalf("fallback assemble: code=%d status=%s", code, done.Status)
	}
}

// 复用链候选损坏：来源成品被破坏后，经由复用会话组成的候选链也不得被选中，
// 必须退回普通上传会话（且候选遍历不得在单连接池上死锁）。
func TestReuseSkipsBrokenReuseChain(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 42
	file, fileSHA := randomFile(t, total)
	src := env.publish("chain-master.mov", file, fileSHA)

	// A reuse session now also matches digest+size, so the next lookup walks
	// a candidate whose artifact_source must be resolved.
	_, reuse := env.createSessionRaw("chain-copy.mov", total, fileSHA, true)
	if reuse.Status != StatusCompleted {
		t.Fatalf("reuse status = %s", reuse.Status)
	}

	// Break the ultimate backing artifact: every candidate (the source and
	// the reuse pointing at it) is now unusable.
	artPath := filepath.Join(env.dataDir, "artifacts",
		src.SessionID+"-"+sanitizeFilename("chain-master.mov"))
	if err := os.Truncate(artPath, 10); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var code int
	var st sessionJSON
	go func() {
		defer close(done)
		code, st = env.createSessionRaw("chain-retry.mov", total, fileSHA, true)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("reuse lookup deadlocked on a broken reuse chain")
	}
	if code != http.StatusCreated || st.Status != StatusUploading || st.ArtifactSource != nil {
		t.Fatalf("broken reuse chain must fall back to plain upload: code=%d %+v", code, st)
	}
}

// 未声明复用：旧客户端不带 reuse_artifact 字段时，即使存在同内容成品，
// 也仍然返回新建的普通上传会话。
func TestReuseNotDeclaredWhenOmitted(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	env.publish("already-there.mov", file, fileSHA)

	// Old-style body: no reuse_artifact key at all.
	count := (total + ChunkSize - 1) / ChunkSize
	body, _ := json.Marshal(map[string]any{
		"filename": "legacy-client.mov", "total_bytes": total,
		"chunk_count": count, "file_sha256": fileSHA,
	})
	resp, err := http.Post(env.srv.URL+"/api/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var st sessionJSON
	_ = json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("legacy create: got %d", resp.StatusCode)
	}
	if st.Status != StatusUploading || st.ArtifactSource != nil {
		t.Fatalf("undeclared reuse must create a plain upload session: %+v", st)
	}
	if !equalInts(st.MissingChunks, []int64{0, 1}) {
		t.Fatalf("legacy missing = %v, want [0 1]", st.MissingChunks)
	}

	// Explicitly opting out behaves the same.
	code, st2 := env.createSessionRaw("opted-out.mov", total, fileSHA, false)
	if code != http.StatusCreated || st2.Status != StatusUploading || st2.ArtifactSource != nil {
		t.Fatalf("opted-out create: code=%d %+v", code, st2)
	}
}

// 重启后复用引用仍能解析：新进程复用同一数据卷，复用会话的查询与下载正常。
func TestReuseSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	total := 2*ChunkSize + 99
	file, fileSHA := randomFile(t, total)

	db1, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := NewServer(db1, dataDir)
	srv1 := httptest.NewServer(s1.Handler())
	env1 := &testEnv{t: t, srv: srv1, server: s1, dataDir: dataDir}
	src := env1.publish("母版.mov", file, fileSHA)
	code, reuse := env1.createSessionRaw("巡展拷贝.mov", total, fileSHA, true)
	if code != http.StatusCreated || reuse.Status != StatusCompleted {
		t.Fatalf("reuse create: code=%d status=%s", code, reuse.Status)
	}
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
	env2 := &testEnv{t: t, srv: srv2, server: s2, dataDir: dataDir}

	st := env2.getSession(reuse.SessionID)
	if st.Status != StatusCompleted || st.ArtifactSource == nil || *st.ArtifactSource != src.SessionID {
		t.Fatalf("reuse reference lost after restart: %+v", st)
	}
	code, hdr, body := env2.download(reuse.SessionID, "")
	if code != http.StatusOK {
		t.Fatalf("download after restart: got %d", code)
	}
	if !bytes.Equal(body, file) {
		t.Fatal("download after restart: body mismatch")
	}
	if cd := hdr.Get("Content-Disposition"); !strings.Contains(cd, "%E5%B7%A1%E5%B1%95") {
		t.Fatalf("Content-Disposition after restart must carry the reuse filename: %q", cd)
	}
}

// 复用会话拒绝写入：已完成（复用）会话不接受分块上传与组装。
func TestReusedSessionRejectsWrites(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	env.publish("seed.mov", file, fileSHA)
	_, reuse := env.createSessionRaw("again.mov", total, fileSHA, true)
	if reuse.Status != StatusCompleted {
		t.Fatalf("reuse status = %s", reuse.Status)
	}
	if code, _, _ := env.uploadChunk(reuse.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusConflict {
		t.Fatalf("chunk upload to reused session: got %d", code)
	}
	if code, _ := env.assemble(reuse.SessionID); code != http.StatusConflict {
		t.Fatalf("assemble on reused session: got %d", code)
	}
}

// 迁移：旧库（无 artifact_source 列）打开后自动补齐可空列，
// 原有会话记录无需迁移数据，读取时成品来源为空。
func TestMigrateAddsArtifactSourceColumn(t *testing.T) {
	dataDir := t.TempDir()
	// Build a database with the pre-reuse schema by hand.
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
VALUES ('legacy1', 'old.mov', 10, 1, '` + sha256Hex([]byte("x")) + `', 'uploading');`)
	if err != nil {
		t.Fatal(err)
	}
	raw.Close()

	// Opening with the new binary migrates the schema in place.
	db, err := openDB(dataDir)
	if err != nil {
		t.Fatalf("openDB over legacy schema: %v", err)
	}
	defer db.Close()
	has, err := hasColumn(db, "sessions", "artifact_source")
	if err != nil || !has {
		t.Fatalf("artifact_source column missing after migration: has=%v err=%v", has, err)
	}
	s, err := NewServer(db, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := s.getSession("legacy1")
	if err != nil || legacy == nil {
		t.Fatalf("legacy session unreadable after migration: %v", err)
	}
	if legacy.ArtifactSource.Valid {
		t.Fatal("legacy row must keep NULL artifact_source without data migration")
	}
	if legacy.Filename != "old.mov" || legacy.Status != StatusUploading {
		t.Fatalf("legacy row altered by migration: %+v", legacy)
	}
	// The migrated schema accepts reuse sessions.
	sha := sha256Hex([]byte("y"))
	if err := s.createReusedSession(&Session{
		ID: "reuse1", Filename: "new.mov", TotalBytes: 10, ChunkCount: 1, FileSHA256: sha,
	}, "legacy1"); err != nil {
		t.Fatalf("insert with artifact_source on migrated schema: %v", err)
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

// rawCreate POSTs an arbitrary request body and returns the status plus
// how many sessions ended up persisted.
func (e *testEnv) rawCreate(body string, headers map[string]string) (int, int) {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/api/sessions", strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	var n int
	if err := e.server.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return resp.StatusCode, n
}

// 摘要大小写：X-Chunk-SHA256 必须为小写十六进制；全大写但数值正确的
// 摘要同样拒绝（400），且分块保持未确认。
func TestChunkUppercaseDigestRejected(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("upper.mov", total, fileSHA)
	data := chunkOf(t, file, total, 0)

	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/sessions/%s/chunks/0", env.srv.URL, sess.SessionID),
		bytes.NewReader(data))
	// Uppercase encoding of the otherwise-correct digest.
	req.Header.Set("X-Chunk-SHA256", strings.ToUpper(sha256Hex(data)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("uppercase digest header: got %d, want 400", resp.StatusCode)
	}

	// The chunk must remain unconfirmed: no row, zero bytes, still missing.
	st := env.getSession(sess.SessionID)
	if st.ReceivedCount != 0 || st.ConfirmedBytes != 0 {
		t.Fatalf("rejected chunk was recorded: count=%d bytes=%d", st.ReceivedCount, st.ConfirmedBytes)
	}
	if !equalInts(st.MissingChunks, []int64{0, 1}) {
		t.Fatalf("missing = %v, want [0 1]", st.MissingChunks)
	}
	if _, err := os.Stat(filepath.Join(env.dataDir, "chunks", sess.SessionID, "00000000.chunk")); !os.IsNotExist(err) {
		t.Fatalf("rejected chunk left a file on disk: %v", err)
	}
	// The session stays uploadable.
	if st.Status != StatusUploading {
		t.Fatalf("status = %s, want uploading", st.Status)
	}
}

// 尾随内容：合法对象后附加第二个 JSON 对象或尾随垃圾字节必须拒绝，
// 且不得创建会话。仅有 JSON 允许的空白尾随则仍受理。
func TestCreateSessionTrailingContentRejected(t *testing.T) {
	env := newTestEnv(t)
	sha := sha256Hex([]byte("x"))
	first := func() string {
		b, _ := json.Marshal(map[string]any{
			"filename": "trailing.mov", "total_bytes": 10, "chunk_count": 1, "file_sha256": sha,
		})
		return string(b)
	}
	bad := map[string]string{
		"second JSON object": first() + first(),
		"trailing garbage":   first() + "???",
	}
	for name, body := range bad {
		code, n := env.rawCreate(body, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("%s: got %d, want 400", name, code)
		}
		if n != 0 {
			t.Fatalf("%s: %d sessions persisted, want 0", name, n)
		}
	}
	// RFC 8259 allows whitespace after the top-level value.
	code, n := env.rawCreate(first()+"   \n\t ", nil)
	if code != http.StatusCreated || n != 1 {
		t.Fatalf("trailing whitespace: code=%d sessions=%d, want 201/1", code, n)
	}
}

// 超大 total_bytes：天花板分块数 (total+ChunkSize-1)/ChunkSize 在 int64
// 上溢出为负时，配合算出的负 chunk_count 必须拒绝；任何没有有效分块范围
// （chunk_count < 1）的元数据都不得建会话。
func TestCreateSessionHugeTotalBytesRejected(t *testing.T) {
	env := newTestEnv(t)
	sha := sha256Hex([]byte("x"))

	// math.MaxInt64 pushes the ceiling computation into int64 overflow;
	// derive the wrapped result in runtime int64 arithmetic, which is the
	// client-supplied count the bug lets through.
	var huge int64 = math.MaxInt64
	wrappedCount := (huge + ChunkSize - 1) / ChunkSize // negative via wrap-around
	if wrappedCount >= 0 {
		t.Fatalf("test setup wrong: wrappedCount=%d", wrappedCount)
	}
	overflowBody := `{"filename":"overflow.mov","total_bytes":` +
		fmt.Sprintf("%d", huge) + `,"chunk_count":` + fmt.Sprintf("%d", wrappedCount) +
		`,"file_sha256":"` + sha + `"}`
	code, n := env.rawCreate(overflowBody, nil)
	if code != http.StatusBadRequest {
		t.Fatalf("overflow total with negative chunk_count: got %d, want 400", code)
	}
	if n != 0 {
		t.Fatalf("overflow request persisted %d sessions, want 0", n)
	}

	// Same huge total paired with the positive (arbitrary-precision) ceiling
	// count must be rejected by the size bound, not crash the server.
	posCount := (huge/ChunkSize + 1) // 2^43, no negative wrap
	posBody := `{"filename":"huge.mov","total_bytes":` + fmt.Sprintf("%d", huge) +
		`,"chunk_count":` + fmt.Sprintf("%d", posCount) + `,"file_sha256":"` + sha + `"}`
	if code, n := env.rawCreate(posBody, nil); code != http.StatusBadRequest || n != 0 {
		t.Fatalf("positive huge count: code=%d sessions=%d, want 400/0", code, n)
	}
	// Anything above the supported size cap is refused outright.
	overCap := `{"filename":"cap.mov","total_bytes":` + fmt.Sprintf("%d", MaxTotalBytes+1) +
		`,"chunk_count":1,"file_sha256":"` + sha + `"}`
	if code, n := env.rawCreate(overCap, nil); code != http.StatusBadRequest || n != 0 {
		t.Fatalf("total over cap: code=%d sessions=%d, want 400/0", code, n)
	}

	// Plain non-positive chunk counts are invalid regardless of total.
	for _, cc := range []int64{0, -1, -42} {
		body := `{"filename":"neg.mov","total_bytes":10,"chunk_count":` +
			fmt.Sprintf("%d", cc) + `,"file_sha256":"` + sha + `"}`
		if code, _ := env.rawCreate(body, nil); code != http.StatusBadRequest {
			t.Fatalf("chunk_count=%d: got %d, want 400", cc, code)
		}
	}

	// The count must still equal the exact ceiling, even for large values.
	big := int64(1) << 40
	wrongCeil := (big + ChunkSize - 1) / ChunkSize
	body := `{"filename":"big.mov","total_bytes":` + fmt.Sprintf("%d", big) +
		`,"chunk_count":` + fmt.Sprintf("%d", wrongCeil+1) + `,"file_sha256":"` + sha + `"}`
	if code, n := env.rawCreate(body, nil); code != http.StatusBadRequest || n != 0 {
		t.Fatalf("large file mismatched count: code=%d sessions=%d, want 400/0", code, n)
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
