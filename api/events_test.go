package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sseBlock is one parsed SSE record (blocks are separated by blank lines).
type sseBlock struct {
	comment bool
	id      string
	event   string
	data    string
}

// streamBlocks parses an SSE response body into blocks on the returned
// channel until the body closes. Comment blocks (heartbeats / resume
// markers) are delivered with comment=true so tests can distinguish them.
func streamBlocks(body io.Reader) <-chan sseBlock {
	ch := make(chan sseBlock, 16)
	go func() {
		defer close(ch)
		br := bufio.NewReader(body)
		for {
			var b sseBlock
			saw := false
			for {
				line, err := br.ReadString('\n')
				trimmed := strings.TrimRight(line, "\r\n")
				if trimmed != "" {
					saw = true
					switch {
					case strings.HasPrefix(trimmed, ":"):
						b.comment = true
					case strings.HasPrefix(trimmed, "id:"):
						b.id = strings.TrimSpace(trimmed[3:])
					case strings.HasPrefix(trimmed, "event:"):
						b.event = strings.TrimSpace(trimmed[6:])
					case strings.HasPrefix(trimmed, "data:"):
						b.data = strings.TrimSpace(trimmed[5:])
					}
				}
				if err != nil {
					if saw {
						ch <- b
					}
					return
				}
				if trimmed == "" && saw {
					ch <- b
					break
				}
				if trimmed == "" {
					// Defensive: stray blank line with nothing buffered.
					continue
				}
			}
		}
	}()
	return ch
}

func waitBlock(t *testing.T, ch <-chan sseBlock, within time.Duration) (sseBlock, bool) {
	t.Helper()
	select {
	case b, ok := <-ch:
		return b, ok
	case <-time.After(within):
		return sseBlock{}, false
	}
}

// nextEvent returns the next non-comment SSE block, failing the test on
// timeout or a closed stream.
func nextEvent(t *testing.T, ch <-chan sseBlock) sseBlock {
	t.Helper()
	for {
		b, ok := waitBlock(t, ch, 3*time.Second)
		if !ok {
			t.Fatal("timed out waiting for SSE event")
		}
		if b.comment {
			continue
		}
		return b
	}
}

// quietFor fails the test if any non-comment event arrives within d.
func quietFor(t *testing.T, ch <-chan sseBlock, d time.Duration) {
	t.Helper()
	select {
	case b, ok := <-ch:
		if ok && !b.comment {
			t.Fatalf("unexpected extra event: %+v", b)
		}
	case <-time.After(d):
	}
}

func parseProgressEvent(t *testing.T, b sseBlock) ProgressEvent {
	t.Helper()
	var ev ProgressEvent
	if err := json.Unmarshal([]byte(b.data), &ev); err != nil {
		t.Fatalf("invalid event JSON %q: %v", b.data, err)
	}
	return ev
}

// dialEvents opens an SSE subscription, optionally with ?after=N and/or a
// Last-Event-ID header. The caller closes resp.Body.
func (e *testEnv) dialEvents(id string, after int64, lastEventID string) (*http.Response, <-chan sseBlock) {
	e.t.Helper()
	u := e.srv.URL + "/api/sessions/" + id + "/events"
	if after > 0 {
		u += fmt.Sprintf("?after=%d", after)
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		e.t.Fatalf("events: status %d body %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		e.t.Fatalf("events Content-Type = %q", ct)
	}
	return resp, streamBlocks(resp.Body)
}

// 订阅首先收到完整快照，且快照载荷与 GET 会话查询逐字段一致。
func TestProgressEventsInitialSnapshot(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("snapshot.mov", total, fileSHA)

	resp, blocks := env.dialEvents(sess.SessionID, 0, "")
	defer resp.Body.Close()

	b := nextEvent(t, blocks)
	if b.event != "snapshot" {
		t.Fatalf("first event = %q, want snapshot", b.event)
	}
	if b.id != "1" {
		t.Fatalf("snapshot SSE id = %q, want 1", b.id)
	}
	ev := parseProgressEvent(t, b)
	if ev.Seq != 1 || ev.Session == nil {
		t.Fatalf("snapshot event malformed: %+v", ev)
	}
	snap := ev.Session
	if snap.SessionID != sess.SessionID || snap.Status != StatusUploading {
		t.Fatalf("snapshot session wrong: %+v", snap)
	}
	if snap.ReceivedCount != 0 || snap.ConfirmedBytes != 0 {
		t.Fatalf("fresh session must report zero progress: %+v", snap)
	}
	if !equalInts(snap.MissingChunks, []int64{0, 1}) {
		t.Fatalf("snapshot missing = %v, want [0 1]", snap.MissingChunks)
	}

	// The snapshot payload is exactly what the plain session query returns.
	got := env.getSession(sess.SessionID)
	if snap.Status != got.Status || snap.ConfirmedBytes != got.ConfirmedBytes ||
		snap.ReceivedCount != got.ReceivedCount || !equalInts(snap.MissingChunks, got.MissingChunks) ||
		snap.FinalSHA256 != got.FinalSHA256 || snap.Error != got.Error ||
		snap.ArtifactSource != got.ArtifactSource {
		t.Fatalf("snapshot %+v != GET response %+v", snap, got)
	}

	// No state change -> no update events (heartbeat stays at its 15s default).
	quietFor(t, blocks, 300*time.Millisecond)

	_ = file
}

// 未知会话的事件流与普通查询一样返回 404（且为普通 JSON 错误，不是事件流）。
func TestProgressEventsUnknownSession(t *testing.T) {
	env := newTestEnv(t)
	resp, err := http.Get(env.srv.URL + "/api/sessions/0123456789abcdef0123456789abcdef/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("events for unknown session: got %d, want 404", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("404 must be JSON, got %q", resp.Header.Get("Content-Type"))
	}
}

// 分块确认后流上恰好出现一次带递增序号的更新，重复幂等提交不产生事件；
// 组装完成后再收到一次 completed 更新——顺序为 snapshot(1) → 2 → 3 → 4。
func TestProgressEventsChunkUpdatesAndCompletionOrder(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("stream-order.mov", total, fileSHA)

	resp, blocks := env.dialEvents(sess.SessionID, 0, "")
	defer resp.Body.Close()

	if ev := parseProgressEvent(t, nextEvent(t, blocks)); ev.Seq != 1 {
		t.Fatalf("opening snapshot seq = %d, want 1", ev.Seq)
	}

	// Chunk 0 -> exactly one update at seq 2.
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	ev := parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Type != "update" || ev.Seq != 2 {
		t.Fatalf("after chunk 0: %+v, want update seq 2", ev)
	}
	if ev.Session.ConfirmedBytes != ChunkSize || ev.Session.ReceivedCount != 1 {
		t.Fatalf("update progress wrong: %+v", ev.Session)
	}
	if !equalInts(ev.Session.MissingChunks, []int64{1}) {
		t.Fatalf("update missing = %v, want [1]", ev.Session.MissingChunks)
	}

	// Idempotent duplicate: no state change, no event.
	if code, dup, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusOK || !dup {
		t.Fatalf("duplicate chunk: code=%d dup=%v", code, dup)
	}
	quietFor(t, blocks, 300*time.Millisecond)

	// Chunk 1 -> single update at seq 3, all chunks present.
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("chunk 1: got %d", code)
	}
	ev = parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Seq != 3 || ev.Session.ConfirmedBytes != total || len(ev.Session.MissingChunks) != 0 {
		t.Fatalf("after chunk 1: %+v, want seq 3 with all bytes confirmed", ev)
	}

	// Assembly publishes -> final update at seq 4, completed with digest.
	if code, done := env.assemble(sess.SessionID); code != http.StatusOK || done.Status != StatusCompleted {
		t.Fatalf("assemble: code=%d status=%s", code, done.Status)
	}
	ev = parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Type != "update" || ev.Seq != 4 {
		t.Fatalf("completion event: %+v, want update seq 4", ev)
	}
	if ev.Session.Status != StatusCompleted || ev.Session.FinalSHA256 == nil || *ev.Session.FinalSHA256 != fileSHA {
		t.Fatalf("completion payload wrong: %+v", ev.Session)
	}
	// Terminal state does not generate further updates.
	quietFor(t, blocks, 300*time.Millisecond)
}

// 冲突冻结在原事务中递增序号，订阅者收到 failed 更新与失败原因。
func TestProgressEventsConflictFreeze(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, _ := randomFile(t, total)
	sess := env.createSession("stream-conflict.mov", total, sha256Hex(file))

	resp, blocks := env.dialEvents(sess.SessionID, 0, "")
	defer resp.Body.Close()
	nextEvent(t, blocks) // snapshot seq 1

	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	nextEvent(t, blocks) // seq 2

	evil := make([]byte, ChunkSize)
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, evil); code != http.StatusConflict {
		t.Fatalf("conflict upload: got %d, want 409", code)
	}
	ev := parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Seq != 3 {
		t.Fatalf("freeze event seq = %d, want 3", ev.Seq)
	}
	if ev.Session.Status != StatusFailed || ev.Session.Error == nil ||
		!strings.Contains(*ev.Session.Error, "conflict") {
		t.Fatalf("freeze payload wrong: %+v", ev.Session)
	}
}

// 多个工作站同时订阅同一批次：每个连接都独立收到快照与后续更新。
func TestProgressEventsMultipleSubscribers(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 1
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("multi-sub.mov", total, fileSHA)

	r1, b1 := env.dialEvents(sess.SessionID, 0, "")
	defer r1.Body.Close()
	r2, b2 := env.dialEvents(sess.SessionID, 0, "")
	defer r2.Body.Close()
	nextEvent(t, b1)
	nextEvent(t, b2)

	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	for i, ch := range []<-chan sseBlock{b1, b2} {
		ev := parseProgressEvent(t, nextEvent(t, ch))
		if ev.Seq != 2 || ev.Session.ConfirmedBytes != ChunkSize {
			t.Fatalf("subscriber %d: %+v", i, ev)
		}
	}
}

// 重连携带当前序号、期间无增量：仍先收到当前状态的快照（同序号），
// 之后的新变化继续以 update 推送。
func TestProgressEventsReconnectUpToDateGetsSnapshot(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("reconnect-current.mov", total, fileSHA)

	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}

	// Reconnect at the latest sequence (2): no increments exist.
	resp, blocks := env.dialEvents(sess.SessionID, 2, "")
	defer resp.Body.Close()
	b := nextEvent(t, blocks)
	if b.event != "snapshot" {
		t.Fatalf("up-to-date reconnect first event = %q, want snapshot", b.event)
	}
	ev := parseProgressEvent(t, b)
	if ev.Seq != 2 || ev.Session.ConfirmedBytes != ChunkSize || !equalInts(ev.Session.MissingChunks, []int64{1}) {
		t.Fatalf("up-to-date reconnect snapshot wrong: %+v", ev)
	}

	// A change after reconnect arrives as a normal update, sequence continues.
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("chunk 1: got %d", code)
	}
	ev = parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Seq != 3 || ev.Session.ConfirmedBytes != total {
		t.Fatalf("post-reconnect update wrong: %+v", ev)
	}
}

// 断线期间错过多次提交：重连（用 Last-Event-ID 携带旧序号）直接收到
// 最新快照（completed、完成摘要就绪），不逐条补旧更新。
func TestProgressEventsReconnectStaleGetsLatestSnapshot(t *testing.T) {
	env := newTestEnv(t)
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)
	sess := env.createSession("reconnect-stale.mov", total, fileSHA)

	// First live subscription: sees only the seq-1 snapshot.
	first, blocks1 := env.dialEvents(sess.SessionID, 0, "")
	nextEvent(t, blocks1)
	first.Body.Close()

	// Disconnected window: both chunks confirm and assembly publishes.
	if code, _, _ := env.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	if code, _, _ := env.uploadChunk(sess.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("chunk 1: got %d", code)
	}
	if code, done := env.assemble(sess.SessionID); code != http.StatusOK || done.Status != StatusCompleted {
		t.Fatalf("assemble: code=%d status=%s", code, done.Status)
	}

	// Reconnect carrying the browser-style Last-Event-ID: 1.
	resp, blocks := env.dialEvents(sess.SessionID, 0, "1")
	defer resp.Body.Close()
	b := nextEvent(t, blocks)
	if b.event != "snapshot" {
		t.Fatalf("stale reconnect first event = %q, want snapshot", b.event)
	}
	ev := parseProgressEvent(t, b)
	if ev.Seq != 4 {
		t.Fatalf("stale reconnect snapshot seq = %d, want 4", ev.Seq)
	}
	if ev.Session.Status != StatusCompleted || ev.Session.FinalSHA256 == nil || *ev.Session.FinalSHA256 != fileSHA {
		t.Fatalf("stale reconnect must receive newest completed state: %+v", ev.Session)
	}
	if ev.Session.ConfirmedBytes != total {
		t.Fatalf("stale reconnect confirmed bytes = %d, want %d", ev.Session.ConfirmedBytes, total)
	}
}

// 重启：序号由 SQLite 持久化，新进程打开同一数据卷后快照序号接着旧值，
// 之后的确认继续在该值上递增。
func TestProgressSequenceSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	total := ChunkSize + 7
	file, fileSHA := randomFile(t, total)

	db1, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := NewServer(db1, dataDir)
	srv1 := httptest.NewServer(s1.Handler())
	env1 := &testEnv{t: t, srv: srv1, server: s1, dataDir: dataDir}
	sess := env1.createSession("seq-restart.mov", total, fileSHA)
	if code, _, _ := env1.uploadChunk(sess.SessionID, 0, chunkOf(t, file, total, 0)); code != http.StatusCreated {
		t.Fatalf("chunk 0: got %d", code)
	}
	srv1.Close()
	db1.Close()

	// "Restart": new process/server over the same volume.
	db2, err := openDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	s2, _ := NewServer(db2, dataDir)
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()
	env2 := &testEnv{t: t, srv: srv2, server: s2, dataDir: dataDir}

	resp, blocks := env2.dialEvents(sess.SessionID, 0, "")
	defer resp.Body.Close()
	ev := parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Seq != 2 {
		t.Fatalf("after restart snapshot seq = %d, want 2 (persisted)", ev.Seq)
	}
	if ev.Session.ConfirmedBytes != ChunkSize || !equalInts(ev.Session.MissingChunks, []int64{1}) {
		t.Fatalf("after restart snapshot state wrong: %+v", ev.Session)
	}

	// The sequence keeps increasing across the restart: 2 -> 3.
	if code, _, _ := env2.uploadChunk(sess.SessionID, 1, chunkOf(t, file, total, 1)); code != http.StatusCreated {
		t.Fatalf("chunk 1 after restart: got %d", code)
	}
	ev = parseProgressEvent(t, nextEvent(t, blocks))
	if ev.Seq != 3 || ev.Session.ConfirmedBytes != total {
		t.Fatalf("post-restart update wrong: %+v", ev)
	}
}

// 心跳：空闲连接周期性收到注释帧保持存活。
func TestProgressEventsHeartbeat(t *testing.T) {
	old := heartbeatInterval
	heartbeatInterval = 25 * time.Millisecond
	defer func() { heartbeatInterval = old }()

	env := newTestEnv(t)
	total := ChunkSize + 1
	_, fileSHA := randomFile(t, total)
	sess := env.createSession("heartbeat.mov", total, fileSHA)

	resp, blocks := env.dialEvents(sess.SessionID, 0, "")
	defer resp.Body.Close()
	nextEvent(t, blocks) // snapshot

	// A comment heartbeat must arrive well within a second even with no
	// progress, proving the stream is deliberately kept alive.
	b, ok := waitBlock(t, blocks, time.Second)
	if !ok {
		t.Fatal("no heartbeat received on an idle stream")
	}
	if !b.comment {
		t.Fatalf("expected heartbeat comment, got %+v", b)
	}
}
