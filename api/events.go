package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// heartbeatInterval keeps the SSE connection open through proxies and lets
// clients notice a half-open link quickly instead of hanging silently. It is
// a variable so tests can shorten it; production never changes it.
var heartbeatInterval = 15 * time.Second

// ProgressEvent is one message on the session progress stream. Type is
// "snapshot" (the first event of every subscription, and the answer to a
// reconnect whose last seen sequence is already current) or "update" (a
// change to confirmed bytes / missing chunks / status / completion summary /
// failure reason). Seq is the persisted, monotonically increasing session
// progress version.
type ProgressEvent struct {
	Type    string       `json:"type"`
	Seq     int64        `json:"seq"`
	Session *sessionJSON `json:"session"`
}

// progressHub fans progress events out to every live SSE subscriber of a
// session. State changes are produced by ordinary API requests (chunk
// upload, conflict freeze, assembly) on other goroutines; the hub only
// delivers notifications, never owns the state itself.
type progressHub struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func newProgressHub() *progressHub {
	return &progressHub{subs: make(map[string]map[chan struct{}]struct{})}
}

// subscribe registers a wake-up channel for a session. The channel is closed
// by unsubscribe so a waiting select is released even if no event is sent.
func (h *progressHub) subscribe(sessionID string) chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	set := h.subs[sessionID]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		h.subs[sessionID] = set
	}
	set[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *progressHub) unsubscribe(sessionID string, ch chan struct{}) {
	h.mu.Lock()
	if set := h.subs[sessionID]; set != nil {
		if _, ok := set[ch]; ok {
			delete(set, ch)
			close(ch)
		}
		if len(set) == 0 {
			delete(h.subs, sessionID)
		}
	}
	h.mu.Unlock()
}

// notify wakes at most one pending waiter per subscriber channel; channels
// are buffered with capacity 1, so a burst of changes while a snapshot is
// being rendered collapses into one wake-up and the client simply receives
// the newest state.
func (h *progressHub) notify(sessionID string) {
	h.mu.Lock()
	for ch := range h.subs[sessionID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	h.mu.Unlock()
}

// afterSeq parses the Last-Event-ID style query parameter a reconnecting
// client carries (?after=N). Values <= 0 mean "no position known".
func afterSeq(r *http.Request) int64 {
	raw := r.URL.Query().Get("after")
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// resumePosition reports the sequence a reconnecting client last saw: the
// explicit ?after=N query parameter wins, otherwise the EventSource-native
// Last-Event-ID header is honored. Values <= 0 mean "no position known".
func resumePosition(r *http.Request) int64 {
	if n := afterSeq(r); n > 0 {
		return n
	}
	if raw := r.Header.Get("Last-Event-ID"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// handleProgressEvents serves GET /api/sessions/{id}/events as a Server-Sent
// Events stream. The stream always opens with a snapshot carrying the full
// session state; afterwards an update is pushed only when the persisted
// progress sequence advances (confirmed chunk, conflict freeze, assembly
// completion/failure). A reconnect carrying its last sequence (?after=N or
// Last-Event-ID) with no increments in between receives the current snapshot
// at that same sequence; if commits happened while it was disconnected, the
// opening snapshot already carries the newest state and sequence, so none of
// the increments are missed.
func (s *Server) handleProgressEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	after := resumePosition(r)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	sess, err := s.getSession(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sess == nil {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// writeEvent emits one SSE record. The id field lets browsers carry the
	// last sequence in Last-Event-ID after a transient drop; the page also
	// passes it explicitly as ?after= on reconnect.
	writeEvent := func(ev ProgressEvent) error {
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, data); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Opening snapshot: every subscription starts from the complete state,
	// including a reconnect with no (or a stale) position.
	resp, err := s.sessionResponse(sess)
	if err != nil {
		return
	}
	if after > 0 {
		// Visible resync marker for a reconnect carrying a last sequence.
		if _, err := fmt.Fprintf(w, ": resume after %d\n\n", after); err != nil {
			return
		}
	}
	if err := writeEvent(ProgressEvent{Type: "snapshot", Seq: sess.ProgressVersion, Session: resp}); err != nil {
		return
	}

	wake := s.hub.subscribe(id)
	defer s.hub.unsubscribe(id, wake)

	// Corner case the subscription window could miss: a change committed
	// between the opening snapshot read and the subscribe call. Re-check
	// once after registering so such an update is delivered immediately.
	if cur, err := s.getSession(id); err == nil && cur != nil && cur.ProgressVersion > sess.ProgressVersion {
		if resp, perr := s.sessionResponse(cur); perr == nil {
			if err := writeEvent(ProgressEvent{Type: "update", Seq: cur.ProgressVersion, Session: resp}); err != nil {
				return
			}
			sess = cur // advance lastSeq so the loop never re-emits this sequence
		}
	}

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	ctx := r.Context()
	lastSeq := sess.ProgressVersion
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			// A comment line is an SSE heartbeat: it keeps intermediaries
			// from idling the connection out and carries no event state.
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-wake:
			cur, err := s.getSession(id)
			if err != nil || cur == nil {
				return
			}
			if cur.ProgressVersion <= lastSeq {
				// Coalesced wake-up for a state we already rendered.
				continue
			}
			resp, err := s.sessionResponse(cur)
			if err != nil {
				return
			}
			if err := writeEvent(ProgressEvent{Type: "update", Seq: cur.ProgressVersion, Session: resp}); err != nil {
				return
			}
			lastSeq = cur.ProgressVersion
			// A terminal session has no further state changes; leave the
			// stream open with heartbeats so the page keeps a live link and
			// reconnect-after-restart semantics remain uniform.
		}
	}
}

// emitProgress notifies live subscribers after a state-changing transaction
// has committed. It is called after the commit point, so a woken subscriber
// always reads the new persisted sequence.
func (s *Server) emitProgress(sessionID string) {
	s.hub.notify(sessionID)
}
