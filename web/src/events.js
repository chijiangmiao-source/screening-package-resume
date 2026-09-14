// Server-Sent Events subscription for a session's progress stream.
//
// A modern browser opens GET /api/sessions/{id}/events: the server first
// sends a complete snapshot, then only sends an update when confirmed bytes,
// missing chunks, status, the completion summary or the failure reason
// changes. Every event carries the persisted, monotonically increasing
// sequence number (SSE `id`). On a transient drop the page shows a small
// "real-time updates interrupted" notice and reconnects on its own; upload
// controls are never touched by stream state. Browsers without EventSource
// keep working through the original 3-second polling fallback.
import { getSession } from './client.js'

// Evaluated on demand (not at module import) so tests can install an
// EventSource double before the app decides which transport to use.
export function eventsSupported() {
  return typeof globalThis !== 'undefined' && typeof globalThis.EventSource !== 'undefined'
}

const reconnectDelayMs = 2000
const fallbackPollMs = 3000

// Subscribe to live progress for a session.
//
// handlers:
//   onSnapshot(sessionJSON)  full state, delivered on (re)connect and, for
//                            fallback polling, on every refresh
//   onStatus('connecting' | 'live' | 'interrupted')
//
// Returns a controller with close(). The last seen sequence is carried on
// reconnect as ?after=N (and the browser also sends Last-Event-ID).
export function subscribeProgress(sessionId, handlers = {}) {
  const { onSnapshot, onStatus } = handlers
  let stopped = false
  let lastSeq = 0
  let es = null
  let timer = null

  function schedule(fn, delay) {
    timer = setTimeout(() => {
      timer = null
      fn()
    }, delay)
  }

  function dispatch(ev) {
    if (!ev || !ev.session) return
    if (typeof ev.seq === 'number' && ev.seq > lastSeq) lastSeq = ev.seq
    onSnapshot?.(ev.session)
  }

  function connectSSE() {
    if (stopped) return
    if (!eventsSupported()) {
      // Support disappeared between retries (e.g. tests): drop back to
      // polling rather than throwing out of a timer callback.
      pollOnce()
      return
    }
    onStatus?.('connecting')
    const url = `/api/sessions/${encodeURIComponent(sessionId)}/events` +
      (lastSeq ? `?after=${lastSeq}` : '')
    es = new EventSource(url)

    es.addEventListener('open', () => {
      if (!stopped) onStatus?.('live')
    })
    // Both "snapshot" and "update" carry the same full session payload; the
    // difference only matters to tests/inspectors, the page replaces state
    // wholesale either way (the server never sends partial updates).
    const onMessage = (e) => {
      let ev
      try {
        ev = JSON.parse(e.data)
      } catch {
        return
      }
      if (stopped) return
      onStatus?.('live')
      dispatch(ev)
    }
    es.addEventListener('snapshot', onMessage)
    es.addEventListener('update', onMessage)
    es.addEventListener('error', () => {
      if (stopped) return
      // The link dropped: surface the notice, but do not touch phase or
      // upload controls. We reconnect explicitly so the URL carries the last
      // sequence we rendered; the server answers with the current snapshot.
      onStatus?.('interrupted')
      if (es) {
        es.close()
        es = null
      }
      schedule(connectSSE, reconnectDelayMs)
    })
  }

  // Fallback for browsers without EventSource: poll the session endpoint on
  // the original 3-second cadence. A failed poll shows the same notice and
  // the loop keeps trying; it never alters upload state.
  async function pollOnce() {
    if (stopped) return
    const r = await getSession(sessionId)
    if (stopped) return
    if (r.status === 200) {
      onStatus?.('live')
      onSnapshot?.(r.body)
    } else if (!r.network) {
      // A definitive answer (e.g. 404) still keeps the notice up but does
      // not throw; subsequent polls may recover.
      onStatus?.('interrupted')
    } else {
      onStatus?.('interrupted')
    }
    if (!stopped) schedule(pollOnce, fallbackPollMs)
  }

  if (eventsSupported()) {
    connectSSE()
  } else {
    pollOnce()
  }

  return {
    get lastSeq() {
      return lastSeq
    },
    close() {
      stopped = true
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      if (es) {
        es.close()
        es = null
      }
    },
  }
}
