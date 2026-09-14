// One-shot acceptance suite. Runs against the REAL api service over the
// compose network (no stubs, no mocks) and inspects the shared data volume
// to confirm artifact visibility. Exits non-zero on any failure.
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'

const API = process.env.API_URL || 'http://api:8080'
const API_PEER = process.env.API_PEER_URL || '' // second process over the same volume
const DATA_DIR = process.env.DATA_DIR || '/data'
const CHUNK = 1048576

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex')
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// ---- Server-Sent Events client (raw streaming fetch, no EventSource in node) ----

function parseSSEBlock(block) {
  let id = null, event = null, data = ''
  for (const line of block.split('\n')) {
    const ln = line.replace(/\r$/, '')
    if (ln.startsWith(':')) return null // heartbeat / comment
    if (ln.startsWith('id:')) id = ln.slice(3).trim()
    else if (ln.startsWith('event:')) event = ln.slice(6).trim()
    else if (ln.startsWith('data:')) data += ln.slice(5).trim()
  }
  if (!data) return null
  let payload
  try { payload = JSON.parse(data) } catch { return null }
  return { id: id === null ? null : Number(id), event, payload }
}

// Open an SSE subscription. nextEvent() waits for the next non-heartbeat
// event; quiet() asserts nothing arrives; close() aborts the connection.
function openEvents(sessionId, { after = 0, base = API } = {}) {
  const controller = new AbortController()
  const url = `${base}/api/sessions/${sessionId}/events` + (after ? `?after=${after}` : '')
  const queue = []
  const waiters = []
  let failed = null
  const fail = (err) => {
    failed = err
    let w
    while ((w = waiters.shift())) w.reject(err || new Error('stream closed'))
  }
  // ready resolves once the response headers are validated as an SSE
  // response; the body keeps pumping in the background afterwards.
  const ready = (async () => {
    const resp = await fetch(url, { signal: controller.signal })
    if (!resp.ok || !(resp.headers.get('content-type') || '').startsWith('text/event-stream')) {
      throw new Error(`events open failed: HTTP ${resp.status}`)
    }
    // Pump independently of readiness so events queue while the caller is
    // still between `await ready` and the first nextEvent().
    ;(async () => {
      const reader = resp.body.getReader()
      const dec = new TextDecoder()
      let buf = ''
      try {
        while (true) {
          const { value, done } = await reader.read()
          if (done) break
          buf += dec.decode(value, { stream: true })
          let cut
          while ((cut = buf.indexOf('\n\n')) >= 0) {
            const ev = parseSSEBlock(buf.slice(0, cut))
            buf = buf.slice(cut + 2)
            if (ev) {
              // Hand the event to a waiting nextEvent, or buffer it — never
              // both, or one event would be observed twice.
              const waiter = waiters.shift()
              if (waiter) waiter.resolve(ev)
              else queue.push(ev)
            }
          }
        }
      } catch (err) {
        if (err.name !== 'AbortError') fail(err)
      }
      if (!failed) fail(null)
    })()
  })()
  return {
    ready,
    async nextEvent(ms = 5000) {
      if (queue.length) return queue.shift()
      if (failed) throw failed
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error('timed out waiting for SSE event')), ms)
        waiters.push({ resolve: (ev) => { clearTimeout(t); resolve(ev) }, reject: (e) => { clearTimeout(t); reject(e) } })
      })
    },
    async quiet(ms = 400) {
      await sleep(ms)
      return queue.length === 0 && !failed
    },
    close() { controller.abort() },
  }
}

let passed = 0
let failed = 0
function check(name, cond, detail = '') {
  if (cond) {
    passed++
    console.log(`  PASS  ${name}`)
  } else {
    failed++
    console.log(`  FAIL  ${name} ${detail}`)
  }
}

async function api(p, options = {}) {
  const r = await fetch(`${API}/api${p}`, options)
  const text = await r.text()
  let body = null
  try { body = text ? JSON.parse(text) : null } catch { body = { error: text } }
  return { status: r.status, body }
}

function artifactFor(sessionId) {
  const dir = path.join(DATA_DIR, 'artifacts')
  if (!fs.existsSync(dir)) return null
  const hit = fs.readdirSync(dir).find((n) => n.startsWith(sessionId + '-'))
  return hit ? path.join(dir, hit) : null
}

async function waitForApi() {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`${API}/api/health`)
      if (r.ok) return
    } catch { /* not up yet */ }
    await sleep(1000)
  }
  throw new Error(`api not healthy at ${API}`)
}

async function createSession(name, buf, fileSha) {
  return api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename: name,
      total_bytes: buf.length,
      chunk_count: Math.ceil(buf.length / CHUNK),
      file_sha256: fileSha,
    }),
  })
}

async function putChunk(sessionId, idx, buf) {
  return api(`/sessions/${sessionId}/chunks/${idx}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream', 'X-Chunk-SHA256': sha(buf) },
    body: buf,
  })
}

const slice = (buf, i) => buf.subarray(i * CHUNK, Math.min((i + 1) * CHUNK, buf.length))

async function main() {
  console.log(`verify: waiting for api at ${API}`)
  await waitForApi()
  console.log('verify: api is healthy, running acceptance flow\n')

  // ---------- scenario 1: interrupt -> resume -> duplicate -> publish ----------
  console.log('[1] interruption / resume / idempotent duplicate / atomic publish')
  const file = crypto.randomBytes(2 * CHUNK + 333_333) // 3 chunks, short tail
  const fileSha = sha(file)
  const count = Math.ceil(file.length / CHUNK)

  let r = await createSession('festival-screening.mov', file, fileSha)
  check('create session returns 201', r.status === 201, JSON.stringify(r.body))
  const sess = r.body
  check('initial missing list is complete',
    JSON.stringify(sess.missing_chunks) === JSON.stringify([...Array(count).keys()]),
    `got ${sess.missing_chunks}`)

  // "network drop": only chunk 0 is delivered
  r = await putChunk(sess.session_id, 0, slice(file, 0))
  check('chunk 0 accepted', r.status === 201, JSON.stringify(r.body))

  r = await api(`/sessions/${sess.session_id}`)
  check('after interruption missing = [1,2]',
    JSON.stringify(r.body.missing_chunks) === '[1,2]', `got ${r.body.missing_chunks}`)
  check('confirmed bytes = exactly 1 MiB', r.body.confirmed_bytes === CHUNK)

  // duplicate delivery of chunk 0 with identical content
  r = await putChunk(sess.session_id, 0, slice(file, 0))
  check('duplicate chunk returns success', r.status === 200, `got ${r.status}`)
  check('duplicate flagged idempotent', r.body.duplicate === true)
  r = await api(`/sessions/${sess.session_id}`)
  check('duplicate not counted twice',
    r.body.received_count === 1 && r.body.confirmed_bytes === CHUNK,
    `count=${r.body.received_count} bytes=${r.body.confirmed_bytes}`)

  // resume: deliver only the missing chunks
  for (const idx of r.body.missing_chunks) {
    const up = await putChunk(sess.session_id, idx, slice(file, idx))
    check(`resumed chunk ${idx} accepted`, up.status === 201, JSON.stringify(up.body))
  }
  r = await api(`/sessions/${sess.session_id}`)
  check('no chunks missing after resume', r.body.missing_chunks.length === 0)
  check('confirmed bytes = total', r.body.confirmed_bytes === file.length)

  r = await api(`/sessions/${sess.session_id}/assemble`, { method: 'POST' })
  check('assemble succeeds', r.status === 200, JSON.stringify(r.body))
  check('session completed', r.body.status === 'completed')
  check('final digest matches whole-file sha256', r.body.final_sha256 === fileSha,
    `${r.body.final_sha256} != ${fileSha}`)

  const art = artifactFor(sess.session_id)
  check('artifact visible on data volume', art !== null)
  if (art) {
    check('artifact content hash matches', sha(fs.readFileSync(art)) === fileSha)
    check('artifact has no temp-file leftovers',
      !fs.readdirSync(path.join(DATA_DIR, 'artifacts')).some((n) => n.startsWith('.assemble-')))
  }

  // ---------- scenario 2: conflicting chunk freezes the session ----------
  console.log('[2] same index, different content -> session frozen as failed')
  const file2 = crypto.randomBytes(CHUNK + 100)
  const file2Sha = sha(file2)
  r = await createSession('conflict-reel.mov', file2, file2Sha)
  const s2 = r.body.session_id
  await putChunk(s2, 0, slice(file2, 0))
  const evil = crypto.randomBytes(CHUNK)
  r = await putChunk(s2, 0, evil)
  check('conflicting chunk rejected with 409', r.status === 409, `got ${r.status}`)
  r = await api(`/sessions/${s2}`)
  check('session frozen as failed', r.body.status === 'failed')
  check('failure reason recorded', typeof r.body.error === 'string' && r.body.error.includes('conflict'))
  r = await putChunk(s2, 1, slice(file2, 1))
  check('failed session rejects further chunks', r.status === 409, `got ${r.status}`)
  r = await api(`/sessions/${s2}/assemble`, { method: 'POST' })
  check('failed session cannot assemble', r.status === 409, `got ${r.status}`)
  check('no artifact published for failed session', artifactFor(s2) === null)

  // ---------- scenario 3: whole-file digest mismatch -> failed, invisible ----------
  console.log('[3] assembled digest mismatch -> failed, artifact invisible')
  const file3 = crypto.randomBytes(CHUNK + 7)
  const wrongSha = sha(crypto.randomBytes(64))
  r = await createSession('tampered-reel.mov', file3, wrongSha)
  const s3 = r.body.session_id
  await putChunk(s3, 0, slice(file3, 0))
  await putChunk(s3, 1, slice(file3, 1))
  r = await api(`/sessions/${s3}/assemble`, { method: 'POST' })
  check('assemble with wrong declared digest fails', r.status === 409, `got ${r.status}`)
  check('session marked failed', r.body.status === 'failed')
  check('no artifact visible after digest mismatch', artifactFor(s3) === null)

  // ---------- scenario 4: protocol validation ----------
  console.log('[4] chunk-length and digest-header validation')
  const file4 = crypto.randomBytes(CHUNK + 500)
  r = await createSession('length-check.mov', file4, sha(file4))
  const s4 = r.body.session_id
  r = await putChunk(s4, 0, file4.subarray(0, CHUNK - 1))
  check('non-last chunk shorter than 1 MiB rejected', r.status === 400, `got ${r.status}`)
  r = await putChunk(s4, 1, file4.subarray(CHUNK, CHUNK + 499))
  check('last chunk with wrong length rejected', r.status === 400, `got ${r.status}`)
  r = await putChunk(s4, 1, file4.subarray(CHUNK))
  check('exact-length last chunk accepted', r.status === 201, `got ${r.status}`)
  r = await api(`/sessions/${s4}/chunks/0`, {
    method: 'POST',
    headers: { 'X-Chunk-SHA256': sha(Buffer.from('lies')) },
    body: slice(file4, 0),
  })
  check('digest header not matching content rejected', r.status === 400, `got ${r.status}`)

  // ---------- scenario 5: artifact download with range resume ----------
  console.log('[5] artifact download: full, ranged resume, invalid ranges')
  // The session from scenario 1 is completed; its artifact is on the volume.
  const dl = `/sessions/${sess.session_id}/download`

  // Old session query still works after publishing.
  r = await api(`/sessions/${sess.session_id}`)
  check('completed session still queryable', r.status === 200 && r.body.status === 'completed')

  // Full download: 200, byte-identical, original filename advertised.
  let resp = await fetch(`${API}/api${dl}`)
  const full = Buffer.from(await resp.arrayBuffer())
  check('full download returns 200', resp.status === 200, `got ${resp.status}`)
  check('full download bytes identical to source', full.equals(file),
    `got ${full.length} bytes, want ${file.length}`)
  check('full download Content-Length is total bytes',
    resp.headers.get('content-length') === String(file.length),
    `got ${resp.headers.get('content-length')}`)
  check('full download advertises Accept-Ranges: bytes',
    resp.headers.get('accept-ranges') === 'bytes')
  check('Content-Disposition keeps original filename',
    (resp.headers.get('content-disposition') || '').includes('festival-screening.mov'),
    `got ${resp.headers.get('content-disposition')}`)

  // Interrupted transfer resumes by offset: first part, then the remainder.
  const split = CHUNK // pretend the connection dropped after 1 MiB
  resp = await fetch(`${API}/api${dl}`, { headers: { Range: `bytes=0-${split - 1}` } })
  const part1 = Buffer.from(await resp.arrayBuffer())
  check('first range returns 206', resp.status === 206, `got ${resp.status}`)
  check('first range Content-Range exact',
    resp.headers.get('content-range') === `bytes 0-${split - 1}/${file.length}`,
    `got ${resp.headers.get('content-range')}`)
  check('first range Content-Length exact',
    resp.headers.get('content-length') === String(split),
    `got ${resp.headers.get('content-length')}`)
  check('first range bytes match source prefix', part1.equals(file.subarray(0, split)))

  resp = await fetch(`${API}/api${dl}`, { headers: { Range: `bytes=${split}-` } })
  const part2 = Buffer.from(await resp.arrayBuffer())
  check('resume range returns 206', resp.status === 206, `got ${resp.status}`)
  check('resume range Content-Range exact',
    resp.headers.get('content-range') === `bytes ${split}-${file.length - 1}/${file.length}`,
    `got ${resp.headers.get('content-range')}`)
  check('rejoined parts identical to source', Buffer.concat([part1, part2]).equals(file))

  // Out-of-bounds and multi-range requests: 416 with no body.
  resp = await fetch(`${API}/api${dl}`, { headers: { Range: `bytes=${file.length}-` } })
  let body = await resp.text()
  check('out-of-bounds range returns 416', resp.status === 416, `got ${resp.status}`)
  check('416 carries Content-Range */size',
    resp.headers.get('content-range') === `bytes */${file.length}`,
    `got ${resp.headers.get('content-range')}`)
  check('416 has no body', body.length === 0, `got ${body.length} bytes`)
  resp = await fetch(`${API}/api${dl}`, { headers: { Range: 'bytes=0-1,3-4' } })
  body = await resp.text()
  check('multi-range request returns 416', resp.status === 416, `got ${resp.status}`)
  check('multi-range 416 has no body', body.length === 0, `got ${body.length} bytes`)

  // Sessions that never published cannot be downloaded.
  resp = await fetch(`${API}/api/sessions/${s4}/download`) // still uploading
  check('uploading session download returns 409', resp.status === 409, `got ${resp.status}`)
  resp = await fetch(`${API}/api/sessions/${s2}/download`) // frozen as failed
  check('failed session download returns 409', resp.status === 409, `got ${resp.status}`)
  resp = await fetch(`${API}/api/sessions/${'0'.repeat(32)}/download`)
  check('unknown session download returns 404', resp.status === 404, `got ${resp.status}`)

  // ---------- scenario 6: strict digest casing and session metadata ----------
  console.log('[6] strict digest casing, single-object body, chunk-count range')
  const file6 = crypto.randomBytes(CHUNK + 11)
  r = await createSession('strict-check.mov', file6, sha(file6))
  const s6 = r.body.session_id

  // Uppercase encoding of the otherwise-correct chunk digest is rejected.
  r = await api(`/sessions/${s6}/chunks/0`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream', 'X-Chunk-SHA256': sha(slice(file6, 0)).toUpperCase() },
    body: slice(file6, 0),
  })
  check('uppercase (but correct) chunk digest rejected', r.status === 400, `got ${r.status}`)
  r = await api(`/sessions/${s6}`)
  check('uppercase-rejected chunk stays unconfirmed',
    r.body.received_count === 0 && r.body.confirmed_bytes === 0,
    `count=${r.body.received_count} bytes=${r.body.confirmed_bytes}`)

  // A valid JSON object followed by a second object / garbage is rejected.
  const validMeta = JSON.stringify({
    filename: 'trailing.mov',
    total_bytes: file6.length,
    chunk_count: Math.ceil(file6.length / CHUNK),
    file_sha256: sha(file6),
  })
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: validMeta + validMeta,
  })
  check('trailing second JSON object rejected', r.status === 400, `got ${r.status}`)
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: validMeta + '???',
  })
  check('trailing garbage after JSON body rejected', r.status === 400, `got ${r.status}`)

  // Overflow: near-MaxInt64 total_bytes with the wrapped (negative) ceiling
  // count must be refused. BigInt.asIntN wraps the (total+ChunkSize-1)
  // addition the way int64 arithmetic does.
  const huge = 2n ** 63n - 1n
  const wrappedCount = BigInt.asIntN(64, BigInt.asIntN(64, huge + BigInt(CHUNK) - 1n) / BigInt(CHUNK))
  if (wrappedCount >= 0n) throw new Error(`test setup: expected negative wrap, got ${wrappedCount}`)
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: `{"filename":"overflow.mov","total_bytes":${huge},"chunk_count":${wrappedCount},"file_sha256":"${sha(file6)}"}`,
  })
  check('overflow total_bytes with wrapped negative chunk_count rejected', r.status === 400, `got ${r.status}`)
  // Same huge total with the positive (mathematically correct) count exceeds
  // the supported size cap and must also be refused.
  const posCount = (huge + BigInt(CHUNK) - 1n) / BigInt(CHUNK)
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: `{"filename":"toobig.mov","total_bytes":${huge},"chunk_count":${posCount},"file_sha256":"${sha(file6)}"}`,
  })
  check('petabyte-scale total_bytes rejected by size cap', r.status === 400, `got ${r.status}`)
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename: 'zero.mov', total_bytes: 10, chunk_count: 0, file_sha256: sha(file6) }),
  })
  check('zero chunk_count rejected', r.status === 400, `got ${r.status}`)

  // ---------- scenario 7: artifact reuse for repeat deliveries ----------
  console.log('[7] artifact reuse: same content, new filename, zero chunks')
  // Scenario 1 published `file`; a repeat delivery of the same content under
  // a different filename and with reuse declared must complete at creation.
  r = await api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename: 'festival-screening-revival.mov',
      total_bytes: file.length,
      chunk_count: count,
      file_sha256: fileSha,
      reuse_artifact: true,
    }),
  })
  check('reuse create returns 201', r.status === 201, JSON.stringify(r.body))
  check('reuse hit completes immediately', r.body.status === 'completed', `got ${r.body.status}`)
  check('reuse hit carries final digest', r.body.final_sha256 === fileSha,
    `got ${r.body.final_sha256}`)
  check('reuse hit uploads zero chunks', r.body.received_count === 0,
    `received_count=${r.body.received_count}`)
  check('reuse hit has no missing chunks',
    Array.isArray(r.body.missing_chunks) && r.body.missing_chunks.length === 0,
    `got ${r.body.missing_chunks}`)
  check('reuse hit references the source session', r.body.artifact_source === sess.session_id,
    `got ${r.body.artifact_source}`)
  const reuseId = r.body.session_id
  check('reuse creates a new session id', typeof reuseId === 'string' && reuseId !== sess.session_id)

  // Query and download go through the NEW session id.
  r = await api(`/sessions/${reuseId}`)
  check('reused session queryable by new id', r.status === 200 && r.body.status === 'completed')
  check('query keeps the reuse reference', r.body.artifact_source === sess.session_id)

  resp = await fetch(`${API}/api/sessions/${reuseId}/download`)
  const reusedBody = Buffer.from(await resp.arrayBuffer())
  check('reused download returns 200', resp.status === 200, `got ${resp.status}`)
  check('reused download bytes identical to source', reusedBody.equals(file),
    `got ${reusedBody.length} bytes, want ${file.length}`)
  check('reused download filename is the new submission',
    (resp.headers.get('content-disposition') || '').includes('festival-screening-revival.mov'),
    `got ${resp.headers.get('content-disposition')}`)
  resp = await fetch(`${API}/api/sessions/${reuseId}/download`, { headers: { Range: `bytes=${CHUNK}-` } })
  const reusedTail = Buffer.from(await resp.arrayBuffer())
  check('reused download range resume returns 206', resp.status === 206, `got ${resp.status}`)
  check('reused download range bytes match', reusedTail.equals(file.subarray(CHUNK)))

  // Old clients that never declare reuse still get a plain upload session.
  r = await createSession('legacy-client.mov', file, fileSha) // no reuse_artifact key
  check('undeclared reuse returns 201', r.status === 201, `got ${r.status}`)
  check('undeclared reuse stays a normal upload session',
    r.body.status === 'uploading' && !r.body.artifact_source,
    `status=${r.body.status} source=${r.body.artifact_source}`)
  check('undeclared reuse lists every chunk missing',
    JSON.stringify(r.body.missing_chunks) === JSON.stringify([...Array(count).keys()]),
    `got ${r.body.missing_chunks}`)

  // ---------- scenario 8: broken candidate falls back to normal upload ----------
  console.log('[8] length-abnormal / missing candidate -> normal upload session')
  const file8 = crypto.randomBytes(CHUNK + 42)
  const file8Sha = sha(file8)
  const count8 = Math.ceil(file8.length / CHUNK)
  r = await createSession('brittle-source.mov', file8, file8Sha)
  const s8 = r.body.session_id
  for (const idx of [...Array(count8).keys()]) {
    const up = await putChunk(s8, idx, slice(file8, idx))
    check(`brittle source chunk ${idx} accepted`, up.status === 201, JSON.stringify(up.body))
  }
  r = await api(`/sessions/${s8}/assemble`, { method: 'POST' })
  check('brittle source published', r.status === 200 && r.body.status === 'completed',
    JSON.stringify(r.body))
  const art8 = artifactFor(s8)
  check('brittle artifact visible on volume', art8 !== null)

  const reuseCreate8 = () => api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename: 'brittle-redelivery.mov',
      total_bytes: file8.length,
      chunk_count: count8,
      file_sha256: file8Sha,
      reuse_artifact: true,
    }),
  })

  // Length-abnormal candidate: truncated artifact must not be reused.
  if (art8) fs.truncateSync(art8, Math.floor(file8.length / 2))
  r = await reuseCreate8()
  check('length-abnormal candidate returns 201', r.status === 201, `got ${r.status}`)
  check('length-abnormal candidate falls back to upload session',
    r.body.status === 'uploading' && !r.body.artifact_source && !r.body.final_sha256,
    `status=${r.body.status} source=${r.body.artifact_source} final=${r.body.final_sha256}`)
  check('fallback session lists every chunk missing',
    JSON.stringify(r.body.missing_chunks) === JSON.stringify([...Array(count8).keys()]),
    `got ${r.body.missing_chunks}`)

  // Missing candidate: artifact deleted entirely.
  if (art8) fs.unlinkSync(art8)
  r = await reuseCreate8()
  check('missing candidate falls back to upload session',
    r.status === 201 && r.body.status === 'uploading' && !r.body.artifact_source,
    `status=${r.status}/${r.body.status} source=${r.body.artifact_source}`)

  // ---------- scenario 9: SSE progress event stream ----------
  console.log('[9] progress event stream: snapshot, per-change updates, reconnect')
  const file9 = crypto.randomBytes(2 * CHUNK + 333) // 3 chunks
  const file9Sha = sha(file9)
  const count9 = Math.ceil(file9.length / CHUNK)
  r = await createSession('live-progress.mov', file9, file9Sha)
  check('events scenario session created', r.status === 201, JSON.stringify(r.body))
  const s9 = r.body.session_id

  // Unknown session: the stream endpoint answers 404 JSON like GET does.
  const ghost = await fetch(`${API}/api/sessions/${'0'.repeat(32)}/events`)
  check('events for unknown session return 404', ghost.status === 404, `got ${ghost.status}`)
  check('events 404 is JSON, not a stream',
    (ghost.headers.get('content-type') || '').startsWith('application/json'))
  await ghost.body.cancel()

  // Subscription first receives the complete snapshot at sequence 1.
  const stream = openEvents(s9)
  await stream.ready
  let ev = await stream.nextEvent()
  check('subscription opens with a snapshot', ev.event === 'snapshot', `got ${ev.event}`)
  check('snapshot sequence starts at 1', ev.id === 1, `got id=${ev.id}`)
  check('snapshot payload is the full uploading session',
    ev.payload.session && ev.payload.session.status === 'uploading' &&
    ev.payload.session.confirmed_bytes === 0 &&
    JSON.stringify(ev.payload.session.missing_chunks) === JSON.stringify([0, 1, 2]),
    JSON.stringify(ev.payload.session))
  check('snapshot matches GET session response',
    (await api(`/sessions/${s9}`)).body.session_id === ev.payload.session.session_id)

  // Chunk 0 confirmed -> exactly one numbered update.
  await putChunk(s9, 0, slice(file9, 0))
  ev = await stream.nextEvent()
  check('chunk confirmation pushes one update', ev.event === 'update' && ev.id === 2,
    `got ${ev.event}/${ev.id}`)
  check('update carries confirmed bytes and missing list',
    ev.payload.session.confirmed_bytes === CHUNK &&
    JSON.stringify(ev.payload.session.missing_chunks) === '[1,2]',
    JSON.stringify({ b: ev.payload.session.confirmed_bytes, m: ev.payload.session.missing_chunks }))

  // Idempotent duplicate changes nothing -> no update event.
  await putChunk(s9, 0, slice(file9, 0))
  check('idempotent duplicate emits no update', await stream.quiet(500))

  // Remaining chunks each produce one incrementing update.
  await putChunk(s9, 1, slice(file9, 1))
  ev = await stream.nextEvent()
  check('second chunk update is seq 3', ev.id === 3 && ev.payload.session.confirmed_bytes === 2 * CHUNK,
    `got id=${ev.id}`)
  await putChunk(s9, 2, slice(file9, 2))
  ev = await stream.nextEvent()
  check('third chunk update is seq 4 with everything confirmed',
    ev.id === 4 && ev.payload.session.confirmed_bytes === file9.length &&
    ev.payload.session.missing_chunks.length === 0,
    `got id=${ev.id}`)

  // Assembly completion arrives as the final, ordered update.
  r = await api(`/sessions/${s9}/assemble`, { method: 'POST' })
  check('streamed session assembles', r.status === 200 && r.body.status === 'completed',
    JSON.stringify(r.body))
  ev = await stream.nextEvent()
  check('completion is delivered in order as seq 5',
    ev.event === 'update' && ev.id === 5 && ev.payload.session.status === 'completed' &&
    ev.payload.session.final_sha256 === file9Sha,
    `got ${ev.event}/${ev.id}`)
  check('completed session emits no further updates', await stream.quiet(500))
  stream.close()

  // Reconnect carrying the last sequence with no increments in between:
  // the server still answers with the current snapshot at that sequence.
  const replay = openEvents(s9, { after: 5 })
  await replay.ready
  ev = await replay.nextEvent()
  check('up-to-date reconnect gets the current snapshot',
    ev.event === 'snapshot' && ev.id === 5 && ev.payload.session.status === 'completed' &&
    ev.payload.session.final_sha256 === file9Sha,
    `got ${ev.event}/${ev.id}`)
  replay.close()

  // Stale reconnect (simulating a longer disconnect) also gets the newest
  // snapshot rather than a replayed delta.
  const stale = openEvents(s9, { after: 2 })
  await stale.ready
  ev = await stale.nextEvent()
  check('stale reconnect gets the newest snapshot directly',
    ev.event === 'snapshot' && ev.id === 5 && ev.payload.session.status === 'completed',
    `got ${ev.event}/${ev.id}`)
  stale.close()

  // ----- cross-process persistence: a second server over the SAME volume -----
  if (API_PEER) {
    console.log('[9b] sequence persists across processes and keeps increasing')
    // The peer container is only "started" (not health-checked) by compose;
    // wait for it here so a slow boot cannot fail the subscription.
    for (let i = 0; i < 60; i++) {
      try {
        if ((await fetch(`${API_PEER}/api/health`)).ok) break
      } catch { /* peer still booting */ }
      await sleep(1000)
    }

    const fileP = crypto.randomBytes(CHUNK + 333) // 2 chunks
    const filePSha = sha(fileP)
    r = await createSession('cross-process.mov', fileP, filePSha)
    check('peer scenario session created', r.status === 201, JSON.stringify(r.body))
    const sp = r.body.session_id

    // Write chunk 0 through the primary process; the sequence is now 2.
    await putChunk(sp, 0, slice(fileP, 0))

    // The second process reads the persisted sequence and snapshots it.
    const peerStream = openEvents(sp, { base: API_PEER })
    await peerStream.ready
    ev = await peerStream.nextEvent()
    check('peer process snapshot shows persisted seq 2',
      ev.event === 'snapshot' && ev.id === 2 && ev.payload.session.confirmed_bytes === CHUNK &&
      JSON.stringify(ev.payload.session.missing_chunks) === '[1]',
      `got ${ev.event}/${ev.id}`)

    // Chunk committed by the peer process increments to 3 and its own live
    // subscriber receives the update.
    const peerPut = await fetch(`${API_PEER}/api/sessions/${sp}/chunks/1`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream', 'X-Chunk-SHA256': sha(slice(fileP, 1)) },
      body: slice(fileP, 1),
    })
    check('peer process accepts chunk', peerPut.status === 201, `got ${peerPut.status}`)
    ev = await peerStream.nextEvent()
    check('sequence keeps increasing across processes to seq 3',
      ev.id === 3 && ev.payload.session.confirmed_bytes === fileP.length &&
      ev.payload.session.missing_chunks.length === 0,
      `got id=${ev.id}`)

    // Assembly by the peer publishes (seq 4) and notifies its subscriber.
    const peerAssemble = await fetch(`${API_PEER}/api/sessions/${sp}/assemble`, { method: 'POST' })
    check('peer process assembles session', peerAssemble.status === 200,
      `got ${peerAssemble.status}`)
    ev = await peerStream.nextEvent()
    check('completion observed on the peer at seq 4',
      ev.id === 4 && ev.payload.session.status === 'completed' &&
      ev.payload.session.final_sha256 === filePSha,
      `got id=${ev.id}`)
    peerStream.close()

    // The primary process never handled these writes: a fresh subscription
    // through it must still return the newest completed snapshot from the
    // shared database, proving the sequence survives across processes.
    const primaryReplay = openEvents(sp, { after: 4 })
    await primaryReplay.ready
    ev = await primaryReplay.nextEvent()
    check('primary reconnect reads peer-persisted newest snapshot',
      ev.event === 'snapshot' && ev.id === 4 && ev.payload.session.status === 'completed' &&
      ev.payload.session.final_sha256 === filePSha,
      `got ${ev.event}/${ev.id}`)
    primaryReplay.close()

    // And a new chunk stream created without a position on the peer likewise
    // starts from the persisted sequence (no restart resets it to 1).
    const peerFresh = openEvents(sp, { base: API_PEER })
    await peerFresh.ready
    ev = await peerFresh.nextEvent()
    check('fresh peer subscription does not restart the sequence at 1',
      ev.id === 4 && ev.payload.session.status === 'completed', `got id=${ev.id}`)
    peerFresh.close()
  } else {
    console.log('  (API_PEER_URL unset; skipping cross-process sequence checks)')
  }

  // ---------- scenario 10: idempotent create token ----------
  console.log('[10] create token: lost-response replay, conflict, restart/peer resolution')
  const newToken = () => crypto.randomBytes(16).toString('hex')
  const createBody = (name, buf, fileSha, { reuse = false, token = '' } = {}) =>
    JSON.stringify({
      filename: name,
      total_bytes: buf.length,
      chunk_count: Math.ceil(buf.length / CHUNK),
      file_sha256: fileSha,
      reuse_artifact: reuse,
      ...(token ? { create_token: token } : {}),
    })

  // postCreate posts one create request and returns {status, body}.
  const postCreate = async (body) => {
    const resp = await fetch(`${API}/api/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    })
    const text = await resp.text()
    let b = null
    try { b = JSON.parse(text) } catch { b = { error: text } }
    return { status: resp.status, body: b }
  }

  // createAndLose fires the create, lets the server accept it, then ABORTS
  // before reading the response — a real timeout-after-accept window.
  const createAndLose = async (body) => {
    const ctrl = new AbortController()
    const p = fetch(`${API}/api/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal: ctrl.signal,
    }).catch(() => { /* lost response */ })
    await sleep(300) // local container network: the row is committed by now
    ctrl.abort()
    await p
  }

  // --- 10a: plain upload, first create response lost -> replay the token ---
  const file10 = crypto.randomBytes(2 * CHUNK + 333_333)
  const file10Sha = sha(file10)
  const token10 = newToken()
  const meta10 = createBody('lost-create.mov', file10, file10Sha, { token: token10 })

  // Deterministic first answer (its body is treated as never reaching the
  // client, which only persisted the token).
  let r10 = await postCreate(meta10)
  check('10 first create returns 201', r10.status === 201, JSON.stringify(r10.body))
  const id10 = r10.body.session_id
  // Confirm chunk 0 AFTER the "lost" create: the replay must show current progress.
  r = await putChunk(id10, 0, slice(file10, 0))
  check('10 chunk 0 confirmed before replay', r.status === 201, JSON.stringify(r.body))

  // Replay the same token with identical metadata: original session, 200.
  r10 = await postCreate(meta10)
  check('10 replayed create returns 200', r10.status === 200, `got ${r10.status}`)
  check('10 replay returns the SAME session id', r10.body.session_id === id10,
    `got ${r10.body.session_id}, want ${id10}`)
  check('10 replay is the CURRENT snapshot (chunk 0 visible)',
    r10.body.confirmed_bytes === CHUNK && r10.body.received_count === 1 &&
    JSON.stringify(r10.body.missing_chunks) === '[1,2]',
    JSON.stringify(r10.body))
  // Exactly one chunk directory exists for this delivery: no duplicate dirs.
  check('10 replay created no second chunk directory',
    fs.existsSync(path.join(DATA_DIR, 'chunks', id10)) &&
    fs.readdirSync(path.join(DATA_DIR, 'chunks')).filter((n) => n === id10).length === 1)

  // A genuinely aborted (response-lost) create with a fresh token: its replay
  // resolves the accepted session. If the abort landed before commit, the
  // first replay is 201 and one more identical request must be 200.
  const token10b = newToken()
  const meta10b = createBody('aborted-create.mov', file10, file10Sha, { token: token10b })
  await createAndLose(meta10b)
  let rb = await postCreate(meta10b)
  let id10b
  if (rb.status === 201) {
    id10b = rb.body.session_id
    rb = await postCreate(meta10b)
  } else {
    id10b = rb.body.session_id
  }
  check('10 aborted-create replay resolves to one session (200 after acceptance)',
    rb.status === 200 && typeof id10b === 'string',
    `status=${rb.status} id=${id10b}`)
  check('10 aborted replay keeps a single chunk directory',
    fs.readdirSync(path.join(DATA_DIR, 'chunks')).filter((n) => n === id10b).length === 1)

  // Finish the first delivery through the replayed session: resume semantics hold.
  for (const idx of [1, 2]) {
    r = await putChunk(id10, idx, slice(file10, idx))
    check(`10 replayed session chunk ${idx} accepted`, r.status === 201, JSON.stringify(r.body))
  }
  r = await api(`/sessions/${id10}/assemble`, { method: 'POST' })
  check('10 replayed session assembles and completes',
    r.status === 200 && r.body.status === 'completed' && r.body.final_sha256 === file10Sha,
    JSON.stringify(r.body))
  check('10 replayed session artifact on volume', artifactFor(id10) !== null)

  // --- 10b: reuse hit whose create response was lost -> same completed session ---
  const token10r = newToken()
  const meta10r = createBody('lost-reuse.mov', file, fileSha, { reuse: true, token: token10r })
  r10 = await postCreate(meta10r)
  check('10 reuse create returns 201 completed',
    r10.status === 201 && r10.body.status === 'completed', `got ${r10.status}/${r10.body.status}`)
  const id10r = r10.body.session_id
  check('10 reuse create references scenario-1 source',
    r10.body.artifact_source === sess.session_id, `got ${r10.body.artifact_source}`)
  // Response lost -> replay: identical id, identical reuse outcome, zero chunks.
  r10 = await postCreate(meta10r)
  check('10 lost reuse replay is 200 same completed session',
    r10.status === 200 && r10.body.session_id === id10r && r10.body.status === 'completed',
    `got ${r10.status}/${r10.body.session_id}/${r10.body.status}`)
  check('10 reuse replay keeps source and zero chunks',
    r10.body.artifact_source === sess.session_id && r10.body.received_count === 0 &&
    r10.body.confirmed_bytes === 0 && r10.body.missing_chunks.length === 0,
    JSON.stringify(r10.body))
  check('10 reuse replay created no chunk directory',
    !fs.existsSync(path.join(DATA_DIR, 'chunks', id10r)))

  // --- 10c: same token with different metadata -> 409, original untouched ---
  const beforeConflict = await api(`/sessions/${id10}`)
  const conflictCases = [
    ['different filename', createBody('renamed-create.mov', file10, file10Sha, { token: token10 })],
    ['different reuse willingness', createBody('lost-create.mov', file10, file10Sha, { reuse: true, token: token10 })],
  ]
  // Different bytes/chunk-count/digest needs its own valid metadata.
  const other10 = crypto.randomBytes(CHUNK + 7)
  conflictCases.push(['different bytes/digest',
    createBody('lost-create.mov', other10, sha(other10), { token: token10 })])
  // chunk_count only (same filename/bytes/digest): for a KNOWN token this is
  // a token conflict, not the ordinary 400 count-consistency error.
  conflictCases.push(['different chunk count',
    JSON.stringify({
      filename: 'lost-create.mov', total_bytes: file10.length, chunk_count: 99,
      file_sha256: file10Sha, create_token: token10,
    })])
  for (const [name, body] of conflictCases) {
    const cc = await postCreate(body)
    check(`10 conflict (${name}) returns 409`, cc.status === 409, `got ${cc.status}`)
  }
  const afterConflict = await api(`/sessions/${id10}`)
  check('10 original session unchanged after conflicts',
    afterConflict.body.status === beforeConflict.body.status &&
    afterConflict.body.filename === beforeConflict.body.filename &&
    afterConflict.body.confirmed_bytes === beforeConflict.body.confirmed_bytes &&
    afterConflict.body.final_sha256 === beforeConflict.body.final_sha256,
    `before=${JSON.stringify(beforeConflict.body)} after=${JSON.stringify(afterConflict.body)}`)
  check('10 original completed session still downloads after conflicts',
    artifactFor(id10) !== null)

  // --- 10d: token resolves through a SEPARATE process (peer = restart semantics) ---
  if (API_PEER) {
    // Publish an artifact through the primary process under a token first.
    const file10p = crypto.randomBytes(CHUNK + 555)
    const file10pSha = sha(file10p)
    const token10p = newToken()
    const meta10p = createBody('peer-token.mov', file10p, file10pSha, { token: token10p })
    r10 = await postCreate(meta10p)
    check('10 peer scenario primary create 201', r10.status === 201, JSON.stringify(r10.body))
    const id10p = r10.body.session_id

    // Publish this delivery fully so it can back a later reuse hit.
    for (const idx of [0, 1]) {
      const up = await putChunk(id10p, idx, slice(file10p, idx))
      check(`10 peer-token source chunk ${idx} accepted`, up.status === 201, JSON.stringify(up.body))
    }
    r = await api(`/sessions/${id10p}/assemble`, { method: 'POST' })
    check('10 peer-token source published',
      r.status === 200 && r.body.status === 'completed', JSON.stringify(r.body))

    // The peer process opened the same volume independently; the token must
    // resolve to the same session there (restart-after-crash recovery).
    const peerResp = await fetch(`${API_PEER}/api/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: meta10p,
    })
    const peerBody = await peerResp.json()
    check('10 token replay through separate peer process is 200 same session',
      peerResp.status === 200 && peerBody.session_id === id10p,
      `status=${peerResp.status} id=${peerBody.session_id}, want ${id10p}`)
    // Reuse willingness is carried by the peer-resolved row too.
    check('10 peer replay keeps identical metadata',
      peerBody.filename === 'peer-token.mov' && peerBody.file_sha256 === file10pSha &&
      peerBody.total_bytes === file10p.length)

    // A token the peer has never seen in its own memory but that is in the
    // shared DB is still replayable: create through primary, replay via peer
    // WITH reuse declared against the just-published artifact.
    const token10p2 = newToken()
    const meta10p2 = createBody('peer-token-reuse.mov', file10p, file10pSha,
      { reuse: true, token: token10p2 })
    const first2 = await postCreate(meta10p2) // completed at creation via reuse
    check('10 second peer-token primary create is reuse hit',
      first2.status === 201 && first2.body.status === 'completed' &&
      first2.body.artifact_source === id10p,
      JSON.stringify(first2.body))
    const peerResp2 = await fetch(`${API_PEER}/api/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: meta10p2,
    })
    const peerBody2 = await peerResp2.json()
    check('10 reuse token replay through peer is the same completed session',
      peerResp2.status === 200 && peerBody2.session_id === first2.body.session_id &&
      peerBody2.status === 'completed' && peerBody2.artifact_source === id10p,
      `status=${peerResp2.status} body=${JSON.stringify(peerBody2)}`)
  } else {
    console.log('  (API_PEER_URL unset; skipping cross-process create-token checks)')
  }

  // --- 10e: old clients with no token still get independent sessions ---
  const legacyBody = JSON.stringify({
    filename: 'legacy-no-token.mov',
    total_bytes: file10.length,
    chunk_count: Math.ceil(file10.length / CHUNK),
    file_sha256: file10Sha,
  })
  r10 = await postCreate(legacyBody)
  check('10 tokenless create still returns 201 independent session',
    r10.status === 201 && r10.body.status === 'uploading' && !r10.body.artifact_source &&
    r10.body.session_id !== id10 && r10.body.session_id !== id10b,
    JSON.stringify(r10.body))
  check('10 tokenless create got its own chunk directory',
    fs.existsSync(path.join(DATA_DIR, 'chunks', r10.body.session_id)))

  // A brand-NEW token still obeys strict metadata validation: an
  // inconsistent chunk_count is 400 (not turned into a token conflict).
  const badCountBody = JSON.stringify({
    filename: 'bad-count.mov', total_bytes: file10.length, chunk_count: 99,
    file_sha256: file10Sha, create_token: newToken(),
  })
  r10 = await postCreate(badCountBody)
  check('10 unknown token with inconsistent chunk_count is 400',
    r10.status === 400, `got ${r10.status}`)
  // A malformed token (not 32 lowercase hex) is also 400 and creates nothing.
  const beforeMalformed = fs.readdirSync(path.join(DATA_DIR, 'chunks')).length
  r10 = await postCreate(JSON.stringify({
    filename: 'bad-token.mov', total_bytes: 10, chunk_count: 1,
    file_sha256: file10Sha, create_token: 'NOT-HEX',
  }))
  check('10 malformed create_token rejected with 400', r10.status === 400, `got ${r10.status}`)
  check('10 malformed token created no chunk directory',
    fs.readdirSync(path.join(DATA_DIR, 'chunks')).length === beforeMalformed)

  console.log(`\nverify: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
  console.log('verify: ACCEPTANCE OK')
}

main().catch((err) => {
  console.error('verify: fatal:', err)
  process.exit(1)
})
