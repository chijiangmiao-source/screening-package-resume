// One-shot acceptance suite. Runs against the REAL api service over the
// compose network (no stubs, no mocks) and inspects the shared data volume
// to confirm artifact visibility. Exits non-zero on any failure.
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'

const API = process.env.API_URL || 'http://api:8080'
const DATA_DIR = process.env.DATA_DIR || '/data'
const CHUNK = 1048576

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex')
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

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

  console.log(`\nverify: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
  console.log('verify: ACCEPTANCE OK')
}

main().catch((err) => {
  console.error('verify: fatal:', err)
  process.exit(1)
})
