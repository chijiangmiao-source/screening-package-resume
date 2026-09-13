import { Sha256 } from './sha256.js'

export const CHUNK_SIZE = 1048576 // 1 MiB, fixed by the delivery protocol

export function chunkCountFor(size) {
  return Math.max(1, Math.ceil(size / CHUNK_SIZE))
}

// Hash a File/Blob incrementally in slices, reporting progress 0..1.
export async function hashFile(file, onProgress) {
  const h = new Sha256()
  const slice = 8 * 1024 * 1024
  let off = 0
  while (off < file.size) {
    const buf = new Uint8Array(await file.slice(off, Math.min(off + slice, file.size)).arrayBuffer())
    h.update(buf)
    off += buf.length
    if (onProgress) onProgress(off / file.size)
    // Yield to keep the page responsive on large files.
    await new Promise((r) => setTimeout(r, 0))
  }
  return h.digestHex()
}

async function api(path, options = {}) {
  try {
    // Timeout guards against half-open connections hanging the page forever.
    const resp = await fetch(`/api${path}`, { signal: AbortSignal.timeout(60_000), ...options })
    const text = await resp.text()
    let body = null
    try { body = text ? JSON.parse(text) : null } catch { body = { error: text } }
    return { status: resp.status, body }
  } catch (err) {
    // Network drop / timeout: surfaced as status 0 so callers can resume later.
    return { status: 0, network: true, body: { error: `网络连接中断：${err.message}` } }
  }
}

export function createSession({ filename, totalBytes, chunkCount, fileSha256 }) {
  return api('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename, total_bytes: totalBytes, chunk_count: chunkCount, file_sha256: fileSha256,
    }),
  })
}

export function getSession(id) {
  return api(`/sessions/${id}`)
}

export async function uploadChunk(sessionId, index, blob) {
  const digest = await blob.arrayBuffer().then((ab) => new Sha256().update(new Uint8Array(ab)).digestHex())
  return api(`/sessions/${sessionId}/chunks/${index}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream', 'X-Chunk-SHA256': digest },
    body: blob,
  })
}

export function assemble(sessionId) {
  return api(`/sessions/${sessionId}/assemble`, { method: 'POST' })
}

// Upload every index in `indexes` with a small worker pool.
// Chunk uploads are idempotent, so network errors are retried with backoff;
// only when retries are exhausted (or the server rejects a chunk) does the
// pool abort by throwing a failure descriptor.
export async function uploadChunks(sessionId, file, indexes, { concurrency = 4, onChunk, onRetry, maxRetries = 5 } = {}) {
  let pos = 0
  let failure = null
  async function worker() {
    while (pos < indexes.length && !failure) {
      const idx = indexes[pos++]
      const start = idx * CHUNK_SIZE
      const end = Math.min(start + CHUNK_SIZE, file.size)
      const blob = file.slice(start, end)
      let result = null
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        result = await uploadChunk(sessionId, idx, blob)
        if (!result.network) break
        if (onRetry) onRetry(idx, attempt + 1)
        await new Promise((r) => setTimeout(r, Math.min(1000 * 2 ** attempt, 10_000)))
      }
      if (result.network) {
        failure = { network: true, index: idx, body: result.body }
        return
      }
      const { status, body } = result
      if (status === 200 || status === 201) {
        if (onChunk) onChunk(idx, body)
      } else {
        failure = { index: idx, status, body }
        return
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, indexes.length) }, worker))
  if (failure) throw failure
}
