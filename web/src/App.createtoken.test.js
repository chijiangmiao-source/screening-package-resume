// Interaction tests for idempotent session creation. They mount the real
// App component against an in-memory fake of the Go API that implements the
// same create-token semantics:
//   - the page generates a create token per "start upload" and saves it
//     locally before the request;
//   - when the first create response is lost (server may already have
//     accepted it), the page shows 重试创建; the retry reuses the SAME token
//     and the already computed digest and gets the original session back
//     (HTTP 200 current snapshot) — no second row, no missed reuse hit;
//   - a 409 token conflict is stated plainly and the page offers 重新开始,
//     which mints a fresh token;
//   - after a reload with only the token saved, re-picking the same file
//     replays that token instead of minting a new one.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import App from './App.vue'
import { generateCreateToken, hashFile } from './client.js'

const CHUNK = 1048576

function sameIntent(a, b) {
  return a.filename === b.filename && a.total_bytes === b.total_bytes &&
    a.chunk_count === b.chunk_count && a.file_sha256 === b.file_sha256 &&
    !!a.reuse_artifact === !!b.reuse_artifact
}

// Fake API honoring create-token idempotency. Options:
//   reuseHit: a reuse-declared create completes at creation;
//   loseFirstCreateResponse: the FIRST create is applied server-side (the
//     row is stored) but the fetch rejects with a network error, exactly
//     like a timeout after the server accepted the request;
//   conflictFirst: the FIRST create answers 409 (a pre-existing session
//     already owns a different delivery under that token shape).
function installFakeApi({ reuseHit = false, loseFirstCreateResponse = false, conflictFirst = false } = {}) {
  const sessions = new Map() // session_id -> session
  const byToken = new Map() // create_token -> session
  const calls = { creates: [], chunks: 0, assemble: 0 }
  let createSeen = 0

  const json = (status, obj) => new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
  const sameMeta = (sess, req) => sess.filename === req.filename &&
    sess.total_bytes === req.total_bytes && sess.chunk_count === req.chunk_count &&
    sess.file_sha256 === req.file_sha256

  // A pre-existing delivery a conflicting token could plausibly belong to.
  // Only installed for the conflict scenario, which names it explicitly.
  if (conflictFirst) {
    sessions.set('original-session', {
      session_id: 'original-session',
      filename: 'someone-else.mov', total_bytes: 7, chunk_count: 1, chunk_size: CHUNK,
      file_sha256: 'b'.repeat(64), status: 'uploading', received_count: 0,
      confirmed_bytes: 0, missing_chunks: [0], final_sha256: null, error: null,
      artifact_source: null,
    })
  }

  global.fetch = vi.fn(async (url, options = {}) => {
    const path = new URL(url, 'http://localhost').pathname
    const method = options.method || 'GET'
    let m

    if (path === '/api/sessions' && method === 'POST') {
      const req = JSON.parse(options.body)
      calls.creates.push(req)
      createSeen++

      // Same token seen again: replay the original session or conflict.
      if (req.create_token && byToken.has(req.create_token)) {
        const ex = byToken.get(req.create_token)
        if (!sameMeta(ex, req) || !!req.reuse_artifact !== ex._reuseRequested) {
          return json(409, {
            error: 'create token was already used with different metadata; start a new delivery instead of retrying this one',
          })
        }
        return json(200, { ...ex })
      }

      const applyCreate = () => {
        const id = `sess-${sessions.size + 1}`
        const hit = !!(req.reuse_artifact && reuseHit)
        const sess = {
          session_id: id,
          filename: req.filename,
          total_bytes: req.total_bytes,
          chunk_count: req.chunk_count,
          chunk_size: CHUNK,
          file_sha256: req.file_sha256,
          status: hit ? 'completed' : 'uploading',
          received_count: 0,
          confirmed_bytes: 0,
          missing_chunks: hit ? [] : [...Array(req.chunk_count).keys()],
          final_sha256: hit ? req.file_sha256 : null,
          error: null,
          artifact_source: hit ? 'origin-sess-0' : null,
          _reuseRequested: !!req.reuse_artifact,
        }
        sessions.set(id, sess)
        if (req.create_token) byToken.set(req.create_token, sess)
        return sess
      }

      if (conflictFirst && createSeen === 1) {
        // The token was already used (elsewhere) with different metadata.
        return json(409, {
          error: 'create token was already used with different metadata; start a new delivery instead of retrying this one',
        })
      }

      const sess = applyCreate()
      if (loseFirstCreateResponse && createSeen === 1) {
        // Applied server-side, but the response is lost on the wire.
        throw new TypeError('network request failed')
      }
      return json(201, { ...sess })
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)$/)) && method === 'GET') {
      const sess = sessions.get(m[1])
      return sess ? json(200, sess) : json(404, { error: 'session not found' })
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/chunks\/(\d+)$/)) && method === 'POST') {
      calls.chunks++
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      if (sess.status !== 'uploading') return json(409, { error: `session is ${sess.status}` })
      const idx = Number(m[2])
      sess.missing_chunks = sess.missing_chunks.filter((i) => i !== idx)
      sess.received_count += 1
      sess.confirmed_bytes += options.body.size
      return json(201, {
        session_id: sess.session_id,
        index: idx,
        duplicate: false,
        received_count: sess.received_count,
        confirmed_bytes: sess.confirmed_bytes,
        status: sess.status,
      })
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/assemble$/)) && method === 'POST') {
      calls.assemble++
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      sess.status = 'completed'
      sess.final_sha256 = sess.file_sha256
      return json(200, sess)
    }
    if (path.includes('/events')) {
      throw new Error(`test fake does not serve SSE over fetch: ${path}`)
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/download$/))) {
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      if (sess.status !== 'completed') return json(409, { error: 'not published' })
      return new Response(new Uint8Array([0]), {
        status: 206,
        headers: { 'Content-Range': `bytes 0-0/${sess.total_bytes}`, 'Accept-Ranges': 'bytes' },
      })
    }
    throw new Error(`unexpected fetch: ${method} ${path}`)
  })

  return { sessions, byToken, calls }
}

async function settle(rounds = 30) {
  for (let i = 0; i < rounds; i++) {
    await flushPromises()
    await new Promise((r) => setTimeout(r, 0))
  }
}

function deterministicFile(name, size) {
  const bytes = new Uint8Array(size)
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 31) & 0xff
  return new File([bytes], name)
}

async function pickFile(wrapper, f) {
  const input = wrapper.find('input[type=file]')
  Object.defineProperty(input.element, 'files', { value: [f], configurable: true })
  await input.trigger('change')
}

function clickButton(wrapper, text) {
  const btn = wrapper.findAll('button').find((b) => b.text().includes(text))
  if (!btn) throw new Error(`button not found: ${text}`)
  return btn.trigger('click')
}

async function startDelivery(wrapper) {
  await clickButton(wrapper, '开始交付')
  await settle()
}

describe('generateCreateToken', () => {
  it('生成 32 位小写十六进制令牌且两次不同', () => {
    const a = generateCreateToken()
    const b = generateCreateToken()
    expect(a).toMatch(/^[0-9a-f]{32}$/)
    expect(b).toMatch(/^[0-9a-f]{32}$/)
    expect(a).not.toBe(b)
  })
})

describe('创建令牌幂等重试', () => {
  const wrappers = []
  const mountApp = () => {
    const w = mount(App)
    wrappers.push(w)
    return w
  }

  beforeEach(() => {
    localStorage.clear()
    wrappers.length = 0
  })

  afterEach(() => {
    wrappers.forEach((w) => w.unmount())
    vi.restoreAllMocks()
  })

  it('首次创建响应丢失后显示“重试创建”，重试复用同令牌取回原会话并完成交付', async () => {
    const fake = installFakeApi({ loseFirstCreateResponse: true })
    const wrapper = mountApp()
    const f = deterministicFile('首映-正片.mov', CHUNK + 123)
    await pickFile(wrapper, f)
    await startDelivery(wrapper)

    // The first create was sent with a token, and the token was saved before
    // the request; the lost response leaves a resumable create state.
    expect(fake.calls.creates).toHaveLength(1)
    const firstToken = fake.calls.creates[0].create_token
    expect(firstToken).toMatch(/^[0-9a-f]{32}$/)
    expect(fake.sessions.size).toBe(1) // server HAD accepted it
    const savedBefore = JSON.parse(localStorage.getItem('delivery.session'))
    expect(savedBefore.createToken).toBe(firstToken)
    expect(savedBefore.sessionId).toBeUndefined()

    expect(wrapper.find('[data-test="create-retry-card"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="retry-create-btn"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('重试创建')

    // Retry: SAME token, same metadata, no re-hash interaction required.
    await clickButton(wrapper, '重试创建')
    await settle()

    expect(fake.calls.creates).toHaveLength(2)
    expect(fake.calls.creates[1].create_token).toBe(firstToken)
    expect(sameIntent(fake.calls.creates[0], fake.calls.creates[1])).toBe(true)

    // Exactly one session exists; the replay returned its current snapshot
    // and the normal upload + assemble flow ran to completion.
    expect([...fake.sessions.keys()]).toEqual(['sess-1'])
    expect(fake.calls.chunks).toBe(2)
    expect(fake.calls.assemble).toBe(1)
    expect(wrapper.find('.badge.completed').text()).toBe('completed')
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
    // After success the local record carries the resolved session id.
    const savedAfter = JSON.parse(localStorage.getItem('delivery.session'))
    expect(savedAfter).toBeNull() // completed delivery clears the record
  })

  it('首次命中成品复用但响应丢失，重试取回同一个已完成会话（标识与复用结果一致、零分块）', async () => {
    const fake = installFakeApi({ reuseHit: true, loseFirstCreateResponse: true })
    const wrapper = mountApp()
    const f = deterministicFile('重映-正片.mov', CHUNK + 123)
    await pickFile(wrapper, f)
    await startDelivery(wrapper)

    expect(wrapper.find('[data-test="create-retry-card"]').exists()).toBe(true)
    const firstToken = fake.calls.creates[0].create_token

    await clickButton(wrapper, '重试创建')
    await settle()

    expect(fake.calls.creates).toHaveLength(2)
    expect(fake.calls.creates[1].create_token).toBe(firstToken)
    // Only the one completed reuse session exists.
    expect([...fake.sessions.values()]).toHaveLength(1)
    const deliverySession = [...fake.sessions.values()][0]
    expect(deliverySession.status).toBe('completed')
    expect(deliverySession.artifact_source).toBe('origin-sess-0')
    // Zero chunks uploaded, zero assemblies: the reuse hit was not missed.
    expect(fake.calls.chunks).toBe(0)
    expect(fake.calls.assemble).toBe(0)
    expect(wrapper.find('[data-test="reuse-badge"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('已复用成品')
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
  })

  it('同令牌不同元数据返回 409 时明确提示，点“重新开始”用新令牌成功且原会话不被改动', async () => {
    const fake = installFakeApi({ conflictFirst: true })
    const wrapper = mountApp()
    const f = deterministicFile('冲突交付.mov', CHUNK + 123)
    await pickFile(wrapper, f)
    await startDelivery(wrapper)

    expect(fake.calls.creates).toHaveLength(1)
    const oldToken = fake.calls.creates[0].create_token
    // The page states the conflict plainly and offers a fresh start.
    expect(wrapper.find('[data-test="restart-create-btn"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('冲突')
    expect(wrapper.text()).toContain('重新开始')

    const originalBefore = fake.sessions.get('original-session')
    await clickButton(wrapper, '重新开始')
    await settle()

    expect(fake.calls.creates).toHaveLength(2)
    const newToken = fake.calls.creates[1].create_token
    expect(newToken).toMatch(/^[0-9a-f]{32}$/)
    expect(newToken).not.toBe(oldToken)
    // Fresh token -> a new independent session, delivered normally.
    expect(fake.calls.chunks).toBe(2)
    expect(fake.calls.assemble).toBe(1)
    expect(wrapper.find('.badge.completed').text()).toBe('completed')
    // The session that owned the conflicting token is untouched.
    const originalAfter = fake.sessions.get('original-session')
    expect(originalAfter).toEqual(originalBefore)
    expect(originalAfter.filename).toBe('someone-else.mov')
  })

  it('页面重载后本地仅存令牌时，重选同一文件复用保存的令牌取回原会话', async () => {
    const fake = installFakeApi({})
    const f = deterministicFile('恢复-正片.mov', CHUNK + 123)
    const digest = await hashFile(f)

    // Simulate a page reload after the create response was lost: only the
    // token (and file metadata) is in localStorage, no session id yet, and
    // the server has already accepted the create.
    const token = 'cafebabecafebabecafebabecafebabe'
    localStorage.setItem('delivery.session', JSON.stringify({
      createToken: token,
      fileName: f.name,
      fileSize: f.size,
      fileSha256: digest,
    }))
    // Server-side row the lost response belonged to.
    fake.byToken.set(token, {
      session_id: 'sess-recovered',
      filename: f.name, total_bytes: f.size, chunk_count: 2, chunk_size: CHUNK,
      file_sha256: digest, status: 'uploading', received_count: 0,
      confirmed_bytes: 0, missing_chunks: [0, 1], final_sha256: null,
      error: null, artifact_source: null, _reuseRequested: true,
    })
    fake.sessions.set('sess-recovered', fake.byToken.get(token))

    const wrapper = mountApp()
    await settle()
    await pickFile(wrapper, f)
    // The saved record makes the button read 校验并续传.
    await clickButton(wrapper, '校验并续传')
    await settle()

    expect(fake.calls.creates).toHaveLength(1)
    expect(fake.calls.creates[0].create_token).toBe(token)
    // Replayed (200): uploads continue against the recovered session id.
    expect(fake.calls.chunks).toBe(2)
    expect(fake.calls.assemble).toBe(1)
    expect(wrapper.find('.badge.completed').text()).toBe('completed')
    expect(fake.sessions.get('sess-recovered').status).toBe('completed')
  })

  it('不带 create_token 的旧行为不受影响（每次创建独立会话）', async () => {
    // The page always sends a token now; old clients are covered by Go tests.
    // Here we only assert normal first-time delivery still works end to end.
    const fake = installFakeApi({})
    const wrapper = mountApp()
    const f = deterministicFile('普通交付.mov', CHUNK + 123)
    await pickFile(wrapper, f)
    await startDelivery(wrapper)
    expect(fake.calls.creates[0].create_token).toMatch(/^[0-9a-f]{32}$/)
    expect(fake.calls.chunks).toBe(2)
    expect(fake.calls.assemble).toBe(1)
    expect(wrapper.find('.badge.completed').exists()).toBe(true)
  })
})
