// Interaction tests for artifact reuse. They mount the real App component
// against an in-memory fake of the Go API (same routes, same status codes):
// when the create call declares reuse and the server already holds a
// published artifact of identical content, the session completes at creation
// and the page must skip chunk upload and assembly and show 已复用成品;
// on a reuse miss the normal chunked upload flow must run untouched.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import App from './App.vue'

const CHUNK = 1048576

// In-memory stand-in for the Go API. `reuseHit` controls whether a create
// request that declares reuse_artifact completes immediately.
function installFakeApi({ reuseHit }) {
  const sessions = new Map()
  const calls = { chunks: 0, assemble: 0, creates: [] }

  const json = (status, obj) => new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })

  global.fetch = vi.fn(async (url, options = {}) => {
    const path = new URL(url, 'http://localhost').pathname
    const method = options.method || 'GET'
    let m

    if (path === '/api/sessions' && method === 'POST') {
      const req = JSON.parse(options.body)
      calls.creates.push(req)
      const base = {
        session_id: `sess-${sessions.size + 1}`,
        filename: req.filename,
        total_bytes: req.total_bytes,
        chunk_count: req.chunk_count,
        chunk_size: CHUNK,
        file_sha256: req.file_sha256,
        received_count: 0,
        confirmed_bytes: 0,
        final_sha256: null,
        error: null,
        artifact_source: null,
      }
      if (req.reuse_artifact && reuseHit) {
        const sess = {
          ...base,
          status: 'completed',
          missing_chunks: [],
          final_sha256: req.file_sha256,
          artifact_source: 'origin-sess-0',
        }
        sessions.set(sess.session_id, sess)
        return json(201, sess)
      }
      const sess = {
        ...base,
        status: 'uploading',
        missing_chunks: [...Array(req.chunk_count).keys()],
      }
      sessions.set(sess.session_id, sess)
      return json(201, sess)
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)$/)) && method === 'GET') {
      const sess = sessions.get(m[1])
      return sess ? json(200, sess) : json(404, { error: 'session not found' })
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/chunks\/(\d+)$/)) && method === 'POST') {
      calls.chunks++
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
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
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/download$/))) {
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      if (sess.status !== 'completed') {
        return json(409, { error: `session is ${sess.status}: artifact not published` })
      }
      return new Response(new Uint8Array([0]), {
        status: 206,
        headers: { 'Content-Range': `bytes 0-0/${sess.total_bytes}`, 'Accept-Ranges': 'bytes' },
      })
    }
    throw new Error(`unexpected fetch: ${method} ${path}`)
  })

  return { sessions, calls }
}

async function settle(rounds = 30) {
  for (let i = 0; i < rounds; i++) {
    await flushPromises()
    await new Promise((r) => setTimeout(r, 0))
  }
}

async function pickFile(wrapper, name, size) {
  const bytes = new Uint8Array(size)
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 31) & 0xff
  const f = new File([bytes], name)
  const input = wrapper.find('input[type=file]')
  Object.defineProperty(input.element, 'files', { value: [f], configurable: true })
  await input.trigger('change')
}

async function startDelivery(wrapper, name, size = CHUNK + 123) {
  await pickFile(wrapper, name, size)
  const startBtn = wrapper.findAll('button').find((b) => b.text().includes('开始交付'))
  await startBtn.trigger('click')
  await settle()
}

describe('成品复用', () => {
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
    // Close progress subscriptions / poll timers before fakes are restored.
    wrappers.forEach((w) => w.unmount())
    vi.restoreAllMocks()
  })

  it('创建时声明复用意愿，命中后跳过分块上传与组装并展示已复用成品', async () => {
    const fake = installFakeApi({ reuseHit: true })
    const wrapper = mountApp()

    await startDelivery(wrapper, '重映-正片.mov')

    // The create request declared the reuse intent.
    expect(fake.calls.creates).toHaveLength(1)
    expect(fake.calls.creates[0].reuse_artifact).toBe(true)
    // Zero chunk uploads, no assembly: the session completed at creation.
    expect(fake.calls.chunks).toBe(0)
    expect(fake.calls.assemble).toBe(0)

    // 已复用成品 is visible, the session shows completed, download entry appears.
    expect(wrapper.find('[data-test="reuse-badge"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('已复用成品')
    expect(wrapper.find('.badge').text()).toBe('completed')
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="download-area"]').text()).toContain('重映-正片.mov')
    expect(wrapper.find('[data-test="reuse-hint"]').exists()).toBe(true)

    // Nothing left to resume: no session persisted for resumption.
    expect(localStorage.getItem('delivery.session')).toBe(null)
  })

  it('复用未命中时退回普通分块上传流程，不展示已复用成品', async () => {
    const fake = installFakeApi({ reuseHit: false })
    const wrapper = mountApp()

    await startDelivery(wrapper, '首映-正片.mov')

    // Declared intent, but the miss falls back to a normal upload session.
    expect(fake.calls.creates[0].reuse_artifact).toBe(true)
    expect(fake.calls.chunks).toBe(2) // CHUNK + 123 bytes -> 2 chunks
    expect(fake.calls.assemble).toBe(1)
    expect(wrapper.find('[data-test="reuse-badge"]').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('已复用成品')
    expect(wrapper.find('.badge').text()).toBe('completed')
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
  })
})
