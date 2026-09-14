// Interaction tests for the artifact download entry. They mount the real
// App component and drive it against an in-memory fake of the Go API
// (same routes, same status codes), covering: completed session shows
// filename / total bytes / download button; successful pre-flight hands
// the download to the browser; 409 and 410 reasons surface inside the
// download area; the original upload -> assemble flow is untouched.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import App from './App.vue'

const CHUNK = 1048576

// In-memory stand-in for the Go API, speaking the same HTTP contract.
function installFakeApi() {
  const sessions = new Map()
  const downloadOverride = new Map() // session_id -> { status, body }

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
      const sess = {
        session_id: `sess-${sessions.size + 1}`,
        filename: req.filename,
        total_bytes: req.total_bytes,
        chunk_count: req.chunk_count,
        chunk_size: CHUNK,
        file_sha256: req.file_sha256,
        status: 'uploading',
        received_count: 0,
        confirmed_bytes: 0,
        missing_chunks: [...Array(req.chunk_count).keys()],
        final_sha256: null,
        error: null,
      }
      sessions.set(sess.session_id, sess)
      return json(201, sess)
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)$/)) && method === 'GET') {
      const sess = sessions.get(m[1])
      return sess ? json(200, sess) : json(404, { error: 'session not found' })
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/chunks\/(\d+)$/)) && method === 'POST') {
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
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      sess.status = 'completed'
      sess.final_sha256 = sess.file_sha256
      return json(200, sess)
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)\/download$/))) {
      const sess = sessions.get(m[1])
      if (!sess) return json(404, { error: 'session not found' })
      const ov = downloadOverride.get(m[1])
      if (ov) return json(ov.status, ov.body)
      if (sess.status !== 'completed') {
        return json(409, { error: `session is ${sess.status}: artifact not published` })
      }
      return new Response(new Uint8Array([0]), {
        status: 206,
        headers: {
          'Content-Range': `bytes 0-0/${sess.total_bytes}`,
          'Accept-Ranges': 'bytes',
        },
      })
    }
    throw new Error(`unexpected fetch: ${method} ${path}`)
  })

  return { sessions, downloadOverride }
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

// Drive the full delivery flow: pick file -> hash -> create -> upload -> assemble.
async function completeDelivery(wrapper, name = 'festival-reel.mov', size = CHUNK + 123) {
  await pickFile(wrapper, name, size)
  const startBtn = wrapper.findAll('button').find((b) => b.text().includes('开始交付'))
  await startBtn.trigger('click')
  await settle()
}

describe('成品下载入口', () => {
  let fake
  let clickSpy
  const wrappers = []
  const mountApp = () => {
    const w = mount(App)
    wrappers.push(w)
    return w
  }

  beforeEach(() => {
    localStorage.clear()
    wrappers.length = 0
    fake = installFakeApi()
    // Keep jsdom from attempting real navigation on the download anchor.
    clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
  })

  afterEach(() => {
    // Close any progress subscriptions / poll timers before fakes are restored.
    wrappers.forEach((w) => w.unmount())
    clickSpy.mockRestore()
    vi.restoreAllMocks()
  })

  it('completed 后显示文件名、总字节数与下载按钮，点击后交给浏览器下载', async () => {
    const wrapper = mountApp()
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(false)

    await completeDelivery(wrapper)

    const area = wrapper.find('[data-test="download-area"]')
    expect(area.exists()).toBe(true)
    expect(area.text()).toContain('festival-reel.mov')
    expect(area.text()).toContain(String(CHUNK + 123))
    expect(wrapper.find('[data-test="download-error"]').exists()).toBe(false)

    await wrapper.find('[data-test="download-btn"]').trigger('click')
    await settle()

    // Pre-flight hit the download endpoint with a single-byte range...
    const preflight = global.fetch.mock.calls.find(([u]) => u.includes('/download'))
    expect(preflight).toBeTruthy()
    expect(preflight[1].headers.Range).toBe('bytes=0-0')
    // ...then the browser was handed the real download URL with the filename.
    expect(clickSpy).toHaveBeenCalledTimes(1)
    expect(wrapper.find('[data-test="download-error"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('已开始下载成品')
  })

  it('上传中会话请求下载返回 409，原因显示在下载区域', async () => {
    const wrapper = mountApp()
    await completeDelivery(wrapper)
    const id = [...fake.sessions.keys()][0]

    // Server-side the session is no longer completed (e.g. rolled back).
    fake.sessions.get(id).status = 'uploading'
    await wrapper.find('[data-test="download-btn"]').trigger('click')
    await settle()

    const err = wrapper.find('[data-test="download-error"]')
    expect(err.exists()).toBe(true)
    expect(err.text()).toContain('artifact not published')
    expect(clickSpy).not.toHaveBeenCalled()
    // Download area stays put; the rest of the page is unaffected.
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
  })

  it('成品缺失返回 410，原因显示在下载区域', async () => {
    const wrapper = mountApp()
    await completeDelivery(wrapper)
    const id = [...fake.sessions.keys()][0]

    fake.downloadOverride.set(id, { status: 410, body: { error: 'artifact missing on storage volume' } })
    await wrapper.find('[data-test="download-btn"]').trigger('click')
    await settle()

    const err = wrapper.find('[data-test="download-error"]')
    expect(err.exists()).toBe(true)
    expect(err.text()).toContain('artifact missing on storage volume')
    expect(clickSpy).not.toHaveBeenCalled()
  })

  it('按会话标识查询到 completed 的旧会话同样出现下载入口', async () => {
    fake.sessions.set('old-session-1', {
      session_id: 'old-session-1',
      filename: '往届展映.mov',
      total_bytes: 2 * CHUNK + 5,
      chunk_count: 3,
      chunk_size: CHUNK,
      file_sha256: 'a'.repeat(64),
      status: 'completed',
      received_count: 3,
      confirmed_bytes: 2 * CHUNK + 5,
      missing_chunks: [],
      final_sha256: 'a'.repeat(64),
      error: null,
    })

    const wrapper = mountApp()
    await wrapper.find('input.sid').setValue('old-session-1')
    const loadBtn = wrapper.findAll('button').find((b) => b.text() === '载入会话')
    await loadBtn.trigger('click')
    await settle()

    const area = wrapper.find('[data-test="download-area"]')
    expect(area.exists()).toBe(true)
    expect(area.text()).toContain('往届展映.mov')
    expect(area.text()).toContain(String(2 * CHUNK + 5))
  })

  it('上传中的会话不显示下载入口，上传/补传流程不受影响', async () => {
    const wrapper = mountApp()
    await pickFile(wrapper, 'partial.mov', CHUNK + 10)

    // Fake a network drop on the second chunk to leave the session uploading.
    const realFetch = global.fetch
    let dropped = false
    global.fetch = vi.fn(async (url, options = {}) => {
      if (!dropped && url.includes('/chunks/1')) {
        dropped = true
        throw new TypeError('fetch failed')
      }
      return realFetch(url, options)
    })

    const startBtn = wrapper.findAll('button').find((b) => b.text().includes('开始交付'))
    await startBtn.trigger('click')
    await settle(60) // let the retry backoff exhaust quickly is not feasible; stop early

    // The session is mid-upload (or interrupted): no download entry yet.
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('分块')
  }, 15000)
})
