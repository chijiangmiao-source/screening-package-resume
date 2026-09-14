// Interaction tests for the live progress event stream. They mount the real
// App component against the same in-memory fake API used by the other App
// tests, plus a controllable EventSource double (jsdom has none):
//   - subscription first receives a complete snapshot;
//   - server-side progress arrives as numbered snapshot/update events and
//     refreshes the existing progress area (including another workstation
//     finishing the publish);
//   - a dropped link shows 实时更新已中断 and reconnects carrying the last
//     sequence, without changing upload controls or phase;
//   - when EventSource is unavailable the page keeps polling every 3s.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import App from './App.vue'
import { FakeEventSource } from './eventsource-mock.js'

const CHUNK = 1048576

function installFakeApi() {
  const sessions = new Map()

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
        artifact_source: null,
      }
      sessions.set(sess.session_id, sess)
      return json(201, sess)
    }
    if ((m = path.match(/^\/api\/sessions\/([\w-]+)$/)) && method === 'GET') {
      const sess = sessions.get(m[1])
      return sess ? json(200, sess) : json(404, { error: 'session not found' })
    }
    // EventSource is faked, so the API double must never be asked for /events.
    if (path.includes('/events')) {
      throw new Error(`test fake does not serve SSE over fetch: ${path}`)
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
      if (sess.status !== 'completed') return json(409, { error: 'not published' })
      return new Response(new Uint8Array([0]), {
        status: 206,
        headers: { 'Content-Range': `bytes 0-0/${sess.total_bytes}`, 'Accept-Ranges': 'bytes' },
      })
    }
    throw new Error(`unexpected fetch: ${method} ${path}`)
  })

  return { sessions }
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

async function startDelivery(wrapper, name = 'festival-reel.mov', size = CHUNK + 123) {
  await pickFile(wrapper, name, size)
  const startBtn = wrapper.findAll('button').find((b) => b.text().includes('开始交付'))
  await startBtn.trigger('click')
  await settle()
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

describe('会话进度事件流', () => {
  const wrappers = []
  const mountApp = () => {
    const w = mount(App)
    wrappers.push(w)
    return w
  }

  beforeEach(() => {
    localStorage.clear()
    wrappers.length = 0
    installFakeApi()
    FakeEventSource.install()
  })

  afterEach(() => {
    // Close progress subscriptions / reconnect timers before fakes go away.
    wrappers.forEach((w) => w.unmount())
    FakeEventSource.uninstall()
    vi.restoreAllMocks()
  })

  it('订阅首先收到完整快照，并进入实时状态', async () => {
    const wrapper = mountApp()
    await startDelivery(wrapper)

    // Exactly one EventSource per session, opened against the events route.
    expect(FakeEventSource.instances.length).toBe(1)
    expect(FakeEventSource.last.url).toMatch(/^\/api\/sessions\/sess-1\/events$/)
    const es = FakeEventSource.last
    es.emit('open')
    await settle()
    expect(wrapper.find('[data-test="stream-live"]').exists()).toBe(true)

    // A server snapshot carrying full state refreshes the progress area even
    // though no upload response produced it locally.
    es.emit('snapshot', {
      type: 'snapshot', seq: 1,
      session: {
        session_id: 'sess-1', filename: 'festival-reel.mov', total_bytes: CHUNK + 123,
        chunk_count: 2, chunk_size: CHUNK, file_sha256: 'a'.repeat(64),
        status: 'uploading', received_count: 1, confirmed_bytes: CHUNK,
        missing_chunks: [1], final_sha256: null, error: null, artifact_source: null,
      },
    })
    await settle()
    expect(wrapper.text()).toContain(`${CHUNK}`)
    expect(wrapper.find('.missing').text()).toBe('1')
  })

  it('另一工作站完成发布时，update 事件直接刷新出完成状态与下载区', async () => {
    const wrapper = mountApp()
    await startDelivery(wrapper)
    const es = FakeEventSource.last
    es.emit('open')

    // The other technician finishes assembly; this client only receives the
    // numbered update event (it never POSTs assemble itself here).
    es.emit('update', {
      type: 'update', seq: 4,
      session: {
        session_id: 'sess-1', filename: 'festival-reel.mov', total_bytes: CHUNK + 123,
        chunk_count: 2, chunk_size: CHUNK, file_sha256: 'a'.repeat(64),
        status: 'completed', received_count: 2, confirmed_bytes: CHUNK + 123,
        missing_chunks: [], final_sha256: 'a'.repeat(64), error: null, artifact_source: null,
      },
    })
    await settle()
    expect(wrapper.find('.badge.completed').text()).toBe('completed')
    expect(wrapper.find('[data-test="download-area"]').exists()).toBe(true)
    // Upload controls were not altered into an interrupted/error phase.
    expect(wrapper.find('[data-test="stream-interrupted"]').exists()).toBe(false)
  })

  it('连接暂断显示“实时更新已中断”，重连携带最后序号且不改变上传控制', async () => {
    const wrapper = mountApp()
    await startDelivery(wrapper)
    const es = FakeEventSource.last
    es.emit('open')
    es.emit('update', {
      type: 'update', seq: 3,
      session: {
        session_id: 'sess-1', filename: 'festival-reel.mov', total_bytes: CHUNK + 123,
        chunk_count: 2, chunk_size: CHUNK, file_sha256: 'a'.repeat(64),
        status: 'uploading', received_count: 2, confirmed_bytes: CHUNK + 123,
        missing_chunks: [], final_sha256: null, error: null, artifact_source: null,
      },
    })
    await settle()

    es.emit('error')
    await settle()
    const notice = wrapper.find('[data-test="stream-interrupted"]')
    expect(notice.exists()).toBe(true)
    expect(notice.text()).toContain('实时更新已中断')
    // The old connection was closed before reconnecting.
    expect(es.closed).toBe(true)
    // Buttons are not disabled by the stream drop.
    const buttons = wrapper.findAll('button').map((b) => b.attributes('disabled'))
    expect(buttons.every((d) => d === undefined)).toBe(true)

    // Automatic reconnect after the backoff, carrying the last sequence.
    await sleep(2200)
    await settle()
    const reconnected = FakeEventSource.instances[FakeEventSource.instances.length - 1]
    expect(reconnected).not.toBe(es)
    expect(reconnected.url).toContain('?after=3')
    reconnected.emit('open')
    await settle()
    expect(wrapper.find('[data-test="stream-interrupted"]').exists()).toBe(false)
    expect(wrapper.find('[data-test="stream-live"]').exists()).toBe(true)
  }, 10000)

  it('浏览器不支持事件流时继续按 3 秒轮询刷新进度区', async () => {
    FakeEventSource.uninstall() // no EventSource in this environment
    const fake = installFakeApi()
    const wrapper = mountApp()
    await startDelivery(wrapper)

    // Polling fallback: the session GET refreshed the area immediately and
    // the page explains why streaming is unavailable.
    await settle()
    expect(wrapper.find('[data-test="stream-poll"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="stream-live"]').exists()).toBe(false)
    const getCalls = global.fetch.mock.calls
      .filter(([u, o]) => new URL(u, 'http://localhost').pathname === '/api/sessions/sess-1'
        && (!o.method || o.method === 'GET'))
    expect(getCalls.length).toBeGreaterThanOrEqual(1)

    // A later poll reflecting server-side progress refreshes the area too.
    const sess = fake.sessions.get('sess-1')
    sess.received_count = 1
    sess.confirmed_bytes = CHUNK
    sess.missing_chunks = [1]
    await sleep(3100) // one 3s poll tick
    await settle()
    expect(wrapper.text()).toContain(`${CHUNK}`)
    expect(wrapper.find('.missing').text()).toBe('1')
  }, 10000)
})
