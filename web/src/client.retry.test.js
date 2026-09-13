// Unit tests for the upload client's retry accounting. A chunk upload is
// one initial attempt plus at most `maxRetries` automatic retries; when the
// network stays down the pool stops after exactly that many retries and
// surfaces a network failure so the page enters its resumable "interrupted"
// state instead of logging an extra retry.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { uploadChunks } from './client.js'

const CHUNK = 1048576

function fakeFile(size = CHUNK + 1) {
  const bytes = new Uint8Array(size)
  for (let i = 0; i < size; i++) bytes[i] = i & 0xff
  return new File([bytes], 'festival-reel.mov')
}

describe('uploadChunks 断网重试上限', () => {
  let setTimeoutSpy

  beforeEach(() => {
    // Collapse exponential backoff so the test stays fast.
    setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn) => {
      fn()
      return 0
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('maxRetries=2 时仅记录两次重试，随后抛出网络失败', async () => {
    global.fetch = vi.fn(async () => {
      throw new TypeError('network request failed')
    })
    const retries = []
    let failure
    try {
      await uploadChunks('sess-1', fakeFile(), [0], {
        maxRetries: 2,
        onRetry: (idx, attempt) => retries.push({ idx, attempt }),
      })
    } catch (e) {
      failure = e
    }

    // Initial attempt + 2 retries = 3 requests — never 4.
    expect(global.fetch).toHaveBeenCalledTimes(3)
    expect(retries).toEqual([{ idx: 0, attempt: 1 }, { idx: 0, attempt: 2 }])
    expect(failure).toMatchObject({ network: true, index: 0 })
  })

  it('maxRetries=0 时不重试，仅初次尝试即中断', async () => {
    global.fetch = vi.fn(async () => {
      throw new TypeError('network request failed')
    })
    const retries = []
    await expect(
      uploadChunks('sess-2', fakeFile(), [0], {
        maxRetries: 0,
        onRetry: (idx, attempt) => retries.push(attempt),
      }),
    ).rejects.toMatchObject({ network: true })
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(retries).toEqual([])
  })

  it('默认上限 5 次时共发出 6 次请求', async () => {
    global.fetch = vi.fn(async () => {
      throw new TypeError('network request failed')
    })
    await expect(
      uploadChunks('sess-3', fakeFile(), [0], { onRetry: () => {} }),
    ).rejects.toMatchObject({ network: true })
    expect(global.fetch).toHaveBeenCalledTimes(6)
  })
})
