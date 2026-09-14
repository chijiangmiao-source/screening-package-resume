// Controllable EventSource double for jsdom tests. The real EventSource is
// not implemented in jsdom; tests install this minimal stand-in (it records
// constructed URLs and lets the test emit open/message/error events) so the
// app's SSE code path runs exactly as it would in a browser.
export class FakeEventSource {
  static instances = []

  constructor(url) {
    this.url = url
    this.readyState = FakeEventSource.CONNECTING
    this.listeners = {}
    this.closed = false
    FakeEventSource.instances.push(this)
  }

  addEventListener(type, fn) {
    ;(this.listeners[type] ||= []).push(fn)
  }

  emit(type, data) {
    for (const fn of this.listeners[type] || []) {
      fn(typeof data === 'string' || data == null ? { type, data } : { type, data: JSON.stringify(data) })
    }
  }

  close() {
    this.closed = true
    this.readyState = FakeEventSource.CLOSED
  }

  static install() {
    FakeEventSource.instances = []
    globalThis.EventSource = FakeEventSource
  }

  static uninstall() {
    FakeEventSource.closeAll()
    if (globalThis.EventSource === FakeEventSource) delete globalThis.EventSource
  }

  static closeAll() {
    for (const es of FakeEventSource.instances) es.close()
    FakeEventSource.instances = []
  }

  static get last() {
    return FakeEventSource.instances[FakeEventSource.instances.length - 1] || null
  }
}

FakeEventSource.CONNECTING = 0
FakeEventSource.OPEN = 1
FakeEventSource.CLOSED = 2
