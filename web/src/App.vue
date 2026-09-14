<script setup>
import { computed, onMounted, ref } from 'vue'
import {
  CHUNK_SIZE, chunkCountFor, hashFile,
  createSession, getSession, uploadChunks, assemble,
  downloadUrl, checkDownload,
} from './client.js'

const LS_KEY = 'delivery.session'

const file = ref(null)
const phase = ref('idle') // idle | hashing | uploading | interrupted | assembling | done | failed
const hashProgress = ref(0)
const session = ref(null)      // server session JSON
const error = ref('')
const log = ref([])
const manualId = ref('')
const saved = ref(null)        // pending session restored from localStorage
const retrying = ref(false)    // a chunk is being retried after a network drop
const downloadError = ref('')  // 409/410 reason shown inside the download area
let autoResumeTimer = null

const confirmedBytes = computed(() => session.value?.confirmed_bytes ?? 0)
const missingChunks = computed(() => session.value?.missing_chunks ?? [])
const isCompleted = computed(() => session.value?.status === 'completed')
const isReused = computed(() => !!session.value?.artifact_source)
const progressPct = computed(() => {
  if (!session.value || !session.value.total_bytes) return 0
  // A reuse hit completes with zero chunks of its own: show full progress.
  if (isReused.value) return 100
  return Math.min(100, (confirmedBytes.value / session.value.total_bytes) * 100)
})

function note(msg) {
  log.value.unshift(`[${new Date().toLocaleTimeString()}] ${msg}`)
  if (log.value.length > 60) log.value.pop()
}

function fmtBytes(n) {
  if (n == null) return '-'
  const units = ['B', 'KiB', 'MiB', 'GiB']
  let v = n, i = 0
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++ }
  return `${v.toFixed(i ? 2 : 0)} ${units[i]}`
}

onMounted(() => {
  const raw = localStorage.getItem(LS_KEY)
  if (raw) {
    try {
      saved.value = JSON.parse(raw)
      note(`发现未完成会话 ${saved.value.sessionId}，重新选择同一文件即可续传`)
    } catch { localStorage.removeItem(LS_KEY) }
  }
})

function onPick(e) {
  file.value = e.target.files[0] || null
  error.value = ''
}

async function refreshSession(id) {
  const { status, body } = await getSession(id)
  if (status === 200) {
    session.value = body
    downloadError.value = ''
    if (body.status === 'failed') { phase.value = 'failed'; error.value = body.error || '会话已冻结为 failed' }
    if (body.status === 'completed') phase.value = 'done'
    return true
  }
  error.value = body?.error || `查询会话失败（HTTP ${status}）`
  return false
}

async function start() {
  if (!file.value) return
  error.value = ''
  const f = file.value

  phase.value = 'hashing'
  hashProgress.value = 0
  const digest = await hashFile(f, (p) => { hashProgress.value = p })
  note(`整文件 SHA-256 = ${digest}`)

  // Resume path: a saved session for the same file content.
  if (saved.value && saved.value.fileSize === f.size && saved.value.fileSha256 === digest) {
    note(`续传会话 ${saved.value.sessionId}，仅补传缺块`)
    if (await refreshSession(saved.value.sessionId)) {
      if (session.value.status === 'uploading') return uploadMissing()
      return
    }
    note('原会话不可用，改为创建新会话')
    localStorage.removeItem(LS_KEY)
    saved.value = null
  }

  const chunkCount = chunkCountFor(f.size)
  const { status, body, network } = await createSession({
    filename: f.name, totalBytes: f.size, chunkCount, fileSha256: digest, reuseArtifact: true,
  })
  if (network) {
    phase.value = 'idle'
    error.value = '无法连接服务器，请检查网络后重试（文件摘要已计算，无需重新选择）'
    note('创建会话失败：网络中断')
    return
  }
  if (status !== 201) {
    phase.value = 'failed'
    error.value = body?.error || `创建会话失败（HTTP ${status}）`
    return
  }
  session.value = body
  if (body.status === 'completed') {
    // 成品复用命中：服务端已有同内容成品，零分块完成，跳过上传与组装。
    phase.value = 'done'
    localStorage.removeItem(LS_KEY)
    saved.value = null
    note(`已复用成品（来源会话 ${body.artifact_source}），无需上传 ${chunkCount} 个分块`)
    return
  }
  localStorage.setItem(LS_KEY, JSON.stringify({
    sessionId: body.session_id, fileName: f.name, fileSize: f.size, fileSha256: digest,
  }))
  note(`会话已创建：${body.session_id}（${chunkCount} 块 × 1 MiB）`)
  return uploadMissing()
}

async function resumeById() {
  const id = manualId.value.trim()
  if (!id) return
  error.value = ''
  if (await refreshSession(id)) {
    saved.value = {
      sessionId: id,
      fileName: session.value.filename,
      fileSize: session.value.total_bytes,
      fileSha256: session.value.file_sha256,
    }
    localStorage.setItem(LS_KEY, JSON.stringify(saved.value))
    note(`已载入会话 ${id}，请选择文件「${session.value.filename}」以补传缺块`)
    if (session.value.status !== 'uploading') saved.value = null
  }
}

async function uploadMissing() {
  const sess = session.value
  const missing = sess.missing_chunks
  if (missing.length === 0) return doAssemble()

  phase.value = 'uploading'
  note(`开始上传 ${missing.length} 个缺失分块：${missing.join(', ')}`)
  try {
    await uploadChunks(sess.session_id, file.value, missing, {
      concurrency: 4,
      onRetry: (idx, attempt) => {
        retrying.value = true
        note(`分块 ${idx} 网络中断，第 ${attempt} 次重试…`)
      },
      onChunk: (idx, info) => {
        retrying.value = false
        session.value = {
          ...session.value,
          confirmed_bytes: info.confirmed_bytes,
          received_count: info.received_count,
          missing_chunks: session.value.missing_chunks.filter((m) => m !== idx),
        }
        note(`分块 ${idx} 已确认${info.duplicate ? '（重复，幂等成功）' : ''}，累计 ${fmtBytes(info.confirmed_bytes)}`)
      },
    })
  } catch (fail) {
    retrying.value = false
    if (fail && fail.network) {
      // Network is down: keep the session, offer (and schedule) resumption.
      phase.value = 'interrupted'
      note(`分块 ${fail.index} 多次重试仍失败：网络未恢复，会话已保存，可续传`)
      scheduleAutoResume(sess.session_id)
      return
    }
    note(`分块 ${fail.index} 上传被拒绝（HTTP ${fail.status}）`)
    await refreshSession(sess.session_id)
    if (phase.value !== 'failed') {
      phase.value = 'failed'
      error.value = fail.body?.error || `上传失败（HTTP ${fail.status}）`
    }
    return
  }
  await refreshSession(sess.session_id)
  return doAssemble()
}

// Poll the session until the API is reachable again, then resume exactly
// where we stopped: re-query the missing list and upload only those chunks.
function scheduleAutoResume(id) {
  if (autoResumeTimer) return
  autoResumeTimer = setInterval(() => resumeNow(id), 3000)
}

async function resumeNow(id = session.value?.session_id) {
  if (!id) return
  const r = await getSession(id)
  if (r.network) return // still offline; the timer will try again
  if (autoResumeTimer) {
    clearInterval(autoResumeTimer)
    autoResumeTimer = null
  }
  session.value = r.body
  note('连接已恢复，已重新查询缺块列表')
  if (r.body.status === 'completed') {
    phase.value = 'done'
    localStorage.removeItem(LS_KEY)
    saved.value = null
  } else if (r.body.status === 'failed') {
    phase.value = 'failed'
    error.value = r.body.error || '会话已冻结为 failed'
  } else if (r.body.missing_chunks.length === 0) {
    return doAssemble()
  } else {
    return uploadMissing()
  }
}

async function doAssemble() {
  phase.value = 'assembling'
  note('全部分块就绪，请求按序组装…')
  const { status, body, network } = await assemble(session.value.session_id)
  if (network) {
    // The assemble request may or may not have reached the server; re-query
    // the session on recovery instead of guessing.
    phase.value = 'interrupted'
    note('组装请求时网络中断，恢复后将重新查询会话状态')
    scheduleAutoResume(session.value.session_id)
    return
  }
  if (status === 200) {
    session.value = body
    phase.value = 'done'
    localStorage.removeItem(LS_KEY)
    saved.value = null
    note(`发布完成，最终摘要 ${body.final_sha256}`)
  } else {
    if (body && body.session_id) session.value = body
    phase.value = 'failed'
    error.value = body?.error || `组装失败（HTTP ${status}）`
    note(`组装失败：${error.value}`)
  }
}

// Pre-flight the artifact endpoint, then hand the real download to the
// browser so Content-Disposition keeps the original filename. Failures
// (409 not published / 410 artifact gone) stay inside the download area.
async function downloadArtifact() {
  if (!session.value) return
  downloadError.value = ''
  const id = session.value.session_id
  const pre = await checkDownload(id)
  if (pre.network) {
    downloadError.value = '网络中断，无法连接服务器，请稍后重试'
    note('成品下载预检失败：网络中断')
    return
  }
  if (pre.status !== 200) {
    downloadError.value = pre.body?.error || `成品不可下载（HTTP ${pre.status}）`
    note(`成品下载被拒绝（HTTP ${pre.status}）：${downloadError.value}`)
    return
  }
  const a = document.createElement('a')
  a.href = downloadUrl(id)
  a.download = session.value.filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  note(`已开始下载成品「${session.value.filename}」（${fmtBytes(session.value.total_bytes)}）`)
}

function reset() {
  if (autoResumeTimer) {
    clearInterval(autoResumeTimer)
    autoResumeTimer = null
  }
  localStorage.removeItem(LS_KEY)
  saved.value = null
  session.value = null
  file.value = null
  phase.value = 'idle'
  error.value = ''
  downloadError.value = ''
  retrying.value = false
  log.value = []
}
</script>

<template>
  <main class="wrap">
    <h1>影展放映素材交付站</h1>
    <p class="sub">1 MiB 固定分块 · SHA-256 校验 · 断点续传 · 冲突冻结 · 原子发布</p>

    <section class="card">
      <label class="pick">
        <input type="file" @change="onPick" :disabled="phase === 'uploading' || phase === 'hashing'" />
      </label>
      <div v-if="file" class="fileinfo">
        {{ file.name }} — {{ fmtBytes(file.size) }}（{{ chunkCountFor(file.size) }} 块）
      </div>
      <div class="row">
        <button @click="start" :disabled="!file || phase === 'uploading' || phase === 'hashing' || phase === 'assembling'">
          {{ saved ? '校验并续传' : '开始交付' }}
        </button>
        <button class="ghost" @click="reset">清空</button>
      </div>
      <div v-if="phase === 'hashing'" class="bar"><i :style="{ width: (hashProgress * 100) + '%' }"></i></div>
      <div v-if="phase === 'hashing'" class="hint">计算整文件 SHA-256… {{ (hashProgress * 100).toFixed(0) }}%</div>
      <div v-if="retrying" class="hint warn">网络波动，正在自动重试当前分块…</div>
    </section>

    <section v-if="phase === 'interrupted'" class="card alert">
      <h2>连接中断，可续传</h2>
      <p class="hint">
        网络暂时断开，会话 <code>{{ session?.session_id }}</code> 已保存，已确认的字节不会丢失。
        正在每 3 秒自动重新查询缺块并续传；网络恢复后无需重新选择文件之外的任何操作。
      </p>
      <div class="row">
        <button @click="resumeNow()">立即续传</button>
      </div>
    </section>

    <section class="card">
      <h2>按会话标识续传</h2>
      <div class="row">
        <input v-model="manualId" placeholder="粘贴会话 ID" class="sid" />
        <button class="ghost" @click="resumeById">载入会话</button>
      </div>
      <div v-if="saved" class="hint">待续传会话：<code>{{ saved.sessionId }}</code>（{{ saved.fileName }}）</div>
    </section>

    <section v-if="session" class="card">
      <h2>会话状态</h2>
      <table class="kv">
        <tr><td>会话 ID</td><td><code>{{ session.session_id }}</code></td></tr>
        <tr><td>状态</td><td><span :class="['badge', session.status]">{{ session.status }}</span></td></tr>
        <template v-if="isReused">
          <tr>
            <td>成品来源</td>
            <td><span class="reuse" data-test="reuse-badge">已复用成品</span>（来源会话 <code>{{ session.artifact_source }}</code>），本次零分块完成</td>
          </tr>
        </template>
        <template v-else>
          <tr><td>已确认字节</td><td>{{ confirmedBytes }} / {{ session.total_bytes }}（{{ fmtBytes(confirmedBytes) }}）</td></tr>
          <tr><td>已确认分块</td><td>{{ session.received_count }} / {{ session.chunk_count }}</td></tr>
          <tr>
            <td>缺块列表</td>
            <td>
              <span v-if="missingChunks.length === 0">无</span>
              <span v-else class="missing">{{ missingChunks.join(', ') }}</span>
            </td>
          </tr>
        </template>
        <tr v-if="session.final_sha256"><td>完成摘要</td><td><code class="ok">{{ session.final_sha256 }}</code></td></tr>
        <tr v-if="session.error"><td>失败原因</td><td class="err">{{ session.error }}</td></tr>
      </table>
      <div class="bar"><i :class="session.status" :style="{ width: progressPct + '%' }"></i></div>
    </section>

    <section v-if="isCompleted" class="card download" data-test="download-area">
      <h2>成品下载</h2>
      <table class="kv">
        <tr><td>文件名</td><td><code>{{ session.filename }}</code></td></tr>
        <tr><td>总字节数</td><td>{{ session.total_bytes }}（{{ fmtBytes(session.total_bytes) }}）</td></tr>
      </table>
      <div class="row">
        <button data-test="download-btn" @click="downloadArtifact">下载成品</button>
      </div>
      <p v-if="isReused" class="hint reuse" data-test="reuse-hint">已复用成品：同内容素材此前已交付，本次未重新上传分块。</p>
      <p class="hint">浏览器下载将沿用原始文件名；网络中断后可从已接收位置继续，无需重新传输整个文件。</p>
      <p v-if="downloadError" class="err" data-test="download-error">{{ downloadError }}</p>
    </section>

    <p v-if="error" class="err banner">{{ error }}</p>

    <section v-if="log.length" class="card">
      <h2>事件</h2>
      <ul class="log"><li v-for="(l, i) in log" :key="i">{{ l }}</li></ul>
    </section>
  </main>
</template>

<style>
:root { color-scheme: light dark; font-family: system-ui, sans-serif; }
body { margin: 0; background: #10131a; color: #e8eaf0; }
.wrap { max-width: 760px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 22px; margin: 8px 0 2px; }
.sub { color: #8b93a7; margin-top: 0; }
.card { background: #1a1f2b; border: 1px solid #2a3142; border-radius: 10px; padding: 16px; margin: 14px 0; }
.card h2 { font-size: 15px; margin: 0 0 10px; color: #aab3c5; }
.row { display: flex; gap: 10px; margin-top: 12px; align-items: center; }
button { background: #3b82f6; color: #fff; border: 0; border-radius: 8px; padding: 9px 18px; font-size: 14px; cursor: pointer; }
button:disabled { opacity: 0.45; cursor: not-allowed; }
button.ghost { background: #2a3142; }
.fileinfo { margin-top: 10px; color: #9fd0ff; font-size: 14px; }
.hint { color: #8b93a7; font-size: 13px; margin-top: 8px; }
.sid { flex: 1; background: #10131a; color: #e8eaf0; border: 1px solid #2a3142; border-radius: 8px; padding: 8px 10px; }
.kv { width: 100%; border-collapse: collapse; font-size: 14px; }
.kv td { padding: 5px 8px; border-bottom: 1px solid #242b3a; vertical-align: top; }
.kv td:first-child { color: #8b93a7; width: 110px; }
code { background: #10131a; padding: 1px 6px; border-radius: 5px; font-size: 12.5px; word-break: break-all; }
code.ok { color: #4ade80; }
.badge { padding: 2px 10px; border-radius: 999px; font-size: 12.5px; background: #2a3142; }
.badge.uploading { background: #1d4ed8; }
.badge.completed { background: #15803d; }
.badge.failed { background: #b91c1c; }
.missing { color: #fbbf24; font-family: ui-monospace, monospace; font-size: 12.5px; word-break: break-all; }
.reuse { color: #4ade80; }
.err { color: #f87171; }
.banner { background: #2b1518; border: 1px solid #7f1d1d; padding: 10px 14px; border-radius: 8px; }
.card.alert { border-color: #b45309; background: #241a10; }
.card.alert h2 { color: #fbbf24; }
.card.download { border-color: #15803d; }
.card.download h2 { color: #4ade80; }
.warn { color: #fbbf24; }
.bar { height: 8px; background: #10131a; border-radius: 999px; margin-top: 12px; overflow: hidden; }
.bar i { display: block; height: 100%; background: #3b82f6; transition: width 0.2s; }
.bar i.completed { background: #22c55e; }
.bar i.failed { background: #ef4444; }
.log { margin: 0; padding-left: 18px; font-size: 13px; color: #aab3c5; max-height: 220px; overflow: auto; }
</style>
