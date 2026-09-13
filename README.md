# 影展放映素材交付站

面向影展高码率放映素材的断点续传交付系统：Vue 单页前端按 **固定 1 MiB（1048576 字节）** 分块上传，Go API 用 **SQLite** 记录会话与已确认分块元数据，分块内容持久化到本地数据卷，全部分块到齐后按序组装、整文件 SHA-256 校验通过才以**原子改名**发布成品。

## 快速开始

```bash
docker compose up --build -d        # 启动 web + api
docker compose up verify            # 运行一次性验收服务（真实联调，非假接口）
docker compose up --exit-code-from verify verify   # 需要退出码时
```

- Web 页面：`http://localhost:${WEB_PORT:-8081}`
- API：`http://localhost:${API_PORT:-8080}`（如 `curl localhost:8080/api/health`）
- 宿主端口可覆盖：`WEB_PORT=9000 API_PORT=9001 docker compose up --build -d`

数据保存在名为 `delivery-data` 的卷中（SQLite 库、分块文件、成品 artifacts），删除卷即清空全部状态。

## 交付协议

1. **创建会话** `POST /api/sessions`
   提交 `filename`、`total_bytes`、`chunk_count`、`file_sha256`（整文件 SHA-256，64 位小写十六进制）。
   `chunk_count` 必须与 `ceil(total_bytes / 1048576)` 一致，否则 400。
2. **上传分块** `POST /api/sessions/{id}/chunks/{index}`
   原始字节 + `X-Chunk-SHA256` 头（小写十六进制）。除最后一块外每块必须**恰好 1048576 字节**，
   最后一块长度由总字节数唯一确定（`total_bytes - 1048576 × (chunk_count - 1)`），不符即 400。
   - 同序号同摘要重复提交 → `200`，幂等成功，**不重复计数**；
   - 同序号不同内容 → `409`，会话立即冻结为 `failed`，之后一切写入与组装均被拒绝。
3. **查询会话** `GET /api/sessions/{id}`
   返回状态、已确认字节数、缺块列表 `missing_chunks`、完成摘要 `final_sha256`、失败原因。
   刷新或重开页面后，前端凭会话标识（localStorage 自动保存，或手动粘贴）拉取缺块列表，**仅补传缺块**。
4. **组装发布** `POST /api/sessions/{id}/assemble`
   仅当全部分块存在时受理：按序写入临时文件并逐块复核摘要，整文件 SHA-256 与申报一致才
   `rename` 原子发布到 `artifacts/` 并标记 `completed`；摘要不符则标记 `failed`，临时文件删除，**成品不可见**。

## 目录结构

```
api/      Go API（标准库 + modernc.org/sqlite，纯 Go 无需 CGO）
  main.go       入口；内置 -healthcheck 供容器健康检查
  store.go      SQLite 模式、会话/分块元数据、数据卷路径
  handlers.go   HTTP 接口：会话、分块、组装、幂等与冲突冻结、原子发布
  server_test.go  真实测试：中断续传、重复幂等、冲突冻结、组装成功/失败、重启恢复
web/      Vue 3 + Vite 前端，nginx 反代 /api 到 api 服务
  src/sha256.js 纯 JS 增量 SHA-256（分片读文件，已对照 node:crypto 校验）
  src/client.js 分块、并发上传、断点续传客户端逻辑
verify/   一次性验收服务：真实 HTTP 联调 + 共享卷核查成品可见性
docker-compose.yml  web / api / verify 三服务，WEB_PORT、API_PORT 可覆盖
```

## 自动化测试（均打真实接口，无假接口）

- **Go 单元/集成测试**（httptest + 真实 SQLite + 真实文件系统）：
  ```bash
  cd api && go test ./...
  ```
  覆盖：中断后按缺块续传、同序号同摘要幂等不重复计数、同序号不同内容冻结 failed、
  组装成功原子发布、整文件摘要不符失败且成品不可见、分块长度与摘要头校验、进程重启后会话状态恢复。
  API 镜像构建阶段会强制执行 `go vet` 与上述测试。
- **verify 验收服务**（容器内真实联调，31 项断言）：
  中断→缺块列表→重复块幂等→补传→组装发布→卷上成品字节级核对；
  冲突冻结；摘要篡改失败且成品不可见；长度/摘要头协议校验。
- **前端**：`cd web && npm ci && npm run build`（镜像构建阶段同样执行）。

## 页面可观测性

页面实时显示：已确认字节数 / 总字节数、已确认分块数、**缺块列表**、进度条、
`uploading / completed / failed` 状态徽章、**完成摘要**（绿色）与失败原因（红色），
并保留事件日志——续传成功与冲突失败都能被直接观察。

## 网络中断与续传

- 所有请求带 60s 超时，半开连接不会让页面永远卡在"上传中"。
- 分块上传幂等，网络抖动时自动按指数退避重试（最多 5 次），页面显示"正在自动重试"。
- 重试耗尽后进入**"连接中断，可续传"**状态：会话已保存、已确认字节不丢失，
  页面每 3 秒自动重新查询缺块列表并续传，也可点"立即续传"手动触发；
  恢复后仅补传服务器仍缺失的分块，无需重新选择文件之外的任何操作。
- 组装请求遇断网同样进入该状态，恢复后先重新查询会话再决定补传或重新组装。
