# 代理服务流式响应问题排查与修复

## 问题描述

代理服务在收到请求后会卡住，客户端只能收到初始的 SSE 事件（`message_start` 和 `content_block_start`），但收不到后续的内容数据（`content_block_delta`），最终超时断开。

## 问题现象

### 客户端表现
```bash
curl -N -X POST http://localhost:4000/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-haiku-20241022","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

只能收到：
```
event: message_start
data: {"type":"message_start",...}

event: content_block_start
data: {"type":"content_block_start",...}
```

然后一直等待，直到超时。

### 服务端日志
```
收到请求时间: 2026-03-05T18:37:38.744Z
客户端断开连接，距请求开始: 27ms
警告: 客户端过早断开，可能是超时
上游响应已连接，开始流式转发... 耗时: 1059 ms
收到首批数据，开始处理流，chunk长度: 1018
检测到客户端已断开，停止处理
```

**关键线索：** 客户端在请求开始后 27ms 就被判定为"断开连接"，但实际上客户端还在等待数据。

## 根本原因

Express 的 `req.on("close")` 事件在使用流式响应时会**过早触发**。这是一个已知的 Node.js/Express 问题：

1. 当代理发送初始 SSE 事件后，`req` 对象可能触发 `close` 事件
2. 代码设置 `isClientConnected = false`
3. 当上游数据到达时，由于 `isClientConnected` 为 false，数据被丢弃
4. 客户端一直等待，最终超时

**关键发现：** 通过添加时间戳日志，发现 `req.on("close")` 在请求开始后仅 27ms 就触发了，而此时客户端明明还在运行并等待响应。这说明 `close` 事件是误报。

## 解决方案

### 问题 1: `req.on("close")` 误报

将 `req.on("close")` 改为 `res.on("error")`，这是检测客户端断开的更可靠方式。

### 问题 2: 数据缓冲导致延迟

**关键发现：** 即使修复了问题 1，仍然存在数据被缓冲的问题。初始 SSE 事件能快速发送，但后续的 `content_block_delta` 数据堆积在 Node.js 缓冲区中，无法实时发送给客户端。

**原因：** Express 的 `res.write()` 默认会缓冲数据以提高效率。在流式场景中，需要显式调用 `flush()` 来立即发送数据。

**修复：** 在每次写入数据后调用 `res.flush()`

```javascript
// 修改前
res.write(`event: content_block_delta
`);
res.write(`data: ${JSON.stringify(event)}

`);

// 修改后
res.write(`event: content_block_delta
`);
res.write(`data: ${JSON.stringify(event)}

`);
// 立即刷新，确保数据实时发送
if (res.flush) res.flush();
```

### 修改前
```javascript
// 监听客户端断开
req.on("close", () => {
  console.log("客户端断开连接");
  isClientConnected = false;
  if (upstreamResponse) {
    upstreamResponse.destroy();
  }
});
```

### 修改后
```javascript
// 监听响应错误来检测客户端断开（更可靠）
res.on("error", (err) => {
  console.log("响应错误，客户端可能已断开:", err.message);
  isClientConnected = false;
  if (upstreamResponse) {
    upstreamResponse.destroy();
  }
});
```

## 验证结果

修复后，数据可以正常流式传输：

```
event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"收到"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"用户"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"发来的"}}
...
```

## 经验总结

1. **不要信任 `req.on("close")` 用于流式响应** - 在 SSE 或流式传输场景下，这个事件可能会误报
2. **使用 `res.on("error")` 代替** - 响应对象的错误事件更可靠
3. **添加详细日志进行诊断** - 时间戳日志对于定位时序问题非常关键
4. **验证客户端实际状态** - 不要仅依赖服务端的断开检测，要结合客户端的实际行为来判断

## 相关问题

类似的问题也可能出现在其他流式场景：
- Server-Sent Events (SSE)
- WebSocket
- 大文件下载
- 实时数据推送

在这些场景中，都应该避免使用 `req.on("close")` 来判断客户端状态。
