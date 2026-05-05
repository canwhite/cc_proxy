require("dotenv").config();
const express = require("express");
const axios = require("axios");
const app = express();

app.use(express.json({ limit: "50mb" }));

app.post("/v1/messages", async (req, res) => {
  let upstreamResponse = null;
  let isClientConnected = true;

  // 监听响应错误来检测客户端断开（更可靠）
  res.on("error", (err) => {
    console.log("响应错误，客户端可能已断开:", err.message);
    isClientConnected = false;
    if (upstreamResponse) {
      upstreamResponse.destroy();
    }
  });

  try {
    console.log("收到请求时间:", new Date().toISOString());
    console.log("请求模型:", req.body.model);
    console.log("请求体预览:", JSON.stringify(req.body).slice(0, 500));

    const { messages, max_tokens, stream } = req.body;

    // 设置 SSE 响应头
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no"); // 禁用 nginx 缓冲

    // 发送开始事件
    res.write(`event: message_start\n`);
    res.write(`data: ${JSON.stringify({
      type: "message_start",
      message: {
        id: `msg_${Date.now()}`,
        type: "message",
        role: "assistant",
        content: [],
        model: "claude-3-5-haiku-20241022",
        stop_reason: null,
        usage: { prompt_tokens: 0, completion_tokens: 0 }
      }
    })}\n\n`);

    res.write(`event: content_block_start\n`);
    res.write(`data: ${JSON.stringify({
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" }
    })}\n\n`);

    // 立即刷新，确保客户端收到
    if (res.flush) {
      res.flush();
    }

    const openaiReq = {
      model: "GLM-5",
      messages,
      max_tokens,
      stream: true,
    };

    console.log("转发请求到后端:", JSON.stringify(openaiReq).slice(0, 500));

    console.log("正在发起上游请求...");
    const response = await axios.post(
      "https://api.edgefn.net/v1/chat/completions",
      openaiReq,
      {
        headers: {
          Authorization: `Bearer ${process.env.EDGEFN_API_KEY}`,
          "Content-Type": "application/json",
          Accept: "text/event-stream",
        },
        responseType: "stream",
        timeout: 60000, // 60秒超时
      },
    );

    upstreamResponse = response.data;
    console.log("上游响应已连接，开始流式转发...");

    let accumulatedTokens = 0;
    let buffer = "";
    let lastHeartbeat = Date.now();
    let dataReceived = false;

    // 发送心跳以保持连接
    const heartbeatInterval = setInterval(() => {
      if (!isClientConnected) {
        clearInterval(heartbeatInterval);
        return;
      }
      // 每 30 秒发送一个注释作为心跳
      if (Date.now() - lastHeartbeat > 30000) {
        try {
          res.write(": heartbeat\n\n");
          lastHeartbeat = Date.now();
        } catch (e) {
          clearInterval(heartbeatInterval);
        }
      }
    }, 15000);

    // 10秒内如果没有收到数据，打印警告
    const dataTimeout = setTimeout(() => {
      if (!dataReceived && isClientConnected) {
        console.warn("警告: 10秒内未收到上游数据，上游可能无响应");
      }
    }, 10000);

    // 处理流式响应
    response.data.on("data", (chunk) => {
      if (!dataReceived) {
        dataReceived = true;
        clearTimeout(dataTimeout);
        console.log("收到首批数据，开始处理流");
      }
      if (!isClientConnected) {
        clearInterval(heartbeatInterval);
        return;
      }

      lastHeartbeat = Date.now();
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || ""; // 保留最后一个可能不完整的行

      for (const line of lines) {
        if (!line.trim() || !line.startsWith("data: ")) continue;

        const data = line.slice(6).trim();
        if (data === "[DONE]") continue;

        try {
          const parsed = JSON.parse(data);
          // 上游返回的是 reasoning_content 而不是 content
          const content = parsed.choices?.[0]?.delta?.content || parsed.choices?.[0]?.delta?.reasoning_content;

          if (content) {
            accumulatedTokens += Math.ceil(content.length / 4);

            try {
              const event = {
                type: "content_block_delta",
                index: 0,
                delta: { type: "text_delta", text: content },
              };
              res.write(`event: content_block_delta\n`);
              res.write(`data: ${JSON.stringify(event)}\n\n`);
              // 立即刷新，确保数据实时发送
              if (res.flush) res.flush();
            } catch (writeErr) {
              console.error("写入响应失败:", writeErr);
              isClientConnected = false;
              clearInterval(heartbeatInterval);
            }
          }
        } catch (e) {
          // 忽略解析错误，可能是数据被截断
        }
      }
    });

    response.data.on("end", () => {
      clearInterval(heartbeatInterval);
      clearTimeout(dataTimeout);
      if (!isClientConnected) return;

      console.log("流式传输结束，累计token:", accumulatedTokens);

      try {
        res.write(`event: content_block_stop\n`);
        res.write(`data: ${JSON.stringify({
          type: "content_block_stop",
          index: 0
        })}\n\n`);

        // message_delta 事件
        res.write(`event: message_delta\n`);
        res.write(`data: ${JSON.stringify({
          type: "message_delta",
          delta: { stop_reason: "end_turn" },
          usage: {
            prompt_tokens: 0,
            completion_tokens: accumulatedTokens
          }
        })}\n\n`);

        res.write(`event: message_stop\n`);
        res.write(`data: ${JSON.stringify({
          type: "message_stop"
        })}\n\n`);

        res.end();
      } catch (e) {
        console.error("结束响应时出错:", e);
      }
    });

    response.data.on("error", (err) => {
      clearInterval(heartbeatInterval);
      clearTimeout(dataTimeout);
      console.error("流式传输错误:", err.message);
      if (isClientConnected && !res.writableEnded) {
        try {
          res.write(`event: error\n`);
          res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
          res.end();
        } catch (e) {
          // 忽略写入错误
        }
      }
    });

  } catch (error) {
    console.error("代理捕获到异常:");
    if (error.response) {
      console.error("后端错误状态:", error.response.status);
      console.error("后端错误数据:", error.response.data?.slice(0, 500));
    } else if (error.request) {
      console.error("后端无响应:", error.message);
    } else {
      console.error("其他错误:", error.message);
    }

    if (!res.headersSent) {
      res.status(502).json({ error: "Upstream error", details: error.message });
    } else if (!res.writableEnded) {
      try {
        res.write(`event: error\n`);
        res.write(`data: ${JSON.stringify({ error: error.message })}\n\n`);
        res.end();
      } catch (e) {
        // 忽略
      }
    }
  }
});

const PORT = 4000;
app.listen(PORT, () => console.log(`Proxy running on port ${PORT}`));
