#!/usr/bin/env node
/**
 * 测试专用 mock 上游（不进项目主代码）：模拟 api.cline.bot 的行为
 *   - POST /api/v1/chat/completions
 *       token 含 "workos:first"        -> 429 free-models-per-day（模拟免费配额限流）
 *       其余 token                     -> 200，响应包 {success:true,data:{...}} 信封
 *       请求带 stream:true             -> SSE 流式，逐块包信封
 *   - POST /api/v1/auth/refresh
 *       返回 {success:true,data:{accessToken:"workos:refreshed-access",...}}
 *   - GET  /api/v1/ai/cline/recommended-models -> 免费模型清单
 *
 * 用法：node test/mock-upstream.js   （监听 9001，可用 PORT 覆盖）
 */
import { createServer } from "node:http";

const PORT = Number(process.env.PORT ?? 9001);

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const json = (status, obj) => {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };

  if (req.method === "GET" && url.pathname === "/api/v1/ai/cline/recommended-models") {
    return json(200, { recommended: [], free: [{ id: "deepseek/deepseek-v4-flash" }], clinePass: [] });
  }

  if (req.method === "POST" && url.pathname === "/api/v1/auth/refresh") {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    console.log(`[mock] refresh grantType=${body.grantType} refresh=${(body.refreshToken ?? "").slice(0, 24)}...`);
    // 场景控制：refresh token 包含特定标记模拟不同失败
    if ((body.refreshToken ?? "").includes("transient-fail")) {
      return json(500, { error: "upstream temporary error" });
    }
    if ((body.refreshToken ?? "").includes("invalid-grant")) {
      return json(400, { error: "invalid_grant: refresh token revoked" });
    }
    // 模拟真实 api.cline.bot：accessToken 是裸 JWT（无 workos: 前缀），
    // refreshToken 轮换为新值 —— 网关必须规范化前缀后才能继续使用
    return json(200, {
      success: true,
      data: {
        accessToken: "refreshed-access-jwt",
        refreshToken: "workos:refreshed-refresh",
        tokenType: "Bearer",
        expiresAt: new Date(Date.now() + 3600_000).toISOString(),
        userInfo: { email: "old@example.com", clineUserId: "user_mock" },
      },
    });
  }

  if (req.method !== "POST" || url.pathname !== "/api/v1/chat/completions") {
    return json(404, { error: { message: "mock not found" } });
  }

  const chunks = [];
  for await (const c of req) chunks.push(c);
  const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  const auth = req.headers.authorization ?? "";

  if (auth.includes("workos:first")) {
    console.log("[mock] 429 for", auth.slice(0, 24));
    // 真实限流消息文本（errors.ts: CLINE_FREE_MODEL_LIMIT_MARKER =
    // "free limit reached on model", CLINE_FREE_MODEL_LIMIT_RETRY_MARKER = "try again in "）
    return json(429, { error: "Free limit reached on model deepseek/deepseek-v4-flash, try again in 1h" });
  }

  // 模拟上游半途断连：发 1 个 chunk 后销毁连接（验证网关照常优雅收尾，
  // 对下游补发 SSE error + [DONE]，而不是"网络中断"）
  // 注意：必须等头+数据真正 flush 到 socket 后再 destroy，否则客户端拿不到响应头
  if (auth.includes("workos:drop-stream")) {
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
    res.write(`data: ${JSON.stringify({ success: true, data: { id: "chatcmpl-drop", object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: body.model, choices: [{ index: 0, delta: { content: "partial" }, finish_reason: null }] } })}\n\n`, () => {
      // 数据确认送入 socket 后再断连，模拟真实"发一半网络中断"
      setTimeout(() => res.destroy(), 50);
    });
    return;
  }

  console.log("[mock] 200 for", auth.slice(0, 24), "| stream =", body.stream, "| model =", body.model, "| stream_options =", JSON.stringify(body.stream_options ?? null));
  // 记录完整请求头（小写键），供测试断言"网关头重写"效果
  console.log("[mock] headers:", JSON.stringify(req.headers));
  if (body.stream !== true) {
    return json(200, {
      success: true,
      data: {
        id: "chatcmpl-mock",
        object: "chat.completion",
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{ index: 0, message: { role: "assistant", content: "hello from mock upstream" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 5, completion_tokens: 4, total_tokens: 9 },
      },
    });
  }

  // 流式：每个块包 data 信封（模拟 cline.bot 的 SSE 信封）
  res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
  const created = Math.floor(Date.now() / 1000);
  const chunksOut = [
    { id: "chatcmpl-mock", object: "chat.completion.chunk", created, model: body.model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] },
    { id: "chatcmpl-mock", object: "chat.completion.chunk", created, model: body.model, choices: [{ index: 0, delta: { content: "hello" }, finish_reason: null }] },
    { id: "chatcmpl-mock", object: "chat.completion.chunk", created, model: body.model, choices: [{ index: 0, delta: { content: " from mock" }, finish_reason: null }] },
    { id: "chatcmpl-mock", object: "chat.completion.chunk", created, model: body.model, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 4, total_tokens: 9 } },
  ];
  for (const c of chunksOut) {
    res.write(`data: ${JSON.stringify({ success: true, data: c })}\n\n`);
  }
  res.end("data: [DONE]\n\n");
});

server.listen(PORT, "127.0.0.1", () => console.log(`[mock] listening on :${PORT}`));