#!/usr/bin/env node
/**
 * cline-gateway — 本地 OpenAI 兼容网关，桥接 Cline 免费模型（api.cline.bot OAuth token）
 *
 * 实际作用：让任何 OpenAI 兼容客户端（Cline OpenAI Compatible provider、
 * CherryStudio、NextChat、curl 等）通过本机一个端点使用 Cline 免费模型。
 *
 * 能力：
 *   - POST /v1/chat/completions  转发上游，流式/非流式均支持
 *   - GET  /v1/models            免费模型清单（实时拉取 + 静态兜底）
 *   - GET  /health               健康检查
 *   - 请求头重写（对齐 CLIProxyAPI 的 ScrubProxyAndFingerprintHeaders）：
 *     下游请求头一律丢弃，转发到上游时按白名单重建——只保留 Cline 客户端
 *     应有的头（Content-Type/Accept/Authorization/指纹头）。下游永远无法用
 *     自己的 Authorization 顶替网关账号、污染指纹头或泄露代理身份。
 *   - 响应头过滤（对齐 CPA 的 FilterUpstreamHeaders）：hop-by-hop 头
 *     （Connection/Transfer-Encoding/Upgrade 等）与 Set-Cookie 不透传。
 *   - 下游认证可选：CLINE_GATEWAY_API_KEYS="k1,k2" 启用 Bearer 校验，
 *     未配置 = 无认证（本地企业内网信任）。
 *   - /v1/models 实时暴露上游真实模型清单（recommended-models 接口），
 *     模型名透传不映射，与成熟中转项目（CLIProxyAPI 等）行为一致。
 *   - 多 key 轮询：round-robin 起点轮换；429(免费配额限流) 冷却跳过；过期前
 *     5 分钟预刷新（复刻 Cline 行为）；401 兜底 refresh（per-account 锁串行化）；
 *     每个账号最多试一次，全部失败返回最后一次上游响应
 *   - token 来源（优先级）：CLINE_TOKEN（逗号分隔多 key）>
 *     <项目>/config/tokens.json（oauth.js 写入，含 refresh/expires，0600 权限）>
 *     ~/.cline/data/settings/providers.json（Cline 客户端登录态，只取 access）
 *   - 复刻 Cline 客户端指纹头（服务端可能校验）；CLINE_FINGERPRINT=false 关闭
 *   - 自动解包上游 {success,data} 响应信封（流式逐块解包）
 *
 * 用法：
 *   node server.js
 *   CLINE_TOKEN="workos:aaa,workos:bbb" node server.js    # 多 key
 *   CLINE_GATEWAY_TOKENS=/path/to/tokens.json node server.js
 *   CLINE_API_BASE=https://api.cline.bot/api/v1 node server.js
 *   CLINE_RATE_COOLDOWN_MS=120000 node server.js          # 429 冷却时长
 *
 * 客户端配置示例（任意 OpenAI 兼容客户端）：
 *   Base URL: http://127.0.0.1:3000/v1
 *   API Key:  任意非空字符串（本地网关不校验）
 *   Model:    deepseek/deepseek-v4-flash
 */
import { createServer } from "node:http";
import { readFileSync, writeFileSync, existsSync, mkdirSync, chmodSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

// ---------------------------------------------------------------- 配置
const PORT = Number(process.env.PORT ?? 3000);
const CLINE_API_BASE = (process.env.CLINE_API_BASE ?? "https://api.cline.bot/api/v1").replace(/\/+$/, "");
const PROJECT_ROOT = dirname(fileURLToPath(import.meta.url));
const DEFAULT_PROVIDERS_JSON = join(homedir(), ".cline", "data", "settings", "providers.json");
const PROVIDERS_JSON = process.env.CLINE_PROVIDERS_JSON || DEFAULT_PROVIDERS_JSON;
// 项目级 token 存储：<项目>/config/tokens.json（oauth.js 同路径写入）
const TOKENS_FILE = process.env.CLINE_GATEWAY_TOKENS ?? join(PROJECT_ROOT, "config", "tokens.json");
const USE_FINGERPRINT = process.env.CLINE_FINGERPRINT !== "false";
const RATE_COOLDOWN_MS = Number(process.env.CLINE_RATE_COOLDOWN_MS ?? 60_000);
// 复刻 Cline 的 DEFAULT_REFRESH_BUFFER_MS：过期前 5 分钟即视为需刷新
const REFRESH_BUFFER_MS = Number(process.env.CLINE_REFRESH_BUFFER_MS ?? 5 * 60 * 1000);
// 可选下游认证：配置后校验 Bearer key；未配置 = 无认证（本地信任）
const GATEWAY_API_KEYS = (process.env.CLINE_GATEWAY_API_KEYS ?? "")
  .split(",").map((s) => s.trim()).filter(Boolean);

const FALLBACK_MODELS = [
  "deepseek/deepseek-v4-flash",
  "nvidia/nemotron-3.5-lightning",
  "poolside/laguna-s-2.1:free",
  "anthropic/claude-opus-5",
  "x-ai/grok-4.5",
  "openai/gpt-5.6-sol",
  "moonshotai/kimi-k3",
];

// ---------------------------------------------------------------- token 加载
function normalizeToken(t) {
  return t.startsWith("workos:") ? t : `workos:${t}`;
}

function loadTokens() {
  const list = [];

  // 1. 环境变量（逗号分隔多 key）
  if (process.env.CLINE_TOKEN) {
    for (const t of process.env.CLINE_TOKEN.split(",")) {
      const s = t.trim();
      if (s) list.push({ access: normalizeToken(s) });
    }
  }

  // 2. oauth.js 的 token 文件（含 refresh/expires/email）
  if (existsSync(TOKENS_FILE)) {
    try {
      const arr = JSON.parse(readFileSync(TOKENS_FILE, "utf8"));
      if (Array.isArray(arr)) {
        for (const t of arr) {
          if (typeof t?.access === "string" && t.access) list.push({ ...t, access: normalizeToken(t.access) });
        }
      }
    } catch (e) {
      console.error(`[cline-gateway] 解析 ${TOKENS_FILE} 失败: ${e.message}`);
    }
  }

  // 3. Cline 客户端登录态（只取 access token）
  if (existsSync(PROVIDERS_JSON)) {
    try {
      const data = JSON.parse(readFileSync(PROVIDERS_JSON, "utf8"));
      const providers = data.providers ?? data;
      const entries = Array.isArray(providers) ? providers : Object.values(providers);
      for (const cfg of entries) {
        if (!cfg || typeof cfg !== "object") continue;
        const auth = cfg.auth ?? cfg.credentials ?? {};
        const t = auth.accessToken ?? auth.access_token ?? auth.access;
        if (typeof t === "string" && t) list.push({ access: normalizeToken(t.trim()) });
      }
    } catch { /* 忽略损坏文件 */ }
  }

  // 按 access 去重
  const seen = new Set();
  return list.filter((t) => (seen.has(t.access) ? false : (seen.add(t.access), true)));
}

// 回写 token 文件（401 refresh 成功后持久化），0600 权限，按 email 去重
function saveTokens() {
  const own = state.tokens.filter((t) => t.refresh);
  if (!own.length) return;
  const dedup = [];
  for (const t of own) {
    const idx = t.email ? dedup.findIndex((d) => d.email === t.email) : -1;
    if (idx >= 0) dedup[idx] = { ...dedup[idx], ...t };
    else dedup.push({ ...t });
  }
  try {
    mkdirSync(dirname(TOKENS_FILE), { recursive: true });
    writeFileSync(TOKENS_FILE, JSON.stringify(dedup, null, 2) + "\n");
    chmodSync(TOKENS_FILE, 0o600);
  } catch (e) {
    console.error(`[cline-gateway] 写 ${TOKENS_FILE} 失败: ${e.message}`);
  }
}

// 复刻 Cline CLI 的真实客户端指纹（apps/cli/src/main.ts 的 client context +
// sdk/packages/llms/src/providers/request-headers.ts 的 buildClineRequestHeaders）：
//   X-CLIENT-TYPE: cline-cli, X-PLATFORM: cli, 版本 = cli package.json version
//   X-CORE-VERSION: @cline/core package.json version (0.0.75)
//   X-Task-ID: sessionId —— 会话级固定（进程启动时生成一次，模拟单个长会话）
const CLINE_CLIENT_VERSION = process.env.CLINE_CLIENT_VERSION ?? "3.0.55";
const CLINE_CORE_VERSION = "0.0.75";
const TASK_ID = randomUUID();

function fingerprintHeaders() {
  return {
    "HTTP-Referer": "https://cline.bot",
    "X-Title": "Cline",
    "X-IS-MULTIROOT": "false",
    "X-CLIENT-TYPE": "cline-cli",
    "X-CLIENT-VERSION": CLINE_CLIENT_VERSION,
    "X-PLATFORM": "cli",
    "X-PLATFORM-VERSION": CLINE_CLIENT_VERSION,
    "X-CORE-VERSION": CLINE_CORE_VERSION,
    "X-Task-ID": TASK_ID,
    "User-Agent": `Cline/${CLINE_CLIENT_VERSION}`,
  };
}

// ---------------------------------------------------------------- 请求头重写
// 对齐 CLIProxyAPI internal/misc/header_utils.go 的 ScrubProxyAndFingerprintHeaders：
// 下游请求头一律不信任。转发到上游时白名单重建，保证下游客户端永远无法：
//   1. 用自己的 Authorization 顶替网关账号（上游凭据由网关统一注入）
//   2. 污染指纹头（X-Title/X-CLIENT-* 等，服务端据此区分产品界面，错配即 403）
//   3. 泄露网关/下游身份（X-Forwarded-*、Sec-Fetch-*、Accept-Encoding 指纹差异）
const HEADERS_DROPPED = [
  // 代理追踪
  "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-port",
  "x-real-ip", "forwarded", "via",
  // 认证/身份（网关统一注入，下游一律丢弃）
  "authorization", "proxy-authorization", "x-api-key", "api-key", "cookie",
  "x-title", "http-referer", "referer", "user-agent",
  "x-client-type", "x-client-version", "x-platform", "x-platform-version",
  "x-core-version", "x-task-id", "x-is-multiroot",
  "x-stainless-lang", "x-stainless-package-version", "x-stainless-os",
  "x-stainless-arch", "x-stainless-runtime", "x-stainless-runtime-version",
  // 浏览器/Electron 指纹
  "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "sec-fetch-mode",
  "sec-fetch-site", "sec-fetch-dest", "priority",
  // 编码协商（Node fetch 自行处理 gzip；不透传下游的 Accept-Encoding 避免指纹差异）
  "accept-encoding", "accept",
  // hop-by-hop（RFC 7230 §6.1）与代理管理
  "connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade",
  "host", "content-length", "content-type",
];

// 上游请求头白名单重建（对齐 CPA 的 PrepareRequest/InjectCredentials：
// 凭据与产品头全部由网关注入，而不是信任下游）
function buildUpstreamHeaders(token) {
  return {
    "Content-Type": "application/json",
    Accept: "application/json",
    Authorization: `Bearer ${token}`,
    ...(USE_FINGERPRINT ? fingerprintHeaders() : {}),
  };
}

// 响应头过滤（对齐 CPA sdk/api/handlers/header_filter.go 的 FilterUpstreamHeaders）：
// hop-by-hop 头与安全敏感头不透传；Content-Type 由本网关统一管理
const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade",
  "set-cookie", "content-length", "content-encoding", "content-type",
]);

function filterUpstreamHeaders(src) {
  const out = {};
  src?.forEach?.((v, k) => {
    if (!HOP_BY_HOP.has(k.toLowerCase())) out[k] = v;
  });
  return out;
}

// 下游认证（可选）：校验通过才放行；校验用的 Authorization 绝不转发给上游
function checkGatewayAuth(req) {
  if (!GATEWAY_API_KEYS.length) return true;
  const auth = req.headers.authorization ?? "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : auth.trim();
  return GATEWAY_API_KEYS.includes(token);
}

// ---------------------------------------------------------------- 信封解包
function unwrapStream() {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buf = "";
  return new TransformStream({
      transform(chunk, ctrl) {
        buf += decoder.decode(chunk, { stream: true });
        const parts = buf.split("\n\n");
        buf = parts.pop();
        for (const part of parts) {
          for (const line of part.split("\n")) {
            if (!line.startsWith("data:")) continue;
            const payload = line.slice(5).trim();
            if (payload === "[DONE]") {
              ctrl.enqueue(encoder.encode("data: [DONE]\n\n"));
              continue;
            }
            let j;
            try { j = JSON.parse(payload); } catch { continue; }
            if (j && typeof j === "object" && "success" in j && "data" in j) {
              ctrl.enqueue(
                encoder.encode(`data: ${JSON.stringify(j.success === false ? (j.data ?? { error: { message: "upstream failed" } }) : j.data)}\n\n`),
              );
            } else {
              ctrl.enqueue(encoder.encode(`data: ${payload}\n\n`));
            }
          }
        }
      },
      flush(ctrl) {
        if (buf.trim()) ctrl.enqueue(encoder.encode(buf));
      },
    });
}

async function unwrapJson(text) {
  let j = null;
  try { j = JSON.parse(text); } catch { /* 非 JSON 原样返回 */ }
  if (j && typeof j === "object" && "success" in j && "data" in j) {
    return JSON.stringify(j.data);
  }
  return text;
}

// ---------------------------------------------------------------- 上游调用
async function callUpstream(token, body) {
  return fetch(`${CLINE_API_BASE}/chat/completions`, {
    method: "POST",
    headers: buildUpstreamHeaders(token),
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5 * 60 * 1000),
  });
}

// 过期判断：复刻 Cline isCredentialLikelyExpired（expires - buffer）
function needsRefresh(cred) {
  return typeof cred.expires === "number" && Date.now() >= cred.expires - REFRESH_BUFFER_MS;
}

// per-account refresh 锁：并发请求共享同一 refresh promise，
// 避免多个请求同时刷新导致 refresh token 轮换竞争
const refreshLocks = new Map(); // key: refresh token -> Promise<boolean>

async function tryRefresh(cred) {
  if (!cred.refresh) return false;
  if (refreshLocks.has(cred.refresh)) return refreshLocks.get(cred.refresh);
  const p = (async () => {
    try {
      const r = await fetch(`${CLINE_API_BASE}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(USE_FINGERPRINT ? fingerprintHeaders() : {}) },
        body: JSON.stringify({ refreshToken: cred.refresh, grantType: "refresh_token" }),
        signal: AbortSignal.timeout(30_000),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.success || !j.data?.accessToken) {
        console.warn("[cline-gateway] refresh 失败，该账号需重新登录 (node oauth.js)");
        return false;
      }
      cred.access = j.data.accessToken;
      if (j.data.refreshToken) cred.refresh = j.data.refreshToken;
      if (j.data.expiresAt) cred.expires = Date.parse(j.data.expiresAt);
      console.warn(`[cline-gateway] token 刷新成功 (${cred.email ?? "unknown"})`);
      saveTokens();
      return true;
    } catch (e) {
      console.warn(`[cline-gateway] refresh 异常: ${e.message}`);
      return false;
    }
  })();
  refreshLocks.set(cred.refresh, p);
  try {
    return await p;
  } finally {
    refreshLocks.delete(cred.refresh);
  }
}

// ---------------------------------------------------------------- 转发
async function forwardChat(req, res) {
  const bodyText = await readBody(req);
  let body;
  try { body = JSON.parse(bodyText); } catch {
    return sendJson(res, 400, { error: { message: "invalid JSON body", type: "invalid_request_error" } });
  }

  // 模拟 Cline 客户端行为：流式请求必带 stream_options.include_usage
  // （@ai-sdk/openai-compatible 的 includeUsage: true 强制注入）
  if (body.stream === true && !body.stream_options) {
    body.stream_options = { include_usage: true };
  }

  const tokens = state.tokens;
  if (tokens.length === 0) {
    return sendJson(res, 503, {
      error: { message: `no Cline tokens found (looked at ${TOKENS_FILE} and ${PROVIDERS_JSON}); run "node oauth.js" or set CLINE_TOKEN`, type: "server_error" },
    });
  }

  const startIdx = state.rr++ % tokens.length;
  let last = null;
  for (let i = 0; i < tokens.length; i++) {
    const cred = tokens[(startIdx + i) % tokens.length];

    // 429 冷却期内跳过该账号
    const until = state.cooldown.get(cred.access) ?? 0;
    if (until > Date.now()) {
      console.warn(`[cline-gateway] account ${cred.email ?? "?"} in cooldown (${Math.ceil((until - Date.now()) / 1000)}s left), skipping`);
      continue;
    }

    // 过期前预刷新（复刻 Cline 的 getValidClineCredentials 行为）
    if (needsRefresh(cred)) {
      console.warn(`[cline-gateway] ${cred.email ?? "?"} token expiring, refreshing before request ...`);
      if (await tryRefresh(cred)) {
        // 刷新成功，继续用新 access 发请求
      } else {
        last = { status: 401, headers: new Headers({ "Content-Type": "application/json" }), body: null, text: async () => JSON.stringify({ error: { message: "token refresh failed, run 'node oauth.js' to re-login", type: "authentication_error" } }) };
        continue;
      }
    }

    const upstream = await callUpstream(cred.access, body).catch((e) => ({ status: 0, error: e }));
    if (upstream.error) {
      return sendJson(res, 502, { error: { message: `upstream error: ${upstream.error.message}`, type: "server_error" } });
    }

    if (upstream.status === 429) {
      state.cooldown.set(cred.access, Date.now() + RATE_COOLDOWN_MS);
      last = upstream;
      console.warn(`[cline-gateway] ${cred.email ?? "?"} -> HTTP 429 (rate limit), cooling down ${RATE_COOLDOWN_MS / 1000}s, switching account`);
      continue;
    }

    if (upstream.status === 401 && cred.refresh) {
      console.warn(`[cline-gateway] ${cred.email ?? "?"} -> HTTP 401, attempting refresh ...`);
      const ok = await tryRefresh(cred);
      if (ok) {
        const retried = await callUpstream(cred.access, body).catch((e) => ({ status: 0, error: e }));
        if (retried.error) {
          return sendJson(res, 502, { error: { message: `upstream error: ${retried.error.message}`, type: "server_error" } });
        }
        if (retried.status === 429) {
          state.cooldown.set(cred.access, Date.now() + RATE_COOLDOWN_MS);
          last = retried;
          continue;
        }
        return passThrough(res, retried, body);
      }
      last = upstream;
      continue;
    }

    return passThrough(res, upstream, body);
  }
  // 所有账号都失败：原样转发最后一次上游响应
  return passThrough(res, last ?? { status: 502, headers: new Headers({ "Content-Type": "application/json" }), body: null, text: async () => JSON.stringify({ error: { message: "all accounts failed", type: "server_error" } }) }, body);
}

async function passThrough(res, upstream, body) {
  const contentType = upstream.headers?.get?.("content-type") ?? "application/json";
  const isStream = body?.stream === true && contentType.includes("text/event-stream");
  // 响应头过滤：hop-by-hop 与安全敏感头不透传（Content-Type 由网关管理）
  const headers = { "Content-Type": contentType, ...filterUpstreamHeaders(upstream.headers) };
  res.writeHead(upstream.status, headers);
  if (isStream) {
    // ServerResponse 是 Node 流而非 web WritableStream，不能 pipeTo，手动转发
    const reader = upstream.body.pipeThrough(unwrapStream()).getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        res.write(value);
      }
    } finally {
      res.end();
    }
    return;
  }
  const text = await upstream.text();
  res.end(await unwrapJson(text));
}

// ---------------------------------------------------------------- HTTP
async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}

function sendJson(res, status, obj) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

// ---------------------------------------------------------------- 模型清单
async function fetchFreeModels() {
  try {
    const r = await fetch(`${CLINE_API_BASE}/ai/cline/recommended-models`, { signal: AbortSignal.timeout(8000) });
    if (r.ok) {
      const j = await r.json();
      const ids = [...(j.free ?? []), ...(j.recommended ?? [])].map((m) => m.id).filter(Boolean);
      if (ids.length) return ids;
    }
  } catch { /* 兜底 */ }
  return FALLBACK_MODELS;
}

const state = {
  tokens: loadTokens(),
  rr: 0,
  cooldown: new Map(), // access -> cooldownUntilMs
};

const server = createServer(async (req, res) => {
  // 可选下游认证（CLINE_GATEWAY_API_KEYS 配置后启用；未配置 = 无认证）
  if (!checkGatewayAuth(req)) {
    return sendJson(res, 401, { error: { message: "invalid gateway API key", type: "invalid_request_error" } });
  }
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (req.method === "GET" && url.pathname === "/health") {
    return sendJson(res, 200, { ok: true, tokens: state.tokens.length, upstream: CLINE_API_BASE });
  }
  if (req.method === "GET" && url.pathname === "/v1/models") {
    const models = await fetchFreeModels();
    return sendJson(res, 200, {
      object: "list",
      data: models.map((id) => ({ id, object: "model", owned_by: "cline", created: 0 })),
    });
  }
  if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
    return forwardChat(req, res);
  }
  return sendJson(res, 404, { error: { message: `not found: ${req.method} ${url.pathname}`, type: "invalid_request_error" } });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`[cline-gateway] listening on http://127.0.0.1:${PORT}/v1`);
  console.log(`[cline-gateway] upstream: ${CLINE_API_BASE}`);
  console.log(`[cline-gateway] tokens: ${state.tokens.length}${state.tokens.length ? "" : "  (WARNING: none — run 'node oauth.js' or set CLINE_TOKEN)"}`);
  console.log(`[cline-gateway] rate cooldown: ${RATE_COOLDOWN_MS / 1000}s | fingerprint: ${USE_FINGERPRINT ? "on" : "off"}`);
  console.log(`[cline-gateway] downstream auth: ${GATEWAY_API_KEYS.length ? `on (${GATEWAY_API_KEYS.length} key(s))` : "off (local trust)"}`);
});