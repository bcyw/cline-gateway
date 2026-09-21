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
import { once } from "node:events";
import { readFileSync, writeFileSync, existsSync, mkdirSync, chmodSync, renameSync } from "node:fs";
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
// 上游整体超时（含流式 body 读取）：默认 20 分钟，防长响应（思维链等）被 5 分钟硬超时误杀
const UPSTREAM_TIMEOUT_MS = Number(process.env.CLINE_UPSTREAM_TIMEOUT_MS ?? 20 * 60 * 1000);
// 流式无数据看门狗：上游超过该时长未吐任何数据视为死连接（网络中断但 TCP 未关），主动中止
const UPSTREAM_INACTIVITY_MS = Number(process.env.CLINE_UPSTREAM_INACTIVITY_MS ?? 5 * 60 * 1000);
// 下游 SSE 心跳间隔：上游长时间不吐数据（推理阶段）时向下游发 ": ping" 注释保活，
// 防止中间链路（代理/路由器空闲超时）把连接掐断，表现为客户端"网络中断"
const SSE_HEARTBEAT_MS = Number(process.env.CLINE_SSE_HEARTBEAT_MS ?? 15_000);
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
// 原子写：先写临时文件再 rename，避免进程崩溃/多实例并发读到半写文件
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
    const tmp = `${TOKENS_FILE}.tmp`;
    writeFileSync(tmp, JSON.stringify(dedup, null, 2) + "\n");
    chmodSync(tmp, 0o600);
    renameSync(tmp, TOKENS_FILE);
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
async function callUpstream(token, body, signal) {
  return fetch(`${CLINE_API_BASE}/chat/completions`, {
    method: "POST",
    headers: buildUpstreamHeaders(token),
    body: JSON.stringify(body),
    // 超时/中止由 forwardChat 的统一 AbortController 管理（含客户端断开联动）
    signal,
  });
}

// 过期判断：复刻 Cline isCredentialLikelyExpired（expires - buffer）
function needsRefresh(cred) {
  return typeof cred.expires === "number" && Date.now() >= cred.expires - REFRESH_BUFFER_MS;
}

// per-account refresh 锁：并发请求共享同一 refresh promise，
// 避免多个请求同时刷新导致 refresh token 轮换竞争
const refreshLocks = new Map(); // key: refresh token -> Promise<"ok"|"transient"|"invalid_grant">

// 对齐 Cline 源码 getValidClineCredentials 的错误语义（cline.ts:821-871）：
//   - invalid_grant（400/401/403 + invalid/expired/revoked/unauthorized）
//     → refresh token 被拒绝，必须重新登录
//   - 其余（网络、超时、5xx、响应异常）→ 瞬时失败，调用方应保留当前 token 继续用
function isInvalidGrant(r, j) {
  const code = String(j?.errorCode ?? j?.error?.code ?? "");
  if (/invalid_grant|invalid_token|unauthorized/i.test(code)) return true;
  if (r.status === 400 || r.status === 401 || r.status === 403) {
    const text = JSON.stringify(j) || "";
    return /invalid|expired|revoked|unauthorized/i.test(text);
  }
  return false;
}

// 多实例防护：重读 tokens.json，若同账号已被另一实例刷新过（access 已更新），
// 直接采用文件里的最新凭证，不再调用 refresh 接口（避免旧 refresh token 竞争）
function adoptNewerFromFile(cred) {
  try {
    const arr = JSON.parse(readFileSync(TOKENS_FILE, "utf8"));
    if (!Array.isArray(arr)) return false;
    const match =
      (cred.email ? arr.find((t) => t.email === cred.email) : undefined) ??
      arr.find((t) => t.refresh && t.refresh === cred.refresh);
    if (!match) return false;
    let adopted = false;
    // 注意：文件里 access 是裸 JWT，必须 normalize 后再比较（否则永远不等，误判"已刷新"）
    if (match.access && normalizeToken(match.access) !== cred.access) {
      cred.access = normalizeToken(match.access);
      if (match.refresh) cred.refresh = match.refresh;
      if (typeof match.expires === "number") cred.expires = match.expires;
      console.warn(`[cline-gateway] ${cred.email ?? "?"} 采用文件中的最新凭证（另一实例已刷新）`);
      adopted = true;
    } else if (match.refresh && match.refresh !== cred.refresh) {
      cred.refresh = match.refresh; // refresh 已轮换，用文件里的最新值
    }
    return adopted;
  } catch {
    return false;
  }
}

// 返回值三态：
//   "ok"           — 刷新成功，cred 已更新为最新凭证（access 已带 workos: 前缀）
//   "transient"    — 瞬时失败（网络/5xx），凭据未变；调用方应沿用当前 access 继续
//   "invalid_grant" — refresh token 被拒绝，需重新登录（node oauth.js）
async function tryRefresh(cred) {
  if (!cred.refresh) return "invalid_grant";
  if (refreshLocks.has(cred.refresh)) return refreshLocks.get(cred.refresh);
  const p = (async () => {
    // 多实例防护：文件里已有更新凭证就直接采用
    if (adoptNewerFromFile(cred)) return "ok";
    try {
      const r = await fetch(`${CLINE_API_BASE}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(USE_FINGERPRINT ? fingerprintHeaders() : {}) },
        body: JSON.stringify({ refreshToken: cred.refresh, grantType: "refresh_token" }),
        signal: AbortSignal.timeout(30_000),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.success || !j.data?.accessToken) {
        if (isInvalidGrant(r, j)) {
          console.warn(`[cline-gateway] ${cred.email ?? "?"} refresh token 被拒绝 (invalid_grant)，需重新登录 (node oauth.js)`);
          return "invalid_grant";
        }
        console.warn(`[cline-gateway] ${cred.email ?? "?"} refresh 瞬时失败 (HTTP ${r.status})，沿用当前 token`);
        return "transient";
      }
      // 根因修复：refresh 返回的 accessToken 是裸 JWT（实测无 workos: 前缀），
      // 必须规范化后再发送，否则上游 401 断连
      cred.access = normalizeToken(j.data.accessToken);
      if (j.data.refreshToken) cred.refresh = j.data.refreshToken;
      // expiresAt 解析失败时保留旧值（避免写入 NaN 导致永不预刷新）
      if (j.data.expiresAt) {
        const e = Date.parse(j.data.expiresAt);
        if (!Number.isNaN(e)) cred.expires = e;
      }
      console.warn(`[cline-gateway] token 刷新成功 (${cred.email ?? "unknown"})`);
      saveTokens();
      return "ok";
    } catch (e) {
      console.warn(`[cline-gateway] ${cred.email ?? "?"} refresh 异常: ${e.message}，沿用当前 token`);
      return "transient";
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
  // 统一中止源：整体超时 + 客户端提前断开，两者任一触发即中止上游请求
  // （避免悬空请求继续消耗 Cline 配额、占住 socket）
  const controller = new AbortController();
  const hardTimeout = setTimeout(
    () => controller.abort(new Error(`upstream timeout after ${UPSTREAM_TIMEOUT_MS / 1000}s`)),
    UPSTREAM_TIMEOUT_MS,
  );
  res.on("close", () => {
    // 正常收尾（res.end 已调用）时 writableEnded == true，不中止；客户端提前断开才中止
    if (!res.writableEnded) controller.abort(new Error("client disconnected"));
  });
  try {
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
      const rr = await tryRefresh(cred);
      if (rr === "ok") {
        // 刷新成功，继续用新 access 发请求
      } else if (rr === "transient") {
        // 对齐 Cline 源码：瞬时刷新失败且当前 token 仍有效（>30s grace）时
        // 保留当前 token 继续用（transient_failure_kept_current），不断连
        if (typeof cred.expires !== "number" || Date.now() < cred.expires - 30_000) {
          console.warn(`[cline-gateway] ${cred.email ?? "?"} 沿用当前 token 继续请求`);
        } else {
          last = { status: 502, headers: new Headers({ "Content-Type": "application/json" }), body: null, text: async () => JSON.stringify({ error: { message: "token refresh failed (transient) and token expired, try again in a moment", type: "server_error" } }) };
          continue;
        }
      } else {
        last = { status: 401, headers: new Headers({ "Content-Type": "application/json" }), body: null, text: async () => JSON.stringify({ error: { message: "token refresh failed, run 'node oauth.js' to re-login", type: "authentication_error" } }) };
        continue;
      }
    }

    const upstream = await callUpstream(cred.access, body, controller.signal).catch((e) => ({ status: 0, error: e }));
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
      const rr = await tryRefresh(cred);
      if (rr === "ok") {
        const retried = await callUpstream(cred.access, body, controller.signal).catch((e) => ({ status: 0, error: e }));
        if (retried.error) {
          return sendJson(res, 502, { error: { message: `upstream error: ${retried.error.message}`, type: "server_error" } });
        }
        if (retried.status === 429) {
          state.cooldown.set(cred.access, Date.now() + RATE_COOLDOWN_MS);
          last = retried;
          continue;
        }
        return passThrough(res, retried, body, controller);
      }
      // invalid_grant 时明确提示重新登录；瞬时失败（transient）时原样透传 401
      if (rr === "invalid_grant") {
        last = { status: 401, headers: upstream.headers, body: null, text: async () => JSON.stringify({ error: { message: "refresh token rejected, run 'node oauth.js' to re-login", type: "authentication_error" } }) };
      } else {
        last = upstream;
      }
      continue;
    }

    return passThrough(res, upstream, body, controller);
  }
  // 所有账号都失败：原样转发最后一次上游响应
    return passThrough(res, last ?? { status: 502, headers: new Headers({ "Content-Type": "application/json" }), body: null, text: async () => JSON.stringify({ error: { message: "all accounts failed", type: "server_error" } }) }, body, controller);
  } finally {
    clearTimeout(hardTimeout);
  }
}

async function passThrough(res, upstream, body, controller) {
  const contentType = upstream.headers?.get?.("content-type") ?? "application/json";
  const isStream = body?.stream === true && contentType.includes("text/event-stream");
  // 响应头过滤：hop-by-hop 与安全敏感头不透传（Content-Type 由网关管理）
  const headers = { "Content-Type": contentType, ...filterUpstreamHeaders(upstream.headers) };
  res.writeHead(upstream.status, headers);
  if (isStream) {
    await pipeStreamToResponse(res, upstream, controller);
    return;
  }
  let text;
  try {
    text = await upstream.text();
  } catch (e) {
    // 非流式：上游在 body 读取阶段中断（超时/断连），头部已发出无法改状态码
    console.warn(`[cline-gateway] 非流式读取上游响应失败: ${e?.message ?? e}`);
    if (!res.destroyed) res.destroy();
    return;
  }
  res.end(await unwrapJson(text));
}

// 流式转发（SSE）：优雅收尾 + 背压 + 心跳保活 + 上游断流看门狗
// 核心目标：上游任何形式的中断（断连/超时/socket reset）都不再以"网络中断"
// 的形式砸给下游——头部已发出时补发 SSE error 事件并以 [DONE] 正常收尾。
async function pipeStreamToResponse(res, upstream, controller) {
  const reader = upstream.body.pipeThrough(unwrapStream()).getReader();
  let lastData = Date.now();
  let heartbeat = null;

  // 心跳：上游长时间不吐数据（推理阶段）时向下游发 SSE 注释行保活，
  // 防止中间链路（代理/路由器空闲超时）把连接掐断成"网络中断"
  if (SSE_HEARTBEAT_MS > 0) {
    heartbeat = setInterval(() => {
      if (!res.destroyed && !res.writableEnded && Date.now() - lastData >= SSE_HEARTBEAT_MS) {
        res.write(": ping\n\n");
      }
    }, SSE_HEARTBEAT_MS);
  }

  // 上游看门狗：超过 UPSTREAM_INACTIVITY_MS 没吐任何数据判定死连接，
  // 主动 abort（TCP 可能已半开，read 永远等不到数据）
  const watchdog = setInterval(() => {
    if (Date.now() - lastData > UPSTREAM_INACTIVITY_MS) {
      controller.abort(new Error(`upstream inactivity: no data for ${UPSTREAM_INACTIVITY_MS / 1000}s`));
    }
  }, Math.min(SSE_HEARTBEAT_MS || 10_000, 10_000));

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      lastData = Date.now();
      if (value?.length) {
        if (!res.write(value)) {
          // 背压：写缓冲满时等 drain；客户端断开时 'close' 先触发，防止永久挂起
          await Promise.race([once(res, "drain"), once(res, "close")]);
        }
      }
    }
    if (!res.destroyed) res.end();
  } catch (err) {
    // 主动中止（客户端断开/整体超时/看门狗）与上游意外断流统一在此收尾
    const aborted = controller.signal.aborted || err?.name === "AbortError";
    const reason = aborted
      ? (controller.signal.reason?.message ?? "aborted")
      : (err?.message ?? String(err));
    console.warn(`[cline-gateway] 流式中断: ${reason}${aborted ? " (已中止)" : ""}`);
    if (!res.destroyed && !res.writableEnded) {
      try {
        // 头部已发出，状态码无法更改；补发 SSE error 事件并以 [DONE] 收尾，
        // 客户端显示"上游出错"而非"网络中断"
        res.write(`data: ${JSON.stringify({ error: { message: `upstream stream interrupted: ${reason}`, type: "stream_error" } })}\n\n`);
        res.write("data: [DONE]\n\n");
        res.end();
      } catch {
        res.destroy();
      }
    } else {
      res.destroy();
    }
  } finally {
    if (heartbeat) clearInterval(heartbeat);
    clearInterval(watchdog);
    // 释放上游 body：undici 要求未读/中止的 body 必须 cancel，否则占住连接池导致 socket 泄漏
    try { await upstream.body?.cancel?.(); } catch { /* 已关闭/已消费，忽略 */ }
  }
}

// ---------------------------------------------------------------- HTTP
async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}

function sendJson(res, status, obj) {
  if (res.destroyed) return; // 客户端已断开时向死 socket 写会触发 error 事件
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