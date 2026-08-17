#!/usr/bin/env node
/**
 * cline-gateway OAuth 登录工具
 *
 * 复刻 Cline 客户端的 WorkOS 设备授权流（sdk/packages/core/src/auth/cline.ts）：
 *   1. POST https://api.workos.com/user_management/authorize/device
 *        body: client_id=client_01K3A541FN8TA3EPPHTD2325AR
 *   2. 浏览器打开 verification_uri_complete，输入 user_code 完成授权
 *   3. 轮询 POST https://api.workos.com/user_management/authenticate
 *        body: grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=...
 *   4. POST {CLINE_API_BASE}/api/v1/auth/register   （带 Cline 指纹头）
 *        body: {"accessToken": workos 原生 access_token, "refreshToken": ...}
 *        返回: {success:true, data:{accessToken:"workos:...", refreshToken:"workos:...",
 *               expiresAt: ISO, userInfo:{email,...}}}
 *   5. 追加保存到 <项目>/config/tokens.json（多账号累加，按 email 去重，0600 权限）
 *
 * 用法：
 *   node oauth.js                          # 交互式登录（默认打浏览器）
 *   CLINE_API_BASE=http://127.0.0.1:7777/api/v1 node oauth.js   # 自定义上游
 *   CLINE_GATEWAY_TOKENS=/path/to/tokens.json node oauth.js     # 自定义存储位置
 */
import { readFileSync, writeFileSync, existsSync, mkdirSync, chmodSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const WORKOS_API_BASE = "https://api.workos.com";
const WORKOS_CLIENT_ID = process.env.CLINE_WORKOS_CLIENT_ID ?? "client_01K3A541FN8TA3EPPHTD2325AR";
const CLINE_API_BASE = (process.env.CLINE_API_BASE ?? "https://api.cline.bot/api/v1").replace(/\/+$/, "");
// 项目级 token 存储：<项目>/config/tokens.json（server.js 同路径读取）
const PROJECT_ROOT = dirname(fileURLToPath(import.meta.url));
const TOKENS_FILE = process.env.CLINE_GATEWAY_TOKENS ?? join(PROJECT_ROOT, "config", "tokens.json");

// 复刻 Cline CLI 的客户端指纹（与 server.js 一致）
const CLINE_CLIENT_VERSION = process.env.CLINE_CLIENT_VERSION ?? "3.0.55";
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
    "X-CORE-VERSION": "0.0.75",
    "X-Task-ID": TASK_ID,
    "User-Agent": `Cline/${CLINE_CLIENT_VERSION}`,
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 1) WorkOS 设备授权
async function requestDeviceAuthorization() {
  const r = await fetch(`${WORKOS_API_BASE}/user_management/authorize/device`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ client_id: WORKOS_CLIENT_ID }),
    signal: AbortSignal.timeout(30_000),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`device authorization failed: ${r.status} ${j.error_description ?? ""}`);
  if (!j.device_code || !j.user_code || !j.verification_uri) throw new Error("invalid device authorization response");
  return {
    deviceCode: j.device_code,
    userCode: j.user_code,
    verificationUriComplete: j.verification_uri_complete ?? j.verification_uri,
    expiresInSeconds: Math.floor(j.expires_in ?? 300),
    pollIntervalSeconds: Math.max(1, Math.floor(j.interval ?? 5)),
  };
}

// 2) 轮询授权结果
async function pollWorkOSTokens(deviceCode, expiresInSeconds, pollIntervalSeconds) {
  const deadline = Date.now() + expiresInSeconds * 1000;
  let interval = pollIntervalSeconds;
  while (Date.now() <= deadline) {
    const r = await fetch(`${WORKOS_API_BASE}/user_management/authenticate`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
        device_code: deviceCode,
        client_id: WORKOS_CLIENT_ID,
      }),
      signal: AbortSignal.timeout(30_000),
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok) {
      if (!j.access_token || !j.refresh_token) throw new Error("invalid WorkOS token response");
      return { accessToken: j.access_token, refreshToken: j.refresh_token };
    }
    if (j.error === "authorization_pending") {
      console.log(`  ...等待浏览器授权确认（${interval}s 后重试）`);
      await sleep(interval * 1000);
    } else if (j.error === "slow_down") {
      interval += 1;
      await sleep(interval * 1000);
    } else {
      throw new Error(`authorization failed: ${j.error} ${j.error_description ?? ""}`);
    }
  }
  throw new Error("device authorization timed out (300s)");
}

// 3) Cline token 注册
// 对齐源码：CLI 的 loginClineOAuth 不传自定义 headers（provider-auth-registry.ts
// createClineAuthHandler.login 只传 apiBaseUrl/useWorkOSDeviceAuth/callbacks），
// register 请求仅 Content-Type: application/json
// 注意 URL：源码 resolveUrl(apiBaseUrl="https://api.cline.bot", "/api/v1/auth/register")
// = https://api.cline.bot/api/v1/auth/register；CLINE_API_BASE 默认已含 /api/v1，
// 因此这里只拼 /auth/register（不要拼成双 /api/v1！）
async function registerClineTokens(workosTokens) {
  const r = await fetch(`${CLINE_API_BASE}/auth/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(workosTokens),
    signal: AbortSignal.timeout(30_000),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.success || !j.data?.accessToken) {
    throw new Error(`token registration failed: ${r.status} ${j.error?.message ?? JSON.stringify(j).slice(0, 200)}`);
  }
  return {
    access: j.data.accessToken,
    refresh: j.data.refreshToken ?? workosTokens.refreshToken,
    expires: j.data.expiresAt ? Date.parse(j.data.expiresAt) : undefined,
    email: j.data.userInfo?.email,
    accountId: j.data.userInfo?.clineUserId,
  };
}

// 4) 存储：0600 权限，按 email 去重（同账号更新、新账号追加）
function loadStored() {
  if (!existsSync(TOKENS_FILE)) return [];
  try {
    const arr = JSON.parse(readFileSync(TOKENS_FILE, "utf8"));
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function saveTokens(all) {
  mkdirSync(dirname(TOKENS_FILE), { recursive: true });
  writeFileSync(TOKENS_FILE, JSON.stringify(all, null, 2) + "\n");
  chmodSync(TOKENS_FILE, 0o600);
}

// ---------------------------------------------------------------- main
const device = await requestDeviceAuthorization();
console.log("=".repeat(60));
console.log("Cline OAuth 登录（WorkOS 设备流）");
console.log("=".repeat(60));
console.log(`1. 打开授权链接（浏览器登录你的 Cline 账号）:`);
console.log(`   ${device.verificationUriComplete}`);
console.log(`2. 输入设备代码: ${device.userCode}`);
console.log("=".repeat(60));

try {
  const workos = await pollWorkOSTokens(device.deviceCode, device.expiresInSeconds, device.pollIntervalSeconds);
  console.log("[1/2] WorkOS 授权成功，正在向 Cline 注册 token ...");
  const cred = await registerClineTokens(workos);
  console.log("[2/2] 注册成功");

  const stored = loadStored();
  const idx = cred.email ? stored.findIndex((t) => t.email === cred.email) : stored.findIndex((t) => t.access === cred.access);
  if (idx >= 0) stored[idx] = { ...stored[idx], ...cred };
  else stored.push(cred);
  saveTokens(stored);

  console.log("=".repeat(60));
  console.log(`账号:     ${cred.email ?? "unknown"}`);
  console.log(`access:   ${cred.access.slice(0, 24)}...`);
  console.log(`refresh:  ${(cred.refresh ?? "?").slice(0, 24)}...`);
  console.log(`expires:  ${cred.expires ? new Date(cred.expires).toISOString() : "n/a"}`);
  console.log(`已保存到: ${TOKENS_FILE}（共 ${stored.length} 个账号）`);
  console.log(`启动网关: node server.js`);
  console.log("=".repeat(60));
} catch (e) {
  console.error(`\n登录失败: ${e.message}`);
  process.exit(1);
}