#!/usr/bin/env node
/**
 * 端到端自测：mock 上游 + cline-gateway 全链路
 * 覆盖：非流式 / 流式 / 信封解包 / 多 key 轮询(429) / 429 冷却 / 401 预刷新 / 模型列表
 *
 * 用法：node test/test-gateway.mjs
 * 依赖：test/mock-upstream.js（mock 上游，测试时自动启动）
 */
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const MOCK = join(ROOT, "test", "mock-upstream.js");
const GATEWAY = join(ROOT, "server.js");
const TMP = mkdtempSync(join(tmpdir(), "cline-gw-test-"));

const MOCK_PORT = 9101;
const GW_PORT = 9102;
const BASE = `http://127.0.0.1:${GW_PORT}/v1`;

let passed = 0;
let failed = 0;

function assert(name, cond, extra = "") {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.log(`  ✗ ${name} ${extra}`); }
}

async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}

async function start(script, env = {}) {
  const p = spawn(process.execPath, [script], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let logs = "";
  p.stdout.on("data", (d) => (logs += d));
  p.stderr.on("data", (d) => (logs += d));
  await new Promise((r) => setTimeout(r, 600));
  return { p, logs: () => logs };
}

function stop(p) { try { p.kill("SIGTERM"); } catch {} }

async function post(body) {
  const r = await fetch(`${BASE}/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: r.status, text: await r.text() };
}

// ---------------------------------------------------------------- 启动
const mock = await start(MOCK, { PORT: String(MOCK_PORT) });
const gw = await start(GATEWAY, {
  PORT: String(GW_PORT),
  CLINE_API_BASE: `http://127.0.0.1:${MOCK_PORT}/api/v1`,
  CLINE_TOKEN: "first-token,second-token",
  CLINE_RATE_COOLDOWN_MS: "2000",
  CLINE_FINGERPRINT: "true",
  CLINE_GATEWAY_TOKENS: join(TMP, "tokens.json"),
});
const gwReady = await waitReady(`http://127.0.0.1:${GW_PORT}/health`);
assert("网关启动", gwReady, gw.logs());
if (!gwReady) { console.log(gw.logs()); process.exit(1); }

// ---------------------------------------------------------------- 1. 非流式 + 429 轮询 + 信封解包
console.log("\n[1] 非流式：first 429 -> second 200，信封解包");
{
  const { status, text } = await post({
    model: "deepseek/deepseek-v4-flash",
    messages: [{ role: "user", content: "hi" }],
  });
  assert("HTTP 200", status === 200);
  const j = JSON.parse(text);
  assert("响应无 success/data 信封", !("success" in j) && !("data" in j));
  assert("model 透传", j.model === "deepseek/deepseek-v4-flash");
  assert("content 正确", j.choices?.[0]?.message?.content === "hello from mock upstream");
  assert("usage 透传", j.usage?.total_tokens === 9);
}

// ---------------------------------------------------------------- 2. 流式 + 逐块信封解包
console.log("\n[2] 流式：SSE 逐块解包");
{
  const r = await fetch(`${BASE}/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: "m", stream: true, messages: [{ role: "user", content: "hi" }] }),
  });
  const text = await r.text();
  assert("HTTP 200", r.status === 200);
  assert("Content-Type 保持 SSE", (r.headers.get("content-type") ?? "").includes("text/event-stream"));
  const dataLines = text.split("\n").filter((l) => l.startsWith("data: "));
  assert("有 5 个数据块(4 chunk + DONE)", dataLines.length === 5, `got ${dataLines.length}`);
  assert("chunk 无 success/data 包装", !dataLines[1].includes('"success"'));
  assert("流式内容拼接正确", text.includes("hello") && text.includes(" from mock"));
  assert("以 [DONE] 结尾", dataLines.at(-1) === "data: [DONE]");
  const usageChunk = JSON.parse(dataLines[dataLines.length - 2].slice(6));
  assert("流式 usage 解包", usageChunk.usage?.total_tokens === 9, JSON.stringify(usageChunk).slice(0, 80));
  // Cline 客户端行为：网关给流式请求注入 stream_options.include_usage
  const withStreamOptions = await post({ model: "m", stream: true, messages: [{ role: "user", content: "opt" }] });
  assert("流式请求注入 stream_options", mock.logs().includes('"include_usage":true') || mock.logs().includes("include_usage"), mock.logs());
  void withStreamOptions;
}

// ---------------------------------------------------------------- 3. 429 冷却：first 被冷却后跳过，second 连续成功
console.log("\n[3] 429 冷却：限流账号冷却期内被跳过");
{
  const before = mock.logs();
  const { status } = await post({ model: "m", messages: [{ role: "user", content: "x" }] });
  assert("HTTP 200", status === 200);
  const after = mock.logs();
  // 冷却生效的标志：网关日志出现 "in cooldown ... skipping"
  assert("网关跳过冷却账号", gw.logs().includes("in cooldown"), gw.logs());
  // first 的 429 计数不应增加（冷却期内未被再次调用）
  const count429 = (s) => (s.match(/429 for/g) ?? []).length;
  assert("冷却期内 first 未被再次调用", count429(after) === count429(before), `before=${count429(before)} after=${count429(after)}`);
}

// ---------------------------------------------------------------- 4. 401 + refresh 预刷新
console.log("\n[4] refresh：tokens.json 中带 refresh/expires(已过期) 的账号，请求前自动刷新");
{
  const tokenFile = join(TMP, "tokens2.json");
  const fs = await import("node:fs");
  fs.writeFileSync(tokenFile, JSON.stringify([
    { access: "workos:expired-access", refresh: "workos:expired-refresh", expires: Date.now() - 1000, email: "old@example.com" },
  ]));
  const gw2 = await start(GATEWAY, {
    PORT: String(9103),
    CLINE_API_BASE: `http://127.0.0.1:${MOCK_PORT}/api/v1`,
    CLINE_GATEWAY_TOKENS: tokenFile,
    CLINE_RATE_COOLDOWN_MS: "2000",
  });
  await waitReady("http://127.0.0.1:9103/health");
  // mock 需要支持 /auth/refresh
  const r = await fetch("http://127.0.0.1:9103/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: "m", messages: [{ role: "user", content: "hi" }] }),
  });
  const text = await r.text();
  assert("refresh 后请求成功", r.status === 200, text.slice(0, 120));
  assert("网关日志含 token 刷新成功", gw2.logs().includes("token 刷新成功"), gw2.logs());
  // 回写检查：access 已更新
  const saved = JSON.parse(fs.readFileSync(tokenFile, "utf8"));
  assert("刷新后 access 已回写文件", saved[0].access === "workos:refreshed-access", saved[0]?.access);
  stop(gw2.p);
}

// ---------------------------------------------------------------- 5. 模型列表
console.log("\n[5] /v1/models");
{
  const r = await fetch(`${BASE}/models`);
  const j = await r.json();
  assert("HTTP 200", r.status === 200);
  assert("包含免费模型", j.data.some((m) => m.id === "deepseek/deepseek-v4-flash"));
}

// ---------------------------------------------------------------- 6. 请求头重写（对齐 CPA ScrubProxyAndFingerprintHeaders）
console.log("\n[6] 请求头重写：下游头全部丢弃，上游头白名单重建");
{
  const before = mock.logs();
  const r = await fetch(`${BASE}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer hacked-key",            // 下游想顶替网关账号
      "X-Title": "Evil",                              // 想污染指纹头
      "X-Forwarded-For": "1.2.3.4",                   // 代理追踪
      "Sec-Fetch-Mode": "navigate",                   // 浏览器指纹
      "Accept-Encoding": "zstd",                      // 编码指纹
      "User-Agent": "curl/8.0",
      "X-Custom-Leak": "secret-header",
    },
    body: JSON.stringify({ model: "m", messages: [{ role: "user", content: "headers" }] }),
  });
  assert("HTTP 200", r.status === 200);
  const got = mock.logs().slice(before.length);
  assert("Authorization 已替换为网关 token(workos:)", got.includes('"authorization":"Bearer workos:'), mock.logs());
  assert("X-Title 已重写为 Cline", got.includes('"x-title":"Cline"'), got.match(/"x-title":[^,]*/)?.[0]);
  assert("X-Forwarded-For 已丢弃", !got.includes("1.2.3.4"));
  // 下游的 navigate 已丢弃；undici 自动注入的 cors 是 Node 原生形态（真实 Cline CLI 同样如此）
  assert("下游 Sec-Fetch-Mode 已丢弃", !got.includes("sec-fetch-mode\\\":\\\"navigate") && !got.includes("sec-fetch-mode\":\"navigate"));
  // 下游的 zstd 已丢弃；undici 自动注入的 gzip,deflate 是 Node 原生形态
  assert("下游 Accept-Encoding 已丢弃", !got.includes("zstd"));
  assert("User-Agent 已重写为 Cline 客户端", got.includes('"user-agent":"Cline/3.0.55"'), got.match(/"user-agent":[^,]*/)?.[0]);
  assert("下游自定义头已丢弃", !got.includes("x-custom-leak"));
  assert("指纹头已注入", got.includes('"x-client-type":"cline-cli"') && got.includes('"x-core-version":"0.0.75"'));
  assert("上游带 Content-Type", got.includes('"content-type":"application/json"'));
}

// ---------------------------------------------------------------- 7. 下游认证（可选）
console.log("\n[7] 下游认证：CLINE_GATEWAY_API_KEYS 配置后校验 Bearer");
{
  const gw3 = await start(GATEWAY, {
    PORT: String(9104),
    CLINE_API_BASE: `http://127.0.0.1:${MOCK_PORT}/api/v1`,
    CLINE_TOKEN: "first-token,second-token",
    CLINE_GATEWAY_API_KEYS: "secret-key-1,secret-key-2",
  });
  await waitReady("http://127.0.0.1:9104/health");
  const mk = (headers) =>
    fetch("http://127.0.0.1:9104/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ model: "m", messages: [{ role: "user", content: "hi" }] }),
    });

  let r = await mk({});
  assert("无 key -> 401", r.status === 401);
  r = await mk({ Authorization: "Bearer wrong" });
  assert("错误 key -> 401", r.status === 401);
  r = await mk({ Authorization: "Bearer secret-key-2" });
  assert("正确 key -> 200", r.status === 200);
  assert("下游 key 不转发给上游", !mock.logs().includes("secret-key-2"));
  stop(gw3.p);
}

// ---------------------------------------------------------------- 清理
stop(mock.p);
stop(gw.p);
console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);