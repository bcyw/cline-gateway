# cline-gateway

本地 OpenAI 兼容网关：把 **Cline 免费模型**（`api.cline.bot` + OAuth token）桥接给任意 OpenAI 兼容客户端。零依赖、单文件、Node >= 20。

任何 OpenAI 兼容客户端（Cline OpenAI Compatible provider、CherryStudio、NextChat、opencode、curl……）指向本机一个端点即可使用 Cline 免费模型，无需把 Cline 登录态交给第三方。

## 快速开始

```bash
# 1. 登录（WorkOS 设备流，浏览器授权一次，凭证写入 config/tokens.json，0600 权限）
node oauth.js

# 2. 启动网关（默认端口 3000）
node server.js
```

客户端配置：

```
Base URL: http://127.0.0.1:3000/v1
API Key:  任意非空字符串（本地网关默认不校验）
Model:    deepseek/deepseek-v4-flash   （或 /v1/models 列出的任意模型）
```

## 能力

- `POST /v1/chat/completions` — 转发上游，流式/非流式均支持
- `GET /v1/models` — 实时暴露上游真实模型清单（`recommended-models` 接口）
- `GET /health` — 健康检查

### 多账号轮询

- round-robin 起点轮换；HTTP 429（免费配额限流）进入冷却并切换账号
- 过期前 5 分钟自动刷新（复刻 Cline `isCredentialLikelyExpired`），per-account 锁避免并发刷新竞争
- HTTP 401 兜底 refresh；refresh 失败返回错误提示重新登录

### 请求头重写（对齐 CLIProxyAPI 实践）

下游请求头一律不信任，转发到上游时**白名单重建**，只保留 Cline 客户端应有的头：

- `Authorization: Bearer workos:<token>` 由网关统一注入 —— 下游永远无法用自定义 key 顶替网关账号
- 指纹头（`X-CLIENT-TYPE: cline-cli`、`X-PLATFORM: cli`、`X-CORE-VERSION`、`X-Task-ID`、`User-Agent: Cline/3.0.55` 等）由网关注入 —— 服务端据此区分产品界面
- 下游的代理追踪头（`X-Forwarded-*`）、身份头（`X-Title`、`Referer`、`X-Stainless-*`）、浏览器指纹头（`Sec-Ch-Ua*`、`Sec-Fetch-*`）、`Accept-Encoding` 等全部丢弃

### 响应处理

- 自动解包上游 `{success, data}` 信封（流式逐块解包）
- hop-by-hop 头（`Connection`、`Transfer-Encoding`、`Upgrade` 等）与 `Set-Cookie` 不透传
- 流式请求自动注入 `stream_options: { include_usage: true }`（对齐 Cline 客户端行为）

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `3000` | 监听端口 |
| `CLINE_API_BASE` | `https://api.cline.bot/api/v1` | 上游地址 |
| `CLINE_TOKEN` | 无 | 逗号分隔多 token（优先级最高） |
| `CLINE_GATEWAY_TOKENS` | `<项目>/config/tokens.json` | token 文件路径 |
| `CLINE_PROVIDERS_JSON` | `~/.cline/data/settings/providers.json` | Cline 客户端登录态（只取 access） |
| `CLINE_FINGERPRINT` | `true` | `false` 关闭指纹头 |
| `CLINE_RATE_COOLDOWN_MS` | `60000` | 429 冷却时长 |
| `CLINE_REFRESH_BUFFER_MS` | `300000` | 过期前刷新缓冲 |
| `CLINE_GATEWAY_API_KEYS` | 无 | 可选下游认证：逗号分隔 key，配置后校验 `Bearer`；未配置 = 无认证（本地信任） |
| `CLINE_CLIENT_VERSION` | `3.0.55` | 指纹头版本号 |

## 测试

```bash
node test/test-gateway.mjs    # mock 上游 + 网关全链路，36 项断言
```

覆盖：非流式/流式/信封解包、429 轮询与冷却、401 预刷新与回写、模型列表、请求头重写（下游头丢弃 + 白名单重建）、可选下游认证。

## 安全说明

- 真实 token 存于 `config/tokens.json`（0600），已加入 `.gitignore`，不会提交
- 可选下游认证：设置 `CLINE_GATEWAY_API_KEYS` 后所有端点（含 `/health`）校验 Bearer；未设置时仅建议绑定内网（默认 `127.0.0.1`）

## License

MIT