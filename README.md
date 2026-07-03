# qqbot-api

一个极简 TypeScript QQ Bot 私聊工具类：基于 QQ Bot WebSocket Gateway 在本地接收 C2C 私聊消息，并提供私聊文本、流式 Markdown、图片发送 API。

特点：

- 只支持 WebSocket 模式，本地运行即可接收 QQ 私聊消息，不需要公网 Webhook。
- 只封装 C2C 私聊能力，避免群聊、频道、DM 等额外复杂度。
- 不依赖 OpenClaw 仓库代码。
- 不依赖第三方 WebSocket 包，要求 Node.js 24+。
- 对外 API 简洁：`start`、`stop`、`onMessage`、`reply`、`replyImage`、`sendPrivate`、`sendPrivateImage`、`sendPrivateStream`。

## 环境要求

- Node.js 24+
- QQ Bot 的 `appId` 和 `clientSecret`
- QQ Bot 已开通 C2C 私聊消息事件权限

## 安装

```bash
npm install
```

本项目只把 `tsx`、`typescript` 和 Node 类型作为开发依赖，核心文件 `qqbot-api-tool.ts` 本身不依赖第三方运行时包。

## 配置

复制环境变量示例：

```bash
cp .env.example .env
```

填写：

```env
QQBOT_APP_ID=你的 appId
QQBOT_CLIENT_SECRET=你的 appSecret
QQBOT_USER_OPENID=可选，默认私聊用户 openid
```

`QQBOT_USER_OPENID` 不是由 `appId` / `clientSecret` 计算出来的。它来自 QQ 私聊消息事件里的 `author.user_openid`。如果要主动私信，必须先知道目标用户 openid，或者在收到消息后自己保存。

## 最小接收和回复示例

```ts
import { QQBotApiTool } from "./qqbot-api-tool.ts";

const bot = new QQBotApiTool({
  appId: process.env.QQBOT_APP_ID!,
  clientSecret: process.env.QQBOT_CLIENT_SECRET!,
});

bot.onMessage(async (event, api) => {
  console.log("收到私聊", event.openid, event.text);

  if (event.text.trim() === "/ping") {
    await api.reply(event, "pong");
  }
});

await bot.start();
```

运行后，本地程序会主动连接 QQ WebSocket Gateway。QQ 平台会沿这条连接推送私聊消息，所以不需要把本机 localhost 暴露给公网。

## 回复图片

`replyImage` 支持公网图片 URL 或本地图片路径：

```ts
bot.onMessage(async (event, api) => {
  if (event.text.trim() === "/image") {
    await api.replyImage(event, "./demo.png");
  }
});
```

使用 URL：

```ts
await api.replyImage(event, "https://example.com/demo.png");
```

## 主动发送私信

如果构造时传了默认用户 openid：

```ts
const bot = new QQBotApiTool({
  appId: process.env.QQBOT_APP_ID!,
  clientSecret: process.env.QQBOT_CLIENT_SECRET!,
  userOpenid: process.env.QQBOT_USER_OPENID,
});

await bot.sendPrivate("你好");
```

也可以临时指定 openid：

```ts
await bot.sendPrivate("你好", "USER_OPENID");
```

## 主动发送图片私信

发送本地图片：

```ts
await bot.sendPrivateImage("./demo.png");
```

发送公网图片 URL：

```ts
await bot.sendPrivateImage("https://example.com/demo.png", "USER_OPENID");
```

## 流式发送私信

参考 QQ Bot markdown stream 消息接口实现。

```ts
await bot.sendPrivateStream(`# 流式消息

你好，这是一条流式 Markdown 私信。`);
```

自定义分片大小和间隔：

```ts
await bot.sendPrivateStream(markdownText, {
  chunkSize: 50,
  intervalMs: 100,
});
```

临时指定 openid：

```ts
await bot.sendPrivateStream(markdownText, {
  openid: "USER_OPENID",
});
```

流式发送流程：

1. 按行切分文本。
2. 多次发送 `state: 1` 的 markdown stream 分片。
3. 最后发送 `state: 10`、`reset: true` 的完整文本收尾。

## 消息事件结构

`onMessage` 只会收到 C2C 私聊事件：

```ts
interface QQBotMessageEvent {
  messageId: string;
  text: string;
  openid: string;
  timestamp?: string;
  attachments: QQBotAttachment[];
  raw: unknown;
}
```

常用字段：

- `event.text`：消息文本。
- `event.openid`：私聊用户 openid。
- `event.messageId`：回复当前消息时使用。
- `event.attachments`：QQ 事件里携带的附件信息。
- `event.raw`：QQ 原始事件数据。

## API 速览

```ts
const bot = new QQBotApiTool(options);

bot.onMessage(handler);
await bot.start();
bot.stop();

await bot.reply(event, "你好");
await bot.replyImage(event, "./demo.png");
await bot.sendPrivate("你好");
await bot.sendPrivateImage("./demo.png");
await bot.sendPrivateStream(markdownText);
```

## 类型检查

```bash
npm run typecheck
```

## 注意事项

- `openid` 不能通过 `appId` 和 `clientSecret` 拼接或计算得到。
- 主动私信需要目标用户 openid。
- QQ 平台可能限制主动消息发送频率和场景。
- 流式消息当前封装的是 C2C 私信 markdown stream。
- 图片发送会先调用 `/v2/users/{openid}/files` 获取 `file_info`，再发送 `msg_type: 7` 消息。
- 本工具类只保留 C2C 私聊能力；群聊、频道、频道私信不在当前封装范围内。
