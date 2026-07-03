# qqbot-api

一个极简 TypeScript QQ Bot API 工具类，基于 QQ Bot WebSocket Gateway 接收消息，并提供简单的发送 API。

特点：

- 只支持 WebSocket 模式，本地运行即可接收 QQ 消息，不需要公网 Webhook。
- 不依赖当前 OpenClaw 仓库代码。
- 不依赖第三方 WebSocket 包，要求 Node.js 24+。
- 对外 API 尽量简单：`start`、`stop`、`onMessage`、`reply`、`sendPrivate`、`sendPrivateStream`、`sendText`。

## 环境要求

- Node.js 24+
- QQ Bot 的 `appId` 和 `clientSecret`
- QQ Bot 已开通相应事件权限，例如私聊、群聊、频道消息等

## 安装

```bash
npm install
```

本项目只把 `tsx` 和 Node 类型作为开发依赖，核心文件 `qqbot-api-tool.ts` 本身不依赖第三方运行时包。

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

`QQBOT_USER_OPENID` 不是由 `appId` / `clientSecret` 计算出来的。它来自 QQ 消息事件里的 `author.user_openid`。如果要主动私信，必须先知道目标用户 openid，或者在收到消息后自己保存。

## 最小接收和回复示例

```ts
import { QQBotApiTool } from "./qqbot-api-tool.ts";

const bot = new QQBotApiTool({
  appId: process.env.QQBOT_APP_ID!,
  clientSecret: process.env.QQBOT_CLIENT_SECRET!,
});

bot.onMessage(async (event, api) => {
  console.log("收到消息", event.scene, event.text);

  await api.reply(event, "你好");
});

await bot.start();
```

运行后，本地程序会主动连接 QQ WebSocket Gateway。QQ 平台会沿这条连接推送消息，所以不需要把本机 localhost 暴露给公网。

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

## 通用文本发送

```ts
await bot.sendText({ type: "c2c", id: "USER_OPENID" }, "你好");
await bot.sendText({ type: "group", id: "GROUP_OPENID" }, "群消息");
await bot.sendText({ type: "channel", id: "CHANNEL_ID" }, "频道消息");
await bot.sendText({ type: "dm", guildId: "GUILD_ID" }, "频道私信");
```

## 消息事件结构

`onMessage` 收到的事件大致如下：

```ts
interface QQBotMessageEvent {
  eventType: string;
  scene: "c2c" | "group" | "channel" | "dm";
  messageId: string;
  text: string;
  timestamp?: string;
  senderId?: string;
  openid?: string;
  groupOpenid?: string;
  channelId?: string;
  guildId?: string;
  attachments: QQBotAttachment[];
  raw: unknown;
}
```

常用字段：

- `event.text`：消息文本。
- `event.scene`：消息场景。
- `event.openid`：私聊用户或群成员 openid。
- `event.groupOpenid`：群 openid。
- `event.messageId`：回复当前消息时使用。
- `event.raw`：QQ 原始事件数据。

## API 速览

```ts
const bot = new QQBotApiTool(options);

bot.onMessage(handler);
await bot.start();
bot.stop();

await bot.reply(event, "你好");
await bot.sendPrivate("你好");
await bot.sendPrivateStream(markdownText);
await bot.sendText({ type: "c2c", id: "USER_OPENID" }, "你好");
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
- 本工具类只封装核心文本与流式文本能力，图片、语音、文件等富媒体能力未包含。
