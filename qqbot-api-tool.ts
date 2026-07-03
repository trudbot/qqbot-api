export type QQBotTarget =
  | { type: "c2c"; id: string }
  | { type: "group"; id: string }
  | { type: "channel"; id: string }
  | { type: "dm"; guildId: string };

export interface QQBotApiToolOptions {
  appId: string;
  clientSecret: string;
  userOpenid?: string;
  logger?: Pick<Console, "log" | "warn" | "error">;
  apiBaseUrl?: string;
  tokenBaseUrl?: string;
  intents?: number;
}

export interface QQBotMessageEvent {
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

export interface QQBotAttachment {
  content_type?: string;
  filename?: string;
  height?: number;
  width?: number;
  size?: number;
  url: string;
  [key: string]: unknown;
}

export interface QQBotMessageResponse {
  id: string;
  timestamp: string | number;
  ext_info?: {
    ref_idx?: string;
  };
}

export interface QQBotStreamOptions {
  openid?: string;
  chunkSize?: number;
  intervalMs?: number;
}

export type QQBotMessageHandler = (event: QQBotMessageEvent, bot: QQBotApiTool) => void | Promise<void>;

type WebSocketLike = {
  readonly readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (event: { code?: number; reason?: string }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
};

type WebSocketConstructor = new (url: string, protocols?: string | string[]) => WebSocketLike;

interface TokenCache {
  token: string;
  expiresAt: number;
}

interface WSPayload<T = unknown> {
  op: number;
  d?: T;
  s?: number;
  t?: string;
}

const DEFAULT_API_BASE = "https://api.sgroup.qq.com";
const DEFAULT_TOKEN_BASE = "https://bots.qq.com";

const WS_OPEN = 1;
const OP_DISPATCH = 0;
const OP_HEARTBEAT = 1;
const OP_IDENTIFY = 2;
const OP_RECONNECT = 7;
const OP_INVALID_SESSION = 9;
const OP_HELLO = 10;
const OP_HEARTBEAT_ACK = 11;

const INTENTS = {
  PUBLIC_GUILD_MESSAGES: 1 << 30,
  DIRECT_MESSAGE: 1 << 12,
  GROUP_AND_C2C: 1 << 25,
  INTERACTION: 1 << 26,
};

const DEFAULT_INTENTS =
  INTENTS.PUBLIC_GUILD_MESSAGES |
  INTENTS.DIRECT_MESSAGE |
  INTENTS.GROUP_AND_C2C |
  INTENTS.INTERACTION;

export class QQBotApiTool {
  private readonly appId: string;
  private readonly clientSecret: string;
  private readonly userOpenid?: string;
  private readonly logger: Pick<Console, "log" | "warn" | "error">;
  private readonly apiBaseUrl: string;
  private readonly tokenUrl: string;
  private readonly intents: number;
  private tokenCache: TokenCache | null = null;
  private tokenPromise: Promise<string> | null = null;
  private ws: WebSocketLike | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = true;
  private connecting = false;
  private lastSeq: number | null = null;
  private reconnectAttempts = 0;
  private messageHandlers: QQBotMessageHandler[] = [];

  constructor(options: QQBotApiToolOptions) {
    this.appId = String(options.appId ?? "").trim();
    this.clientSecret = String(options.clientSecret ?? "");
    if (!this.appId) throw new Error("QQBot appId is required");
    if (!this.clientSecret) throw new Error("QQBot clientSecret is required");

    this.userOpenid = options.userOpenid?.trim() || undefined;
    this.logger = options.logger ?? console;
    this.apiBaseUrl = trimTrailingSlash(options.apiBaseUrl ?? DEFAULT_API_BASE);
    this.tokenUrl = `${trimTrailingSlash(options.tokenBaseUrl ?? DEFAULT_TOKEN_BASE)}/app/getAppAccessToken`;
    this.intents = options.intents ?? DEFAULT_INTENTS;
  }

  onMessage(handler: QQBotMessageHandler): this {
    this.messageHandlers.push(handler);
    return this;
  }

  async start(): Promise<void> {
    if (!this.stopped) return;
    this.stopped = false;
    await this.connectWebSocket();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.ws?.close();
    this.ws = null;
  }

  async reply(event: QQBotMessageEvent, text: string): Promise<QQBotMessageResponse> {
    if (event.scene === "c2c" && event.openid) return this.sendC2CMessage(event.openid, text, event.messageId);
    if (event.scene === "group" && event.groupOpenid) return this.sendGroupMessage(event.groupOpenid, text, event.messageId);
    if (event.scene === "channel" && event.channelId) return this.sendChannelMessage(event.channelId, text, event.messageId);
    if (event.scene === "dm" && event.guildId) return this.sendDmMessage(event.guildId, text, event.messageId);
    throw new Error(`Cannot reply to event: missing target fields for scene ${event.scene}`);
  }

  async sendText(target: QQBotTarget, text: string): Promise<QQBotMessageResponse> {
    if (target.type === "c2c") return this.sendC2CMessage(target.id, text);
    if (target.type === "group") return this.sendGroupMessage(target.id, text);
    if (target.type === "channel") return this.sendChannelMessage(target.id, text);
    return this.sendDmMessage(target.guildId, text);
  }

  async sendPrivate(text: string, openid = this.userOpenid): Promise<QQBotMessageResponse> {
    if (!openid) throw new Error("sendPrivate requires userOpenid in constructor or as the second argument");
    return this.sendC2CMessage(openid, text);
  }

  async sendPrivateStream(text: string, options: QQBotStreamOptions = {}): Promise<QQBotMessageResponse> {
    const openid = options.openid ?? this.userOpenid;
    if (!openid) throw new Error("sendPrivateStream requires userOpenid in constructor or options.openid");
    return this.sendC2CStreamMessage(openid, text, options);
  }

  private async getAccessToken(): Promise<string> {
    const cached = this.tokenCache;
    const refreshAheadMs = cached ? Math.min(5 * 60 * 1000, (cached.expiresAt - Date.now()) / 3) : 0;
    if (cached && Date.now() < cached.expiresAt - refreshAheadMs) return cached.token;
    if (this.tokenPromise) return this.tokenPromise;

    this.tokenPromise = (async () => {
      try {
        const response = await fetch(this.tokenUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "User-Agent": this.userAgent(),
          },
          body: JSON.stringify({ appId: this.appId, clientSecret: this.clientSecret }),
        });
        const text = await response.text();
        const data = parseJson<{ access_token?: string; expires_in?: number }>(text);
        if (!response.ok || !data?.access_token) {
          throw new Error(`Failed to get access token: HTTP ${response.status} ${text.slice(0, 500)}`);
        }
        this.tokenCache = {
          token: data.access_token,
          expiresAt: Date.now() + (data.expires_in ?? 7200) * 1000,
        };
        return data.access_token;
      } finally {
        this.tokenPromise = null;
      }
    })();

    return this.tokenPromise;
  }

  private clearAccessToken(): void {
    this.tokenCache = null;
    this.tokenPromise = null;
  }

  private async connectWebSocket(): Promise<void> {
    if (this.stopped || this.connecting) return;
    this.connecting = true;

    try {
      const token = await this.getAccessToken();
      const gatewayUrl = await this.getGatewayUrl(token);
      const WebSocketCtor = getWebSocketConstructor();
      const ws = new WebSocketCtor(gatewayUrl);
      this.ws = ws;

      ws.addEventListener("open", () => {
        this.connecting = false;
        this.reconnectAttempts = 0;
        this.logger.log("[qqbot] WebSocket connected");
      });

      ws.addEventListener("message", (event) => {
        webSocketDataToString(event.data).then((raw) => this.handleWebSocketMessage(raw, token)).catch((err) => {
          this.logger.error(`[qqbot] WebSocket message failed: ${formatError(err)}`);
        });
      });

      ws.addEventListener("close", (event) => {
        this.logger.warn(`[qqbot] WebSocket closed: ${event.code ?? ""} ${event.reason ?? ""}`.trim());
        this.cleanupWebSocket();
        this.scheduleReconnect();
      });

      ws.addEventListener("error", (event) => {
        this.logger.error(`[qqbot] WebSocket error: ${formatError(event)}`);
      });
    } catch (err) {
      this.connecting = false;
      this.logger.error(`[qqbot] WebSocket connect failed: ${formatError(err)}`);
      this.scheduleReconnect();
    }
  }

  private async handleWebSocketMessage(raw: string, accessToken: string): Promise<void> {
    const payload = parseJson<WSPayload>(raw);
    if (!payload || typeof payload.op !== "number") return;
    if (typeof payload.s === "number") this.lastSeq = payload.s;

    if (payload.op === OP_HELLO) {
      const interval = (payload.d as { heartbeat_interval?: number } | undefined)?.heartbeat_interval;
      this.identify(accessToken);
      this.startHeartbeat(interval ?? 45_000);
      return;
    }

    if (payload.op === OP_DISPATCH) {
      if (payload.t === "READY") {
        this.logger.log("[qqbot] WebSocket ready");
        return;
      }
      await this.dispatchPayload(payload);
      return;
    }

    if (payload.op === OP_RECONNECT || payload.op === OP_INVALID_SESSION) {
      this.logger.warn(`[qqbot] WebSocket requested reconnect: op=${payload.op}`);
      this.ws?.close();
      this.scheduleReconnect(1_000);
      return;
    }

    if (payload.op === OP_HEARTBEAT_ACK) return;
  }

  private identify(accessToken: string): void {
    this.sendWebSocketPayload({
      op: OP_IDENTIFY,
      d: {
        token: `QQBot ${accessToken}`,
        intents: this.intents,
        shard: [0, 1],
      },
    });
  }

  private startHeartbeat(intervalMs: number): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      this.sendWebSocketPayload({ op: OP_HEARTBEAT, d: this.lastSeq });
    }, intervalMs);
  }

  private sendWebSocketPayload(payload: unknown): void {
    if (this.ws?.readyState === WS_OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  private scheduleReconnect(delayMs?: number): void {
    if (this.stopped || this.reconnectTimer) return;
    const delays = [1000, 2000, 5000, 10000, 30000, 60000];
    const delay = delayMs ?? delays[Math.min(this.reconnectAttempts, delays.length - 1)];
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectWebSocket().catch((err) => {
        this.logger.error(`[qqbot] reconnect failed: ${formatError(err)}`);
      });
    }, delay);
  }

  private cleanupWebSocket(): void {
    this.connecting = false;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.ws = null;
  }

  private async dispatchPayload(payload: WSPayload): Promise<void> {
    const message = normalizeMessageEvent(payload);
    if (!message) return;
    await Promise.all(this.messageHandlers.map((handler) => Promise.resolve(handler(message, this))));
  }

  private async getGatewayUrl(accessToken: string): Promise<string> {
    const data = await this.apiRequest<{ url: string }>("GET", "/gateway", undefined, accessToken);
    if (!data.url) throw new Error("QQ gateway response missing url");
    return data.url;
  }

  private async sendC2CMessage(openid: string, content: string, msgId?: string): Promise<QQBotMessageResponse> {
    return this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, buildTextBody(content, msgId));
  }

  private async sendC2CStreamMessage(openid: string, content: string, options: QQBotStreamOptions): Promise<QQBotMessageResponse> {
    const chunks = splitByLines(content, options.chunkSize ?? 50);
    const intervalMs = options.intervalMs ?? 100;
    let streamId: string | null = null;
    let msgSeq = 1;

    for (let index = 0; index < chunks.length; index += 1) {
      const chunkResponse: QQBotMessageResponse = await this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, {
        msg_type: 2,
        markdown: { content: chunks[index] },
        msg_seq: msgSeq,
        stream: {
          state: 1,
          id: streamId,
          index,
          reset: false,
        },
      });
      streamId = chunkResponse.id ?? streamId;
      msgSeq += 1;
      if (intervalMs > 0) await sleep(intervalMs);
    }

    return this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, {
      msg_type: 2,
      markdown: { content },
      msg_seq: msgSeq,
      stream: {
        state: 10,
        id: streamId,
        index: 1,
        reset: true,
      },
    });
  }

  private async sendGroupMessage(groupOpenid: string, content: string, msgId?: string): Promise<QQBotMessageResponse> {
    return this.apiRequest("POST", `/v2/groups/${encodeURIComponent(groupOpenid)}/messages`, buildTextBody(content, msgId));
  }

  private async sendChannelMessage(channelId: string, content: string, msgId?: string): Promise<QQBotMessageResponse> {
    return this.apiRequest("POST", `/channels/${encodeURIComponent(channelId)}/messages`, buildTextBody(content, msgId));
  }

  private async sendDmMessage(guildId: string, content: string, msgId?: string): Promise<QQBotMessageResponse> {
    return this.apiRequest("POST", `/dms/${encodeURIComponent(guildId)}/messages`, buildTextBody(content, msgId));
  }

  private async apiRequest<T>(method: string, path: string, body?: unknown, token?: string): Promise<T> {
    const accessToken = token ?? await this.getAccessToken();
    const response = await fetch(`${this.apiBaseUrl}${path}`, {
      method,
      headers: {
        "Authorization": `QQBot ${accessToken}`,
        "Content-Type": "application/json",
        "User-Agent": this.userAgent(),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    const data = text ? parseJson<unknown>(text) : {};
    if (!response.ok) {
      if (response.status === 401) this.clearAccessToken();
      throw new Error(`QQ API ${method} ${path} failed: HTTP ${response.status} ${text.slice(0, 500)}`);
    }
    return data as T;
  }

  private userAgent(): string {
    return `QQBotApiTool/1.0 Node/${process.versions.node}`;
  }
}

export function normalizeMessageEvent(payload: WSPayload): QQBotMessageEvent | null {
  const eventType = payload.t ?? "";
  const data = payload.d as Record<string, any> | undefined;
  if (!data) return null;

  if (eventType === "C2C_MESSAGE_CREATE") {
    return {
      eventType,
      scene: "c2c",
      messageId: String(data.id ?? ""),
      text: String(data.content ?? ""),
      timestamp: data.timestamp,
      senderId: data.author?.id,
      openid: data.author?.user_openid ?? data.author?.id,
      attachments: Array.isArray(data.attachments) ? data.attachments : [],
      raw: data,
    };
  }

  if (eventType === "GROUP_AT_MESSAGE_CREATE" || eventType === "GROUP_MESSAGE_CREATE") {
    return {
      eventType,
      scene: "group",
      messageId: String(data.id ?? ""),
      text: String(data.content ?? ""),
      timestamp: data.timestamp,
      senderId: data.author?.member_openid ?? data.author?.id,
      openid: data.author?.member_openid ?? data.author?.id,
      groupOpenid: data.group_openid ?? data.group_id,
      attachments: Array.isArray(data.attachments) ? data.attachments : [],
      raw: data,
    };
  }

  if (eventType === "AT_MESSAGE_CREATE") {
    return {
      eventType,
      scene: "channel",
      messageId: String(data.id ?? ""),
      text: String(data.content ?? ""),
      timestamp: data.timestamp,
      senderId: data.author?.id,
      channelId: data.channel_id,
      guildId: data.guild_id,
      attachments: Array.isArray(data.attachments) ? data.attachments : [],
      raw: data,
    };
  }

  if (eventType === "DIRECT_MESSAGE_CREATE") {
    return {
      eventType,
      scene: "dm",
      messageId: String(data.id ?? ""),
      text: String(data.content ?? ""),
      timestamp: data.timestamp,
      senderId: data.author?.id,
      guildId: data.guild_id,
      attachments: Array.isArray(data.attachments) ? data.attachments : [],
      raw: data,
    };
  }

  return null;
}

function buildTextBody(content: string, msgId?: string): Record<string, unknown> {
  if (!content.trim()) throw new Error("message content is required");
  return {
    content,
    msg_type: 0,
    msg_seq: msgId ? Date.now() % 1_000_000 : 1,
    ...(msgId ? { msg_id: msgId } : {}),
  };
}

function splitByLines(text: string, maxChars: number): string[] {
  const chunks: string[] = [];
  let current = "";
  const lines = text.split("\n");

  for (let i = 0; i < lines.length; i += 1) {
    const piece = i < lines.length - 1 ? `${lines[i]}\n` : lines[i];
    if (current.length + piece.length <= maxChars) {
      current += piece;
      continue;
    }
    if (current) chunks.push(current.endsWith("\n") ? current : `${current}\n`);
    current = piece;
  }

  if (current) chunks.push(current.endsWith("\n") ? current : `${current}\n`);
  return chunks;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getWebSocketConstructor(): WebSocketConstructor {
  const WebSocketCtor = (globalThis as unknown as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (!WebSocketCtor) throw new Error("global WebSocket is unavailable; run with Node.js 24+ or newer");
  return WebSocketCtor;
}

async function webSocketDataToString(data: unknown): Promise<string> {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (data instanceof Blob) return data.text();
  return String(data);
}

function parseJson<T>(text: string): T | null {
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

function formatError(err: unknown): string {
  if (err instanceof Error) return err.message;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const bot = new QQBotApiTool({
    appId: process.env.QQBOT_APP_ID ?? "",
    clientSecret: process.env.QQBOT_CLIENT_SECRET ?? "",
  });

  bot.onMessage(async (event, api) => {
    console.log(`[qqbot] ${event.scene} ${event.openid ?? event.groupOpenid ?? event.channelId ?? event.guildId}: ${event.text}`);
    if (event.text.trim() === "/ping") {
      await api.reply(event, "pong");
    }
  });

  bot.start().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
