export type QQBotTokenProvider = () => string | Promise<string>;

export type QQBotImageInput =
  | string
  | Blob
  | ArrayBuffer
  | Uint8Array
  | {
      base64: string;
      filename?: string;
      contentType?: string;
    };

export interface QQBotApiToolOptions {
  appId?: string;
  clientSecret?: string;
  accessToken?: string;
  tokenProvider?: QQBotTokenProvider;
  userOpenid?: string;
  logger?: Pick<Console, "log" | "warn" | "error">;
  apiBaseUrl?: string;
  tokenBaseUrl?: string;
}

export interface QQBotMessageEvent {
  messageId: string;
  text: string;
  openid: string;
  timestamp?: string;
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

export interface QQBotTextStream {
  write(chunk: string): Promise<QQBotMessageResponse>;
  close(finalText?: string): Promise<QQBotMessageResponse>;
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
const C2C_INTENT = 1 << 25;
const IMAGE_FILE_TYPE = 1;

export class QQBotApiTool {
  private readonly appId?: string;
  private readonly clientSecret?: string;
  private readonly staticAccessToken?: string;
  private readonly tokenProvider?: QQBotTokenProvider;
  private readonly userOpenid?: string;
  private readonly logger: Pick<Console, "log" | "warn" | "error">;
  private readonly apiBaseUrl: string;
  private readonly tokenUrl: string;
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
    this.appId = options.appId === undefined ? undefined : String(options.appId).trim();
    this.clientSecret = options.clientSecret === undefined ? undefined : String(options.clientSecret);
    this.staticAccessToken = options.accessToken?.trim() || undefined;
    this.tokenProvider = options.tokenProvider;
    if (!this.staticAccessToken && !this.tokenProvider) {
      if (!this.appId) throw new Error("QQBot appId is required unless accessToken or tokenProvider is provided");
      if (!this.clientSecret) throw new Error("QQBot clientSecret is required unless accessToken or tokenProvider is provided");
    }

    this.userOpenid = options.userOpenid?.trim() || undefined;
    this.logger = options.logger ?? console;
    this.apiBaseUrl = trimTrailingSlash(options.apiBaseUrl ?? DEFAULT_API_BASE);
    this.tokenUrl = `${trimTrailingSlash(options.tokenBaseUrl ?? DEFAULT_TOKEN_BASE)}/app/getAppAccessToken`;
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
    return this.sendPrivateText(event.openid, text, event.messageId);
  }

  async replyImage(event: QQBotMessageEvent, image: QQBotImageInput): Promise<QQBotMessageResponse> {
    return this.sendPrivateImageTo(event.openid, image, event.messageId);
  }

  async replyStream(event: QQBotMessageEvent, text: string, options: Omit<QQBotStreamOptions, "openid"> = {}): Promise<QQBotMessageResponse> {
    return this.sendPrivateStreamTo(event.openid, text, options, event.messageId);
  }

  createReplyStream(event: QQBotMessageEvent, options: Omit<QQBotStreamOptions, "openid"> = {}): QQBotTextStream {
    return this.createPrivateStreamTo(event.openid, options, event.messageId);
  }

  async sendPrivate(text: string, openid = this.userOpenid): Promise<QQBotMessageResponse> {
    if (!openid) throw new Error("sendPrivate requires userOpenid in constructor or as the second argument");
    return this.sendPrivateText(openid, text);
  }

  async sendPrivateImage(image: QQBotImageInput, openid = this.userOpenid): Promise<QQBotMessageResponse> {
    if (!openid) throw new Error("sendPrivateImage requires userOpenid in constructor or as the second argument");
    return this.sendPrivateImageTo(openid, image);
  }

  async sendPrivateStream(text: string, options: QQBotStreamOptions = {}): Promise<QQBotMessageResponse> {
    const openid = options.openid ?? this.userOpenid;
    if (!openid) throw new Error("sendPrivateStream requires userOpenid in constructor or options.openid");
    return this.sendPrivateStreamTo(openid, text, options);
  }

  createPrivateStream(openid = this.userOpenid, options: Omit<QQBotStreamOptions, "openid"> = {}): QQBotTextStream {
    if (!openid) throw new Error("createPrivateStream requires userOpenid in constructor or as the first argument");
    return this.createPrivateStreamTo(openid, options);
  }

  private async getAccessToken(): Promise<string> {
    if (this.staticAccessToken) return this.staticAccessToken;
    if (this.tokenProvider) return this.tokenProvider();

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
        intents: C2C_INTENT,
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

  private async sendPrivateText(openid: string, content: string, msgId?: string): Promise<QQBotMessageResponse> {
    return this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, buildTextBody(content, msgId));
  }

  private async sendPrivateImageTo(openid: string, image: QQBotImageInput, msgId?: string): Promise<QQBotMessageResponse> {
    const fileInfo = await this.uploadPrivateImage(openid, image);
    return this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, {
      msg_type: 7,
      msg_seq: msgId ? Date.now() % 1_000_000 : 1,
      media: { file_info: fileInfo },
      ...(msgId ? { msg_id: msgId } : {}),
    });
  }

  private async uploadPrivateImage(openid: string, image: QQBotImageInput): Promise<string> {
    const body = typeof image === "string" && isHttpUrl(image)
      ? {
          file_type: IMAGE_FILE_TYPE,
          url: image,
          srv_send_msg: false,
        }
      : {
          file_type: IMAGE_FILE_TYPE,
          file_data: await imageToBase64(image),
          srv_send_msg: false,
          ...imageFileName(image),
        };

    const result = await this.apiRequest<{ file_info?: string }>("POST", `/v2/users/${encodeURIComponent(openid)}/files`, body);
    if (!result.file_info) throw new Error("QQ image upload response missing file_info");
    return result.file_info;
  }

  private createPrivateStreamTo(openid: string, options: Omit<QQBotStreamOptions, "openid">, msgId?: string): QQBotTextStream {
    let streamId: string | null = null;
    let msgSeq = 1;
    let index = 0;
    let content = "";
    let closed = false;

    return {
      write: async (chunk: string) => {
        if (closed) throw new Error("stream is already closed");
        if (!chunk) throw new Error("stream chunk is required");
        content += chunk;
        const response: QQBotMessageResponse = await this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, {
          msg_type: 2,
          markdown: { content: chunk },
          msg_seq: msgSeq,
          ...(msgId ? { msg_id: msgId } : {}),
          stream: {
            state: 1,
            id: streamId,
            index,
            reset: false,
          },
        });
        streamId = response.id ?? streamId;
        msgSeq += 1;
        index += 1;
        const intervalMs = options.intervalMs ?? 0;
        if (intervalMs > 0) await sleep(intervalMs);
        return response;
      },
      close: async (finalText = content) => {
        if (closed) throw new Error("stream is already closed");
        closed = true;
        return this.apiRequest("POST", `/v2/users/${encodeURIComponent(openid)}/messages`, {
          msg_type: 2,
          markdown: { content: finalText },
          msg_seq: msgSeq,
          ...(msgId ? { msg_id: msgId } : {}),
          stream: {
            state: 10,
            id: streamId,
            index: 1,
            reset: true,
          },
        });
      },
    };
  }

  private async sendPrivateStreamTo(openid: string, content: string, options: Omit<QQBotStreamOptions, "openid">, msgId?: string): Promise<QQBotMessageResponse> {
    const chunks = splitByLines(content, options.chunkSize ?? 50);
    const stream = this.createPrivateStreamTo(openid, { ...options, intervalMs: options.intervalMs ?? 100 }, msgId);
    for (const chunk of chunks) await stream.write(chunk);
    return stream.close(content);
  }

  private async apiRequest<T>(method: string, path: string, body?: unknown, token?: string): Promise<T> {
    const accessToken = token ?? await this.getAccessToken();
    const response = await fetch(`${this.apiBaseUrl}${path}`, {
      method,
      headers: {
        "Authorization": `QQBot ${accessToken}`,
        "Content-Type": "application/json",
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

}

export function normalizeMessageEvent(payload: WSPayload): QQBotMessageEvent | null {
  if (payload.t !== "C2C_MESSAGE_CREATE") return null;
  const data = payload.d as Record<string, any> | undefined;
  const openid = data?.author?.user_openid ?? data?.author?.id;
  if (!data || !openid) return null;

  return {
    messageId: String(data.id ?? ""),
    text: String(data.content ?? ""),
    timestamp: data.timestamp,
    openid,
    attachments: Array.isArray(data.attachments) ? data.attachments : [],
    raw: data,
  };
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
  if (!WebSocketCtor) throw new Error("global WebSocket is unavailable");
  return WebSocketCtor;
}

async function webSocketDataToString(data: unknown): Promise<string> {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (typeof Blob !== "undefined" && data instanceof Blob) return data.text();
  return String(data);
}

function isHttpUrl(value: string): boolean {
  return value.startsWith("http://") || value.startsWith("https://");
}

async function imageToBase64(image: QQBotImageInput): Promise<string> {
  if (typeof image === "string") return stripDataUrlPrefix(image);
  if (image instanceof ArrayBuffer) return bytesToBase64(new Uint8Array(image));
  if (image instanceof Uint8Array) return bytesToBase64(image);
  if (typeof Blob !== "undefined" && image instanceof Blob) return bytesToBase64(new Uint8Array(await image.arrayBuffer()));
  if (isBase64ImageObject(image)) return stripDataUrlPrefix(image.base64);
  throw new Error("unsupported image input");
}

function imageFileName(image: QQBotImageInput): { file_name?: string } {
  if (isBase64ImageObject(image) && image.filename) return { file_name: image.filename };
  return {};
}

function isBase64ImageObject(image: QQBotImageInput): image is { base64: string; filename?: string; contentType?: string } {
  return typeof image === "object" && image !== null && "base64" in image;
}

function stripDataUrlPrefix(value: string): string {
  const marker = ";base64,";
  const index = value.indexOf(marker);
  return index === -1 ? value : value.slice(index + marker.length);
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    const chunk = bytes.subarray(offset, offset + chunkSize);
    binary += String.fromCharCode(...chunk);
  }

  const btoaFn = (globalThis as unknown as { btoa?: (value: string) => string }).btoa;
  if (!btoaFn) throw new Error("global btoa is unavailable; pass image as base64 string instead");
  return btoaFn(binary);
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
