// Cloudflare Worker + Durable Object: chimp-live
// モメンタムチンパン v5 🔥ライブ（資金フロー）のリアルタイム配信ハブ。
//
// 流れ: PC(tachibana_live_flow.py) が毎分 POST /publish（Bearerトークン）
//       → Durable Object "LiveHub" がメモリ＋ストレージに保持
//       → アプリは GET /live.json（ポーリング）か GET /ws（WebSocket push）で受け取る。
//
// なぜKVでなくDurable Object: KVは書込→読出の反映に最大60秒の遅延があり
// 「毎分更新」と合わせると1〜2分古くなる。DOは強整合＋WebSocketで即時配信できる。
// 無料プランでもSQLiteバックのDOは使える（2025〜）。
//
// 環境変数（wrangler secret put PUBLISH_TOKEN）: PC側の .tachibana/chimp_live_token.txt と一致させる。

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const id = env.LIVE_HUB.idFromName("main");
    const hub = env.LIVE_HUB.get(id);
    if (url.pathname === "/publish") {
      if (request.method !== "POST") return json({ error: "POST only" }, 405);
      const auth = request.headers.get("Authorization") || "";
      if (!env.PUBLISH_TOKEN || auth !== `Bearer ${env.PUBLISH_TOKEN}`) return json({ error: "forbidden" }, 403);
      return hub.fetch(request);
    }
    if (url.pathname === "/live.json" || url.pathname === "/ws" || url.pathname === "/status") {
      return hub.fetch(request);
    }
    return json({ ok: true, service: "chimp-live", routes: ["/live.json", "/ws", "/status", "POST /publish"] });
  },
};

function json(obj, status = 200, extra = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", ...CORS, ...extra },
  });
}

export class LiveHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.latest = null;      // 最新JSON文字列
    this.updatedAt = null;   // ISO
    this.loaded = false;
  }

  async ensureLoaded() {
    if (this.loaded) return;
    const [latest, updatedAt] = await Promise.all([
      this.state.storage.get("latest"),
      this.state.storage.get("updated_at"),
    ]);
    this.latest = latest || null;
    this.updatedAt = updatedAt || null;
    this.loaded = true;
  }

  async fetch(request) {
    await this.ensureLoaded();
    const url = new URL(request.url);

    if (url.pathname === "/publish") {
      let text;
      try {
        text = await request.text();
        JSON.parse(text);   // 壊れたJSONは保存しない
      } catch (e) {
        return json({ error: "invalid json" }, 400);
      }
      if (text.length > 2_000_000) return json({ error: "too large" }, 413);
      this.latest = text;
      this.updatedAt = new Date().toISOString();
      await this.state.storage.put({ latest: text, updated_at: this.updatedAt });
      // 接続中のWebSocketへ即時push（hibernation API）
      let sent = 0;
      for (const ws of this.state.getWebSockets()) {
        try { ws.send(text); sent++; } catch (e) { /* closed */ }
      }
      return json({ ok: true, bytes: text.length, pushed: sent, updated_at: this.updatedAt });
    }

    if (url.pathname === "/live.json") {
      if (!this.latest) return json({ error: "no data yet", updated_at: null }, 404);
      return new Response(this.latest, {
        headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
                   "X-Updated-At": this.updatedAt || "", ...CORS },
      });
    }

    if (url.pathname === "/status") {
      return json({ ok: true, has_data: !!this.latest, bytes: this.latest ? this.latest.length : 0,
                    updated_at: this.updatedAt, sockets: this.state.getWebSockets().length });
    }

    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") return json({ error: "expected websocket" }, 426);
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.state.acceptWebSocket(server);
      if (this.latest) { try { server.send(this.latest); } catch (e) { /* ignore */ } }
      return new Response(null, { status: 101, webSocket: client });
    }
    return json({ error: "not found" }, 404);
  }

  async webSocketMessage(ws, message) {
    // クライアントからは "ping" だけ受ける（生存確認）
    if (message === "ping") { try { ws.send("pong"); } catch (e) { /* ignore */ } }
  }
  async webSocketClose(ws) { try { ws.close(); } catch (e) { /* ignore */ } }
  async webSocketError(ws) { try { ws.close(); } catch (e) { /* ignore */ } }
}
