// Tavla çok oyunculu sunucusu (Cloudflare Workers + Durable Objects)
//
// Oyundaki yerel ağ sunucusuyla (src/net.rs) aynı protokol: her WebSocket mesajı tek bir JSON.
// Sunucu "aptal" bir aktarıcıdır: odadaki oyun olaylarını sırayla kaydeder ve herkese dağıtır;
// oyunun durumu her cihazda olay kaydından hesaplanır. Odalar kalıcıdır (Durable Object deposu).
// Tüm odalar tek bir Durable Object'te ("hub") durur; arkadaşlar arası kullanım için fazlasıyla yeter.

import { DurableObject } from "cloudflare:workers";

const PROTO = 1;
const ROOM_TTL = 14 * 86400; // 14 gün oynanmayan oda silinir
const MAX_MSG = 64 * 1024;
const MAX_LOG = 100000;
const MAX_CHAT = 300;
const EV_KINDS = new Set(["Commit", "Reveal", "Roll", "Moves", "Resign", "Next", "Rematch"]);
const SEAT_KINDS = new Set(["Commit", "Reveal", "Roll", "Moves", "Resign"]);

const nowSecs = () => Math.floor(Date.now() / 1000);

async function shaHex(s) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
const pwHash = async (room, pw) => (await shaHex(`tavla-pw:${room}:${pw}`)).slice(0, 24);
const keyOf = async (pid) => (await shaHex(`tavla-key:${pid}`)).slice(0, 8);

function randId() {
  const b = new Uint8Array(4);
  crypto.getRandomValues(b);
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

function cleanName(s, max = 16) {
  const n = String(s ?? "").replace(/[\u0000-\u001f\u007f]/g, "").slice(0, max).trim();
  return n || "Oyuncu";
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket bekleniyor", { status: 426 });
      }
      const stub = env.HUB.get(env.HUB.idFromName("hub"));
      return stub.fetch(req);
    }
    if (url.pathname === "/" || url.pathname === "/durum") {
      return new Response("Tavla sunucusu çalışıyor.", { headers: { "content-type": "text/plain; charset=utf-8" } });
    }
    return new Response("Bulunamadı", { status: 404 });
  },
};

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    /** @type {Map<string, any>} */
    this.rooms = new Map();
    ctx.blockConcurrencyWhile(async () => {
      const all = await ctx.storage.list({ prefix: "room:" });
      const now = nowSecs();
      for (const [k, r] of all) {
        if (now - (r.active || 0) > ROOM_TTL) await ctx.storage.delete(k);
        else this.rooms.set(r.id, r);
      }
    });
    // Canlı tutma: uyuyan nesneyi uyandırmadan cevaplanır
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"t":"Ping"}', '{"t":"Pong"}'));
  }

  async fetch(_req) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ pid: "", name: "", room: null, seat: null });
    return new Response(null, { status: 101, webSocket: client });
  }

  // --- yardımcılar ---
  att(ws) {
    return ws.deserializeAttachment() || { pid: "", name: "", room: null, seat: null };
  }
  setAtt(ws, a) {
    ws.serializeAttachment(a);
  }
  send(ws, m) {
    try {
      ws.send(JSON.stringify(m));
    } catch {}
  }
  members(room, exclude) {
    return this.ctx.getWebSockets().filter((ws) => ws !== exclude && this.att(ws).room === room);
  }
  broadcast(room, m, exclude) {
    const s = JSON.stringify(m);
    for (const ws of this.members(room, exclude)) {
      try {
        ws.send(s);
      } catch {}
    }
  }
  info(r, exclude) {
    const online = [false, false];
    let spectators = 0;
    for (const ws of this.members(r.id, exclude)) {
      const a = this.att(ws);
      if (a.seat === 0 || a.seat === 1) online[a.seat] = true;
      else spectators++;
    }
    const status = r.seats.some((s) => !s) ? 0 : r.over ? 2 : 1;
    return {
      id: r.id,
      name: r.name,
      host: r.host_name,
      match_to: r.match_to,
      locked: !!r.pw,
      players: [0, 1].map((k) => (r.seats[k] ? r.seats[k].name : "")),
      keys: [0, 1].map((k) => (r.seats[k] ? r.seats[k].key : "")),
      online,
      status,
      score: r.score,
      spectators,
      active: r.active,
    };
  }
  list() {
    const now = nowSecs();
    return [...this.rooms.values()]
      .filter((r) => now - r.active <= ROOM_TTL)
      .sort((a, b) => b.active - a.active)
      .slice(0, 60)
      .map((r) => this.info(r));
  }
  presence(room, exclude) {
    const r = this.rooms.get(room);
    if (r) this.broadcast(room, { t: "Presence", room: this.info(r, exclude) }, exclude);
  }
  async save(r) {
    await this.ctx.storage.put("room:" + r.id, r);
  }
  leave(ws) {
    const a = this.att(ws);
    if (!a.room) return;
    const old = a.room;
    a.room = null;
    a.seat = null;
    this.setAtt(ws, a);
    this.presence(old);
  }
  err(ws, code, msg) {
    this.send(ws, { t: "Error", code, msg });
  }

  // --- olaylar ---
  async webSocketMessage(ws, data) {
    if (typeof data !== "string" || data.length > MAX_MSG) return;
    let m;
    try {
      m = JSON.parse(data);
    } catch {
      return;
    }
    if (!m || typeof m.t !== "string") return;
    const a = this.att(ws);
    switch (m.t) {
      case "Hello": {
        if (m.proto !== PROTO) {
          this.err(ws, "version", "Oyun sürümleri farklı. Lütfen oyunu güncelleyin.");
          return;
        }
        a.pid = String(m.pid || "").slice(0, 32);
        a.name = cleanName(m.name);
        this.setAtt(ws, a);
        return;
      }
      case "Ping":
        this.send(ws, { t: "Pong" });
        return;
      case "List":
        this.send(ws, { t: "Rooms", rooms: this.list() });
        return;
      case "Create": {
        if (!a.pid) return;
        if (this.rooms.size > 500) {
          this.err(ws, "full", "Sunucuda çok fazla oda var, biraz sonra tekrar dene.");
          return;
        }
        const id = randId();
        const now = nowSecs();
        const pw = String(m.pw || "").trim();
        const name = String(m.name || "").replace(/[\u0000-\u001f\u007f]/g, "").slice(0, 28).trim() || `${a.name} ile tavla`;
        const r = {
          id,
          name,
          host_pid: a.pid,
          host_name: a.name,
          match_to: Math.min(25, Math.max(1, Number(m.match_to) | 0 || 5)),
          pw: pw ? await pwHash(id, pw) : null,
          seats: [{ pid: a.pid, name: a.name, key: await keyOf(a.pid) }, null],
          log: [],
          chat: [],
          score: [0, 0],
          over: false,
          created: now,
          active: now,
        };
        this.rooms.set(id, r);
        await this.save(r);
        this.send(ws, { t: "Created", room: id });
        return;
      }
      case "Join": {
        if (!a.pid) return;
        const r = this.rooms.get(String(m.room || ""));
        if (!r) {
          this.err(ws, "notfound", "Oda bulunamadı. Kapatılmış olabilir.");
          return;
        }
        const mine = r.seats.findIndex((s) => s && s.pid === a.pid);
        if (mine < 0 && r.pw) {
          const pw = String(m.pw || "").trim();
          if ((await pwHash(r.id, pw)) !== r.pw) {
            this.err(ws, "password", pw ? "Şifre yanlış." : "Bu oda şifreli.");
            return;
          }
        }
        this.leave(ws);
        let seat = null;
        if (mine >= 0) seat = mine;
        else if (!m.spectate) {
          const free = r.seats.findIndex((s) => !s);
          if (free >= 0) seat = free;
        }
        if (seat !== null) {
          r.seats[seat] = { pid: a.pid, name: a.name, key: await keyOf(a.pid) };
          r.active = nowSecs();
        }
        const b = this.att(ws);
        b.room = r.id;
        b.seat = seat;
        this.setAtt(ws, b);
        this.send(ws, { t: "Joined", room: this.info(r), seat, log: r.log, chat: r.chat });
        this.presence(r.id);
        await this.save(r);
        return;
      }
      case "Leave":
        this.leave(ws);
        return;
      case "Delete": {
        const r = this.rooms.get(String(m.room || ""));
        if (!r || r.host_pid !== a.pid) return;
        this.broadcast(r.id, { t: "Error", code: "closed", msg: "Oda kurucusu odayı kapattı." });
        for (const w of this.members(r.id)) {
          const x = this.att(w);
          x.room = null;
          x.seat = null;
          this.setAtt(w, x);
        }
        this.rooms.delete(r.id);
        await this.ctx.storage.delete("room:" + r.id);
        return;
      }
      case "Act": {
        if (!a.room || (a.seat !== 0 && a.seat !== 1)) return;
        const ev = m.ev;
        if (!ev || !EV_KINDS.has(ev.k)) return;
        if (SEAT_KINDS.has(ev.k) && ev.seat !== a.seat) return;
        const r = this.rooms.get(a.room);
        if (!r || r.log.length >= MAX_LOG) return;
        // Ortak zar adımları her koltuk için bir kez
        if ((ev.k === "Commit" || ev.k === "Reveal") && r.log.some((e) => e.k === ev.k && e.seat === ev.seat)) return;
        r.log.push(ev);
        r.active = nowSecs();
        this.broadcast(r.id, { t: "Ev", seq: r.log.length - 1, ev });
        await this.save(r);
        return;
      }
      case "Chat": {
        if (!a.room) return;
        const text = String(m.text || "").replace(/[\u0000-\u001f\u007f]/g, "").slice(0, 300).trim();
        if (!text) return;
        const r = this.rooms.get(a.room);
        if (!r) return;
        const line = { name: a.name, text, ts: nowSecs(), seat: a.seat };
        r.chat.push(line);
        if (r.chat.length > MAX_CHAT) r.chat.splice(0, r.chat.length - MAX_CHAT);
        r.active = nowSecs();
        this.broadcast(r.id, { t: "Chat", line });
        await this.save(r);
        return;
      }
      case "Status": {
        if (!a.room || (a.seat !== 0 && a.seat !== 1)) return;
        const r = this.rooms.get(a.room);
        if (!r || !Array.isArray(m.score)) return;
        const score = [Number(m.score[0]) | 0, Number(m.score[1]) | 0];
        const over = !!m.over;
        if (score[0] !== r.score[0] || score[1] !== r.score[1] || over !== r.over) {
          r.score = score;
          r.over = over;
          await this.save(r);
        }
        return;
      }
    }
  }

  async webSocketClose(ws, code, _reason, _clean) {
    const a = this.att(ws);
    if (a.room) {
      const room = a.room;
      a.room = null;
      a.seat = null;
      this.setAtt(ws, a);
      this.presence(room, ws);
    }
    try {
      ws.close(code === 1005 ? 1000 : code, "kapandı");
    } catch {}
  }

  async webSocketError(ws, _e) {
    await this.webSocketClose(ws, 1011, "", false);
  }
}
