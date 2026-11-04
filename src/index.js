// Tavla çok oyunculu sunucusu (Cloudflare Workers + Durable Objects)
//
// Oyundaki yerel ağ sunucusuyla (src/net.rs) aynı protokol: her WebSocket mesajı tek bir JSON.
// Sunucu "aptal" bir aktarıcıdır: odadaki oyun olaylarını sırayla kaydeder ve herkese dağıtır;
// oyunun durumu her cihazda olay kaydından hesaplanır. Odalar kalıcıdır (Durable Object deposu).
// Tüm odalar tek bir Durable Object'te ("hub") durur; arkadaşlar arası kullanım için fazlasıyla yeter.

import { DurableObject } from "cloudflare:workers";

const PROTO = 1;
const ROOM_TTL = 14 * 86400; // 14 gün oynanmayan oda silinir
const TOUR_TTL = 60 * 86400; // 60 gün hareketsiz turnuva silinir
const MAX_TOUR_PLAYERS = 256;
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
    /** @type {Map<string, any>} */
    this.tours = new Map();
    ctx.blockConcurrencyWhile(async () => {
      const now = nowSecs();
      const tours = await ctx.storage.list({ prefix: "tour:" });
      for (const [k, t] of tours) {
        if (now - (t.active || 0) > TOUR_TTL) await ctx.storage.delete(k);
        else this.tours.set(t.id, t);
      }
      const all = await ctx.storage.list({ prefix: "room:" });
      for (const [k, r] of all) {
        // Turnuva odaları turnuvayla birlikte yaşar
        const alive = r.tour ? this.tours.has(r.tour) : now - (r.active || 0) <= ROOM_TTL;
        if (!alive) await ctx.storage.delete(k);
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
    server.serializeAttachment({ pid: "", name: "", room: null, seat: null, tour: null });
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
      tour: r.tour || "",
    };
  }
  list() {
    const now = nowSecs();
    return [...this.rooms.values()]
      .filter((r) => !r.tour && now - r.active <= ROOM_TTL)
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

  // --- turnuva / lig ---
  async saveTour(t) {
    await this.ctx.storage.put("tour:" + t.id, t);
  }
  tourList() {
    const now = nowSecs();
    return [...this.tours.values()]
      .filter((t) => now - t.active <= TOUR_TTL)
      .sort((x, y) => y.active - x.active)
      .slice(0, 60)
      .map((t) => this.tourSummary(t));
  }
  tourSummary(t) {
    return {
      id: t.id,
      name: t.name,
      kind: t.kind,
      match_to: t.match_to,
      locked: !!t.pw,
      host: t.host_name,
      host_key: t.host_key,
      count: t.players.length,
      max: t.max,
      started: t.started,
      finished: t.finished,
      champion: t.champion === null || t.champion === undefined ? "" : t.players[t.champion]?.name || "",
      keys: t.players.map((p) => p.key),
    };
  }
  tourFull(t) {
    const s = this.tourSummary(t);
    s.players = t.players.map((p) => p.name);
    s.matches = t.matches.map((mt) => {
      const r = mt.room ? this.rooms.get(mt.room) : null;
      let online = [false, false];
      if (r) online = this.info(r).online;
      return {
        room: mt.room || "",
        a: mt.a,
        b: mt.b,
        round: mt.round,
        winner: mt.winner,
        score: r ? r.score : [0, 0],
        walkover: !!mt.walkover,
        bye: !!mt.bye,
        online,
      };
    });
    return s;
  }
  tourBroadcast(t) {
    const s = JSON.stringify({ t: "Tour", tour: this.tourFull(t) });
    for (const ws of this.ctx.getWebSockets()) {
      if (this.att(ws).tour === t.id) {
        try {
          ws.send(s);
        } catch {}
      }
    }
  }
  async tourRoom(t, mt) {
    const pa = t.players[mt.a];
    const pb = t.players[mt.b];
    const id = randId();
    const now = nowSecs();
    const r = {
      id,
      name: `${t.name}: ${pa.name} – ${pb.name}`.slice(0, 60),
      host_pid: t.host_pid,
      host_name: t.host_name,
      match_to: t.match_to,
      pw: null,
      seats: [
        { pid: pa.pid, name: pa.name, key: pa.key },
        { pid: pb.pid, name: pb.name, key: pb.key },
      ],
      log: [],
      chat: [],
      score: [0, 0],
      over: false,
      created: now,
      active: now,
      tour: t.id,
    };
    this.rooms.set(id, r);
    await this.save(r);
    mt.room = id;
  }
  async tourStart(t) {
    if (t.started) return;
    t.started = true;
    t.active = nowSecs();
    const n = t.players.length;
    const order = [...Array(n).keys()];
    for (let i = n - 1; i > 0; i--) {
      const b = new Uint32Array(1);
      crypto.getRandomValues(b);
      const j = b[0] % (i + 1);
      [order[i], order[j]] = [order[j], order[i]];
    }
    t.matches = [];
    if (t.kind === "lig") {
      // Herkes herkesle bir kez
      for (let i = 0; i < n; i++)
        for (let j = i + 1; j < n; j++) t.matches.push({ room: "", a: order[i], b: order[j], round: 0, winner: null, walkover: false, bye: false });
      for (const mt of t.matches) await this.tourRoom(t, mt);
    } else {
      // Eleme: 2'nin kuvvetine tamamla, eksikler ilk turu "bay" geçer
      let size = 1;
      while (size < n) size *= 2;
      const rounds = Math.log2(size);
      let byes = size - n;
      let k = 0;
      for (let i = 0; i < size / 2; i++) {
        if (byes > 0) {
          t.matches.push({ room: "", a: order[k++], b: null, round: 0, winner: null, walkover: false, bye: true });
          byes--;
        } else {
          t.matches.push({ room: "", a: order[k++], b: order[k++], round: 0, winner: null, walkover: false, bye: false });
        }
      }
      for (let r = 1; r < rounds; r++) {
        for (let i = 0; i < size >> (r + 1); i++) t.matches.push({ room: "", a: null, b: null, round: r, winner: null, walkover: false, bye: false });
      }
      await this.tourAdvance(t);
    }
  }
  /** Eleme ağacını ilerletir, oynanacak maçlara oda açar, bitişi işaretler */
  async tourAdvance(t) {
    if (t.kind === "lig") {
      if (t.matches.length && t.matches.every((mt) => mt.winner !== null)) t.finished = true;
      return;
    }
    const byRound = (r) => t.matches.filter((mt) => mt.round === r);
    let changed = true;
    while (changed) {
      changed = false;
      const maxRound = Math.max(...t.matches.map((mt) => mt.round));
      for (let r = 0; r <= maxRound; r++) {
        const list = byRound(r);
        list.forEach((mt, slot) => {
          if (r > 0) {
            const prev = byRound(r - 1);
            const pa = prev[slot * 2];
            const pb = prev[slot * 2 + 1];
            const wa = pa && pa.winner !== null ? (pa.winner === 0 ? pa.a : pa.b) : null;
            const wb = pb && pb.winner !== null ? (pb.winner === 0 ? pb.a : pb.b) : null;
            if (mt.a !== wa || mt.b !== wb) {
              if (mt.a === null && wa !== null) (mt.a = wa), (changed = true);
              if (mt.b === null && wb !== null) (mt.b = wb), (changed = true);
            }
          }
          if (mt.bye && mt.winner === null && mt.a !== null) {
            mt.winner = 0;
            changed = true;
          }
        });
      }
    }
    for (const mt of t.matches) {
      if (mt.a !== null && mt.b !== null && !mt.room && mt.winner === null) await this.tourRoom(t, mt);
    }
    const fin = t.matches.find((mt) => mt.round === Math.max(...t.matches.map((x) => x.round)));
    if (fin && fin.winner !== null) {
      t.finished = true;
      t.champion = fin.winner === 0 ? fin.a : fin.b;
    }
  }
  /** Turnuva odasında maç bittiğinde sonucu işler */
  async tourResult(r) {
    const t = this.tours.get(r.tour);
    if (!t) return;
    const mt = t.matches.find((x) => x.room === r.id);
    if (!mt) return;
    t.active = nowSecs();
    if (mt.winner === null && r.over) {
      mt.winner = r.score[0] >= r.match_to ? 0 : r.score[1] >= r.match_to ? 1 : null;
      if (mt.winner !== null) await this.tourAdvance(t);
    }
    await this.saveTour(t);
    this.tourBroadcast(t);
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
        if (r.tour && ev.k === "Rematch") return;
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
          if (r.tour) await this.tourResult(r);
        }
        return;
      }
      case "TourList":
        this.send(ws, { t: "Tours", tours: this.tourList() });
        return;
      case "TourOpen": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t) {
          this.err(ws, "tournotfound", "Turnuva bulunamadı. Silinmiş olabilir.");
          return;
        }
        a.tour = t.id;
        this.setAtt(ws, a);
        this.send(ws, { t: "Tour", tour: this.tourFull(t) });
        return;
      }
      case "TourClose":
        a.tour = null;
        this.setAtt(ws, a);
        return;
      case "TourCreate": {
        if (!a.pid) return;
        if (this.tours.size > 200) {
          this.err(ws, "full", "Sunucuda çok fazla turnuva var.");
          return;
        }
        const id = randId();
        const now = nowSecs();
        const pw = String(m.pw || "").trim();
        const kind = m.kind === "eleme" ? "eleme" : "lig";
        const name = String(m.name || "").replace(/[\u0000-\u001f\u007f]/g, "").slice(0, 28).trim() || `${a.name} turnuvası`;
        const max = Math.min(MAX_TOUR_PLAYERS, Math.max(0, Number(m.max) | 0));
        const t = {
          id,
          name,
          kind,
          match_to: Math.min(25, Math.max(1, Number(m.match_to) | 0 || 3)),
          pw: pw ? await pwHash(id, pw) : null,
          max: max === 1 ? 2 : max,
          host_pid: a.pid,
          host_name: a.name,
          host_key: await keyOf(a.pid),
          players: [{ pid: a.pid, name: a.name, key: await keyOf(a.pid) }],
          started: false,
          finished: false,
          champion: null,
          matches: [],
          created: now,
          active: now,
        };
        this.tours.set(id, t);
        a.tour = id;
        this.setAtt(ws, a);
        await this.saveTour(t);
        this.send(ws, { t: "TourCreated", tour: id });
        this.send(ws, { t: "Tour", tour: this.tourFull(t) });
        return;
      }
      case "TourJoin": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t || !a.pid) return;
        if (t.players.some((p) => p.pid === a.pid)) return;
        if (t.started) {
          this.err(ws, "tour", "Turnuva başladı, artık katılınamaz.");
          return;
        }
        if (t.max && t.players.length >= t.max) {
          this.err(ws, "tour", "Turnuvanın kontenjanı dolu.");
          return;
        }
        if (t.pw && (await pwHash(t.id, String(m.pw || "").trim())) !== t.pw) {
          this.err(ws, "tourpassword", String(m.pw || "") ? "Şifre yanlış." : "Bu turnuva şifreli.");
          return;
        }
        t.players.push({ pid: a.pid, name: a.name, key: await keyOf(a.pid) });
        t.active = nowSecs();
        a.tour = t.id;
        this.setAtt(ws, a);
        if (t.max && t.players.length >= t.max) await this.tourStart(t);
        await this.saveTour(t);
        this.tourBroadcast(t);
        return;
      }
      case "TourLeave": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t || t.started || t.host_pid === a.pid) return;
        t.players = t.players.filter((p) => p.pid !== a.pid);
        await this.saveTour(t);
        this.tourBroadcast(t);
        return;
      }
      case "TourStart": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t || t.started || t.host_pid !== a.pid) return;
        if (t.players.length < 2) {
          this.err(ws, "tour", "Başlatmak için en az 2 oyuncu gerekli.");
          return;
        }
        await this.tourStart(t);
        await this.saveTour(t);
        this.tourBroadcast(t);
        return;
      }
      case "TourWalkover": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t || t.host_pid !== a.pid) return;
        const mt = t.matches[Number(m.m) | 0];
        if (!mt || mt.winner !== null || mt.a === null || mt.b === null) return;
        mt.winner = Number(m.side) === 1 ? 1 : 0;
        mt.walkover = true;
        const r = this.rooms.get(mt.room);
        if (r) {
          r.over = true;
          await this.save(r);
          this.broadcast(r.id, { t: "Error", code: "walkover", msg: `Turnuva kurucusu bu maçı ${t.players[mt.winner === 0 ? mt.a : mt.b].name} lehine hükmen sonuçlandırdı.` });
        }
        await this.tourAdvance(t);
        await this.saveTour(t);
        this.tourBroadcast(t);
        return;
      }
      case "TourDelete": {
        const t = this.tours.get(String(m.tour || ""));
        if (!t || t.host_pid !== a.pid) return;
        for (const mt of t.matches) {
          if (mt.room && this.rooms.has(mt.room)) {
            this.broadcast(mt.room, { t: "Error", code: "closed", msg: "Turnuva silindi." });
            this.rooms.delete(mt.room);
            await this.ctx.storage.delete("room:" + mt.room);
          }
        }
        for (const w of this.ctx.getWebSockets()) {
          const x = this.att(w);
          if (x.tour === t.id) {
            this.send(w, { t: "Error", code: "tourclosed", msg: "Turnuva silindi." });
            x.tour = null;
            this.setAtt(w, x);
          }
        }
        this.tours.delete(t.id);
        await this.ctx.storage.delete("tour:" + t.id);
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
