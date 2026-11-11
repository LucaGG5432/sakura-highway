#!/usr/bin/env node
'use strict';
/*
  Sakura Highway: Server für das Spiel im Netz.
  Bestenliste, Mehrspieler (WebSocket) und Admin-Bereich. Keine Abhängigkeiten, Node 18 oder neuer.

  Starten:      node server.js          (dann http://localhost:3000 öffnen)
  Einstellungen (Umgebungsvariablen, alle freiwillig):
    PORT          Port, Standard 3000
    DATA_FILE     Pfad der Punktedatei, Standard ./scores.json
    ADMIN_TOKEN   Admin-Passwort. Fehlt es, denkt sich der Server eines aus und schreibt es
                  in die Datei admin-token.txt (neben der Punktedatei) und ins Protokoll.
    ROUND_SECONDS Länge einer Mehrspieler-Runde, Standard 90
    UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
                  Dauerhafter Speicher für die Bestenliste (kostenlose Datenbank bei upstash.com).
                  Nötig bei Anbietern, die ihre Dateien bei jedem Neustart löschen (zum Beispiel Render Free).
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const VERSION = '2.0';
const PORT = +process.env.PORT || 3000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'scores.json');
const MAX_ENTRIES = 5000;
const ROUND_S = Math.min(600, Math.max(5, +process.env.ROUND_SECONDS || 90));
const COUNT_S = 3;
const MAX_PLAYERS = 8, MAX_ROOMS = 100, MAX_CLIENTS = 400, MAX_PER_IP = 12;
const KV_URL = String(process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || '').trim().replace(/\/$/, '');
const KV_TOKEN = String(process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || '').trim();
const KV_KEY = 'sakura-highway:scores';
const KV_RETRY_MS = +process.env.KV_RETRY_MS || 30000;
const MAX_SPEED_MS = 220; // schneller als 220 m/s (790 km/h) kann niemand gefahren sein
const STARTED = Date.now();

/* ---------- Admin-Passwort ---------- */
try { fs.mkdirSync(path.dirname(DATA), { recursive: true }); } catch (e) {}
const TOKEN_FILE = path.join(path.dirname(DATA), 'admin-token.txt');
function makeToken() {
  const abc = 'abcdefghjkmnpqrstuvwxyz23456789'; let s = '';
  for (let i = 0; i < 12; i++) { s += abc[crypto.randomInt(abc.length)]; if (i % 4 === 3 && i < 11) s += '-'; }
  return s;
}
let ADMIN = String(process.env.ADMIN_TOKEN || '').trim();
const adminFromEnv = !!ADMIN;
let tokenNote = '';
if (!ADMIN) {
  try { ADMIN = fs.readFileSync(TOKEN_FILE, 'utf8').trim(); } catch (e) {}
  if (ADMIN.length < 8) {
    ADMIN = makeToken();
    try { fs.writeFileSync(TOKEN_FILE, ADMIN + '\n', { mode: 0o600 }); tokenNote = ' (gespeichert in ' + TOKEN_FILE + ')'; }
    catch (e) { tokenNote = ' (Datei konnte nicht geschrieben werden, das Passwort gilt nur bis zum Neustart)'; }
  } else tokenNote = ' (steht in ' + TOKEN_FILE + ')';
}

/* ---------- Bestenliste ---------- */
let scores = {};
try { scores = JSON.parse(fs.readFileSync(DATA, 'utf8')) || {}; } catch (e) { scores = {}; }

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = DATA + '.tmp';
    fs.writeFile(tmp, JSON.stringify(scores), (err) => { if (!err) fs.rename(tmp, DATA, () => {}); });
  }, 300);
  kvDirty();
}
function saveNow() {
  clearTimeout(saveTimer);
  try { fs.writeFileSync(DATA + '.tmp', JSON.stringify(scores)); fs.renameSync(DATA + '.tmp', DATA); } catch (e) {}
}

/* ---------- Dauerhafter Speicher (Upstash Redis über HTTPS), optional ---------- */
let kvReady = false, kvIsDirty = false, kvTimer = null, kvBusy = false, kvErr = '';
async function kv(cmd) {
  const r = await fetch(KV_URL, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + KV_TOKEN, 'content-type': 'application/json' },
    body: JSON.stringify(cmd),
    signal: AbortSignal.timeout(8000)
  });
  if (!r.ok) throw new Error('Upstash antwortet mit ' + r.status);
  const j = await r.json();
  if (j && j.error) throw new Error(String(j.error).slice(0, 120));
  return j.result;
}
function kvDirty() {
  if (!KV_URL) return;
  kvIsDirty = true;
  if (kvTimer || !kvReady) return;
  kvTimer = setTimeout(kvFlush, 3000);
}
async function kvFlush() {
  kvTimer = null;
  if (!KV_URL || !kvReady || !kvIsDirty || kvBusy) return;
  kvBusy = true; kvIsDirty = false;
  try { await kv(['SET', KV_KEY, JSON.stringify(scores)]); kvErr = ''; }
  catch (e) {
    kvIsDirty = true; kvErr = e.message;
    console.error('Speicher (Upstash) nicht erreichbar:', e.message);
    kvTimer = setTimeout(kvFlush, 15000);
  }
  kvBusy = false;
  if (kvIsDirty && !kvTimer) kvTimer = setTimeout(kvFlush, 3000);
}
function mergeScores(remote) {
  for (const id of Object.keys(remote || {})) {
    const r = remote[id];
    if (!r || typeof r !== 'object') continue;
    const name = cleanName(r.name), score = Math.floor(Number(r.score)), kmh = Math.floor(Number(r.kmh));
    if (!name || !(score >= 0 && score <= 5e6) || !(kmh >= 0 && kmh <= 700)) continue;
    const p = scores[id];
    if (!p) { if (Object.keys(scores).length < MAX_ENTRIES) scores[id] = { name, score, kmh, at: Number(r.at) || 0 }; }
    else { p.score = Math.max(p.score, score); p.kmh = Math.max(p.kmh, kmh); }
  }
}
// Beim Start die Bestenliste aus dem Speicher holen. Geschrieben wird erst, wenn das geklappt hat,
// damit eine Störung nie die alten Daten überschreibt.
async function kvInit() {
  if (!KV_URL) return;
  try {
    const v = await kv(['GET', KV_KEY]);
    let remoteCount = 0;
    if (v) {
      try { const o = JSON.parse(v); remoteCount = Object.keys(o).length; mergeScores(o); }
      catch (e) { console.error('Gespeicherte Bestenliste ist beschädigt und wird ersetzt.'); }
    }
    kvReady = true; kvErr = '';
    if (Object.keys(scores).length !== remoteCount) kvDirty();
    console.log('Speicher: Upstash verbunden, ' + Object.keys(scores).length + ' Einträge geladen');
  } catch (e) {
    kvErr = e.message;
    console.error('Speicher (Upstash) beim Start nicht erreichbar (' + e.message + '). Neuer Versuch in ' + Math.round(KV_RETRY_MS / 1000) + ' s.');
    setTimeout(kvInit, KV_RETRY_MS).unref();
  }
}

// Die Spieler-ID aus dem Browser bleibt geheim. Gespeichert und angezeigt wird nur ein Hash davon.
const pubId = (pid) => crypto.createHash('sha256').update('sakura:' + pid).digest('hex').slice(0, 16);
const validPid = (p) => typeof p === 'string' && /^[a-f0-9]{32}$/.test(p);
const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f\u007f]/g, '').replace(/\s+/g, ' ').trim().slice(0, 14);

/* ---------- Begrenzungen ---------- */
const hits = new Map();
const fails = new Map();
let gFails = [];
setInterval(() => {
  hits.clear();
  const now = Date.now();
  for (const [ip, w] of fails) { const k = w.filter((t) => now - t < 600000); if (k.length) fails.set(ip, k); else fails.delete(ip); }
  gFails = gFails.filter((t) => now - t < 600000);
}, 10 * 60 * 1000).unref();

function limited(ip) {
  const now = Date.now();
  const w = (hits.get(ip) || []).filter((t) => now - t < 60000);
  w.push(now);
  hits.set(ip, w);
  return w.length > 240;
}
// Falsche Admin-Passwörter: pro Adresse höchstens 8 in 10 Minuten, insgesamt höchstens 200.
function adminLocked(ip) {
  const now = Date.now();
  const w = (fails.get(ip) || []).filter((t) => now - t < 600000);
  fails.set(ip, w);
  gFails = gFails.filter((t) => now - t < 600000);
  return w.length >= 8 || gFails.length >= 200;
}
function adminFail(ip) {
  const now = Date.now();
  const w = fails.get(ip) || []; w.push(now); fails.set(ip, w); gFails.push(now);
}
function sameToken(a, b) {
  const h = (x) => crypto.createHash('sha256').update(String(x || '')).digest();
  return crypto.timingSafeEqual(h(a), h(b));
}
function clientIp(req) {
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return (xf || req.socket.remoteAddress || '').slice(0, 64);
}

/* ---------- HTTP ---------- */
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'content-type, x-player, x-admin-token',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS'
};
function send(res, code, body, type) {
  const headers = Object.assign({ 'Content-Type': type || 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }, CORS);
  res.writeHead(code, headers);
  res.end(body === undefined ? '' : (typeof body === 'string' ? body : JSON.stringify(body)));
}
function readBody(req, cb, max) {
  let size = 0; const chunks = []; const lim = max || 2048;
  req.on('data', (c) => { size += c.length; if (size > lim) { req.destroy(); } else chunks.push(c); });
  req.on('end', () => { try { cb(null, JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); } catch (e) { cb(e); } });
  req.on('error', () => cb(new Error('read')));
}

let page = null;
function getPage(cb) {
  const f = path.join(__dirname, 'index.html');
  fs.stat(f, (err, st) => {
    if (err) return cb(err);
    if (page && page.mtime === st.mtimeMs) return cb(null, page);
    fs.readFile(f, (e2, buf) => {
      if (e2) return cb(e2);
      page = { mtime: st.mtimeMs, tag: 'W/"' + buf.length + '-' + Math.floor(st.mtimeMs) + '"', raw: buf, gz: zlib.gzipSync(buf, { level: 9 }) };
      cb(null, page);
    });
  });
}
function servePage(req, res) {
  getPage((err, p) => {
    if (err) return send(res, 404, 'index.html fehlt', 'text/plain; charset=utf-8');
    const h = { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'ETag': p.tag, 'Vary': 'Accept-Encoding', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' };
    if (req.headers['if-none-match'] === p.tag) { res.writeHead(304, h); return res.end(); }
    const gz = /\bgzip\b/.test(String(req.headers['accept-encoding'] || ''));
    if (gz) h['Content-Encoding'] = 'gzip';
    res.writeHead(200, h);
    res.end(req.method === 'HEAD' ? undefined : gz ? p.gz : p.raw);
  });
}

function topList(mine, n) {
  return Object.keys(scores)
    .map((id) => ({ id, name: scores[id].name, score: scores[id].score, kmh: scores[id].kmh, mine: id === mine }))
    .sort((a, b) => b.score - a.score || b.kmh - a.kmh)
    .slice(0, n);
}

function adminRoutes(req, res, url, ip) {
  const p = url.pathname;
  if (adminLocked(ip)) return send(res, 429, { error: 'locked' });
  if (!sameToken(req.headers['x-admin-token'], ADMIN)) { adminFail(ip); return send(res, 403, { error: 'forbidden' }); }

  if (req.method === 'POST' && p === '/api/admin/login') return send(res, 200, { ok: true });

  if (req.method === 'GET' && p === '/api/admin/stats') {
    const list = topList(null, 1);
    return send(res, 200, {
      version: VERSION, uptime: Math.round((Date.now() - STARTED) / 1000), entries: Object.keys(scores).length,
      best: list[0] ? { name: list[0].name, score: list[0].score, kmh: list[0].kmh } : null,
      online: clients.size, rooms: listRooms(), races: stats.races, roundSeconds: ROUND_S,
      tokenFromEnv: adminFromEnv, maxEntries: MAX_ENTRIES,
      storage: KV_URL ? (kvReady ? 'Upstash' : 'Upstash (offline)') : 'Datei', warnTemp: !KV_URL && !!process.env.RENDER
    });
  }
  if (req.method === 'GET' && p === '/api/admin/scores') {
    const list = Object.keys(scores).map((id) => ({ id, name: scores[id].name, score: scores[id].score, kmh: scores[id].kmh, at: scores[id].at || 0 }))
      .sort((a, b) => b.score - a.score || b.kmh - a.kmh).slice(0, 1000);
    return send(res, 200, list);
  }
  if (req.method === 'GET' && p === '/api/admin/export') {
    const all = Object.keys(scores).map((id) => ({ id, name: scores[id].name, score: scores[id].score, kmh: scores[id].kmh, at: scores[id].at || 0 }));
    res.setHeader('Content-Disposition', 'attachment; filename="sakura-bestenliste.json"');
    return send(res, 200, { game: 'sakura-highway', version: 1, exported: Date.now(), scores: all });
  }
  if (req.method === 'POST' && p === '/api/admin/import') {
    return readBody(req, (err, b) => {
      if (err || !b || !Array.isArray(b.scores)) return send(res, 400, { error: 'bad_body' });
      let added = 0, updated = 0, skipped = 0, count = Object.keys(scores).length;
      for (const e of b.scores.slice(0, MAX_ENTRIES)) {
        if (!e || typeof e !== 'object') { skipped++; continue; }
        const name = cleanName(e.name), score = Math.floor(Number(e.score)), kmh = Math.floor(Number(e.kmh));
        if (!name || !(score >= 0 && score <= 5e6) || !(kmh >= 0 && kmh <= 700)) { skipped++; continue; }
        const id = /^[a-f0-9]{16}$/.test(String(e.id)) ? String(e.id) : crypto.createHash('sha256').update('import:' + name.toLowerCase()).digest('hex').slice(0, 16);
        const prev = scores[id], at = Number(e.at) > 0 ? Number(e.at) : Date.now();
        if (!prev && count >= MAX_ENTRIES) { skipped++; continue; }
        scores[id] = { name: prev ? prev.name : name, score: Math.max(score, prev ? prev.score : 0), kmh: Math.max(kmh, prev ? prev.kmh : 0), at: Math.max(at, prev ? prev.at || 0 : 0) };
        if (prev) updated++; else { added++; count++; }
      }
      save();
      send(res, 200, { added, updated, skipped });
    }, 700000);
  }
  if (req.method === 'PATCH' && p.startsWith('/api/admin/scores/')) {
    const id = decodeURIComponent(p.slice('/api/admin/scores/'.length));
    return readBody(req, (err, b) => {
      if (err || !b || typeof b !== 'object') return send(res, 400, { error: 'bad_body' });
      const name = cleanName(b.name);
      if (!name) return send(res, 400, { error: 'bad_name' });
      if (!scores[id]) return send(res, 404, { error: 'not_found' });
      scores[id].name = name; save();
      send(res, 200, { id, name });
    });
  }
  if (req.method === 'POST' && (p === '/api/admin/clear-scores' || p === '/api/admin/close-room' || p === '/api/admin/announce')) {
    return readBody(req, (err, b) => {
      if (err || !b || typeof b !== 'object') return send(res, 400, { error: 'bad_body' });
      if (p === '/api/admin/clear-scores') {
        if (b.confirm !== 'ALLE') return send(res, 400, { error: 'confirm' });
        const n = Object.keys(scores).length; scores = {}; save();
        return send(res, 200, { removed: n });
      }
      if (p === '/api/admin/close-room') {
        const ok = closeRoom(String(b.code || '').toUpperCase());
        return send(res, ok ? 200 : 404, { ok });
      }
      const text = String(b.text || '').replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, 120);
      if (!text) return send(res, 400, { error: 'bad_text' });
      const n = announce(text);
      return send(res, 200, { sent: n });
    });
  }
  if (req.method === 'DELETE' && p.startsWith('/api/admin/scores/')) {
    const id = decodeURIComponent(p.slice('/api/admin/scores/'.length));
    if (scores[id]) { delete scores[id]; save(); }
    return send(res, 204);
  }
  send(res, 404, { error: 'not_found' });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const ip = clientIp(req);
  const pid = req.headers['x-player'];

  if (req.method === 'OPTIONS') return send(res, 204);

  if (url.pathname === '/' || url.pathname === '/index.html') {
    if (req.method !== 'GET' && req.method !== 'HEAD') return send(res, 405, { error: 'method_not_allowed' });
    return servePage(req, res);
  }
  if (url.pathname === '/favicon.ico') { res.writeHead(204); return res.end(); }

  if (url.pathname === '/api/info') return send(res, 200, { v: VERSION, online: clients.size, rooms: rooms.size, players: MAX_PLAYERS });

  if (!url.pathname.startsWith('/api/')) return send(res, 404, { error: 'not_found' });
  if (limited(ip)) return send(res, 429, { error: 'rate_limited' });

  if (url.pathname.startsWith('/api/admin/')) return adminRoutes(req, res, url, ip);

  if (!url.pathname.startsWith('/api/scores')) return send(res, 404, { error: 'not_found' });

  const mine = validPid(pid) ? pubId(pid) : null;

  if (req.method === 'GET' && url.pathname === '/api/scores') return send(res, 200, topList(mine, 50));

  if (req.method === 'GET' && url.pathname === '/api/scores/me') {
    const e = mine && scores[mine];
    return send(res, 200, e ? { id: mine, name: e.name, score: e.score, kmh: e.kmh, mine: true } : null);
  }

  if (req.method === 'POST' && url.pathname === '/api/scores') {
    if (!mine) return send(res, 400, { error: 'bad_player' });
    return readBody(req, (err, b) => {
      if (err || !b || typeof b !== 'object') return send(res, 400, { error: 'bad_body' });
      const name = cleanName(b.name);
      const score = Math.floor(Number(b.score)), kmh = Math.floor(Number(b.kmh));
      if (!name || !(score >= 0 && score <= 5e6) || !(kmh >= 0 && kmh <= 700)) return send(res, 400, { error: 'bad_values' });
      const prev = scores[mine];
      if (!prev && Object.keys(scores).length >= MAX_ENTRIES) return send(res, 507, { error: 'full' });
      scores[mine] = { name, score: Math.max(score, prev ? prev.score : 0), kmh: Math.max(kmh, prev ? prev.kmh : 0), at: Date.now() };
      save();
      send(res, 200, { id: mine, name, score: scores[mine].score, kmh: scores[mine].kmh, mine: true });
    });
  }

  if (req.method === 'DELETE' && url.pathname.startsWith('/api/scores/')) {
    if (adminLocked(ip)) return send(res, 429, { error: 'locked' });
    if (!sameToken(req.headers['x-admin-token'], ADMIN)) { adminFail(ip); return send(res, 403, { error: 'forbidden' }); }
    const id = decodeURIComponent(url.pathname.slice('/api/scores/'.length));
    if (scores[id]) { delete scores[id]; save(); }
    return send(res, 204);
  }

  send(res, 405, { error: 'method_not_allowed' });
});
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

/* ---------- Mehrspieler: WebSocket nach RFC 6455, von Hand ---------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 2048;
const clients = new Set();
const perIp = new Map();
const rooms = new Map();
const stats = { races: 0 };
let nextId = 1;

function wsFrame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
const textFrame = (s) => wsFrame(1, Buffer.from(s, 'utf8'));
const closeFrame = (code) => { const b = Buffer.alloc(2); b.writeUInt16BE(code, 0); return wsFrame(8, b); };

class Conn {
  constructor(sock, ip) {
    this.sock = sock; this.ip = ip; this.id = nextId++;
    this.buf = Buffer.alloc(0); this.frag = null; this.fragLen = 0;
    this.name = 'Spieler'; this.car = 'std';
    this.room = null; this.alive = true; this.closed = false; this.gone = false;
    this.winAt = Date.now(); this.n = 0; this.pAt = 0;
    sock.on('data', (d) => { try { this.onData(d); } catch (e) { this.kill(); } });
    sock.on('close', () => this.cleanup());
    sock.on('error', () => {});
    sock.on('end', () => { try { sock.end(); } catch (e) {} });
  }
  raw(buf) {
    if (this.closed || this.sock.destroyed) return;
    if (this.sock.writableLength > 1e6) return this.kill();
    this.sock.write(buf);
  }
  send(obj) { this.raw(textFrame(JSON.stringify(obj))); }
  close(code) {
    if (this.closed) return;
    this.raw(closeFrame(code || 1000));
    this.closed = true;
    try { this.sock.end(); } catch (e) {}
    setTimeout(() => { try { this.sock.destroy(); } catch (e) {} }, 1000).unref();
    this.cleanup();
  }
  kill() { this.closed = true; try { this.sock.destroy(); } catch (e) {} this.cleanup(); }
  cleanup() {
    if (this.gone) return;
    this.gone = true; this.closed = true;
    clients.delete(this);
    const k = (perIp.get(this.ip) || 1) - 1;
    if (k > 0) perIp.set(this.ip, k); else perIp.delete(this.ip);
    leaveRoom(this);
  }
  onData(chunk) {
    if (this.closed) return;
    this.alive = true;
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), rsv = b[0] & 0x70, op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (rsv || !masked) return this.close(1002);
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (b.length < 10) return;
        if (b.readUInt32BE(2) !== 0) return this.close(1009);
        len = b.readUInt32BE(6); off = 10;
      }
      if (len > MAX_MSG) return this.close(1009);
      if (b.length < off + 4 + len) return;
      const payload = Buffer.alloc(len);
      for (let i = 0; i < len; i++) payload[i] = b[off + 4 + i] ^ b[off + (i & 3)];
      this.buf = b.slice(off + 4 + len);

      if (op >= 8) { // Steuerframes
        if (!fin || len > 125) return this.close(1002);
        if (op === 8) return this.close(1000);
        if (op === 9) this.raw(wsFrame(10, payload));
        continue;
      }
      if (op === 2) return this.close(1003);
      if (op === 1) {
        if (this.frag) return this.close(1002);
        if (fin) { this.onText(payload.toString('utf8')); continue; }
        this.frag = [payload]; this.fragLen = len; continue;
      }
      if (op === 0) {
        if (!this.frag) return this.close(1002);
        this.fragLen += len;
        if (this.fragLen > MAX_MSG) return this.close(1009);
        this.frag.push(payload);
        if (fin) { const t = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.onText(t); }
        continue;
      }
      return this.close(1002);
    }
  }
  onText(text) {
    const now = Date.now();
    if (now - this.winAt >= 1000) { this.winAt = now; this.n = 0; }
    if (++this.n > 80) return this.close(1008);
    let m;
    try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
    onMessage(this, m, now);
  }
}

function cleanCar(s) { s = String(s || ''); return /^[a-z0-9_-]{1,16}$/.test(s) ? s : 'std'; }
const clamp = (v, a, b) => Math.min(b, Math.max(a, v));

function newCode() {
  const abc = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  for (let k = 0; k < 50; k++) {
    let s = ''; for (let i = 0; i < 4; i++) s += abc[crypto.randomInt(abc.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}
function snapshot(room) {
  return {
    t: 'room', code: room.code, pub: room.pub, host: room.host, state: room.state, max: MAX_PLAYERS, round: ROUND_S,
    players: [...room.players.values()].map((p) => ({ id: p.c.id, name: p.c.name, car: p.c.car }))
  };
}
function roomSend(room, obj) {
  const f = textFrame(JSON.stringify(obj));
  for (const p of room.players.values()) p.c.raw(f);
}
function listRooms() {
  return [...rooms.values()].map((r) => ({ code: r.code, pub: r.pub, state: r.state, players: [...r.players.values()].map((p) => p.c.name) }));
}
function makeRoom(c, pub) {
  if (rooms.size >= MAX_ROOMS) return null;
  const code = newCode();
  if (!code) return null;
  const room = { code, pub: !!pub, host: c.id, state: 'lobby', players: new Map(), cd: 0, cdAt: 0, t0: 0 };
  rooms.set(code, room);
  return room;
}
function joinRoom(c, room) {
  if (c.room === room) return roomSend(room, snapshot(room));
  leaveRoom(c);
  const p = { c, d: 0, x: 0, v: 0, f: 0, maxv: 0 };
  room.players.set(c.id, p);
  c.room = room;
  roomSend(room, snapshot(room));
}
function leaveRoom(c) {
  const room = c.room;
  if (!room) return;
  c.room = null;
  room.players.delete(c.id);
  if (!room.players.size) { rooms.delete(room.code); return; }
  if (room.host === c.id) room.host = room.players.keys().next().value;
  if (room.state === 'count' && room.players.size < 2) room.state = 'lobby';
  roomSend(room, snapshot(room));
  if (room.state === 'race' && room.players.size < 2) finishRace(room);
}
function closeRoom(code) {
  const room = rooms.get(code);
  if (!room) return false;
  for (const p of room.players.values()) { p.c.room = null; p.c.send({ t: 'closed', reason: 'admin' }); }
  rooms.delete(code);
  return true;
}
function announce(text) {
  const f = textFrame(JSON.stringify({ t: 'msg', text }));
  let n = 0;
  for (const c of clients) { c.raw(f); n++; }
  return n;
}
function startCount(room) {
  room.state = 'count'; room.cd = COUNT_S; room.cdAt = Date.now() + 1000;
  for (const p of room.players.values()) { p.d = 0; p.x = 0; p.v = 0; p.f = 0; p.maxv = 0; }
  roomSend(room, snapshot(room));
  roomSend(room, { t: 'phase', s: 'count', n: room.cd });
}
function startRace(room) {
  room.state = 'race'; room.t0 = Date.now();
  roomSend(room, snapshot(room));
  roomSend(room, { t: 'phase', s: 'race', n: ROUND_S });
}
function finishRace(room) {
  if (room.state !== 'race') return;
  const res = [...room.players.values()]
    .sort((a, b) => b.d - a.d || b.maxv - a.maxv)
    .map((p, i) => ({ id: p.c.id, name: p.c.name, car: p.c.car, d: Math.round(p.d), v: Math.round(p.maxv), rank: i + 1 }));
  room.state = 'lobby';
  stats.races++;
  roomSend(room, { t: 'end', res });
  roomSend(room, snapshot(room));
}

function onMessage(c, m, now) {
  switch (m.t) {
    case 'ping': return c.send({ t: 'pong', n: +m.n || 0 });
    case 'create': case 'quick': case 'join': {
      if (c.room && c.room.state !== 'lobby') return c.send({ t: 'err', code: 'busy' });
      const name = cleanName(m.name);
      if (!name) return c.send({ t: 'err', code: 'bad_name' });
      c.name = name; c.car = cleanCar(m.car);
      let room = null;
      if (m.t === 'create') {
        room = makeRoom(c, !!m.pub);
        if (!room) return c.send({ t: 'err', code: 'full' });
      } else if (m.t === 'join') {
        room = rooms.get(String(m.code || '').toUpperCase().slice(0, 4));
        if (!room) return c.send({ t: 'err', code: 'no_room' });
        if (room.state !== 'lobby') return c.send({ t: 'err', code: 'busy' });
        if (room.players.size >= MAX_PLAYERS && !room.players.has(c.id)) return c.send({ t: 'err', code: 'room_full' });
      } else {
        let best = null;
        for (const r of rooms.values()) {
          if (r.pub && r.state === 'lobby' && r.players.size < MAX_PLAYERS && r !== c.room && (!best || r.players.size > best.players.size)) best = r;
        }
        room = best || makeRoom(c, true);
        if (!room) return c.send({ t: 'err', code: 'full' });
      }
      return joinRoom(c, room);
    }
    case 'leave': {
      const had = !!c.room;
      leaveRoom(c);
      return c.send({ t: 'left', had });
    }
    case 'start': {
      const room = c.room;
      if (!room || room.host !== c.id || room.state !== 'lobby') return;
      if (room.players.size < 2) return c.send({ t: 'err', code: 'alone' });
      return startCount(room);
    }
    case 'p': { // Position während der Runde
      const room = c.room;
      if (!room || room.state !== 'race') return;
      if (now - c.pAt < 40) return; // höchstens 25 Meldungen pro Sekunde
      c.pAt = now;
      const p = room.players.get(c.id);
      const el = Math.max(0, (now - room.t0) / 1000);
      let d = Number(m.d);
      if (!isFinite(d) || d < 0) return;
      d = Math.max(p.d, Math.min(d, MAX_SPEED_MS * el + 30));
      p.d = d;
      p.x = clamp(Number(m.x) || 0, -2, 2);
      p.v = clamp(Number(m.v) || 0, 0, 700);
      p.f = (m.f | 0) & 7;
      if (p.v > p.maxv) p.maxv = p.v;
      return;
    }
  }
}

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://localhost');
  const key = req.headers['sec-websocket-key'];
  const bad = (line) => { try { socket.end('HTTP/1.1 ' + line + '\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); } catch (e) {} };
  if (url.pathname !== '/ws') return bad('404 Not Found');
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || !key || req.headers['sec-websocket-version'] !== '13') return bad('400 Bad Request');
  const ip = clientIp(req);
  if (clients.size >= MAX_CLIENTS || (perIp.get(ip) || 0) >= MAX_PER_IP) return bad('503 Service Unavailable');
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  socket.setTimeout(0);
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  const c = new Conn(socket, ip);
  clients.add(c);
  c.send({ t: 'hello', id: c.id, v: VERSION, online: clients.size });
});

// Zehnmal pro Sekunde: Countdown, Positionen verteilen, Runde beenden.
setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    if (room.state === 'count') {
      if (now >= room.cdAt) {
        room.cd--;
        if (room.cd > 0) { room.cdAt += 1000; roomSend(room, { t: 'phase', s: 'count', n: room.cd }); }
        else startRace(room);
      }
    } else if (room.state === 'race') {
      const left = ROUND_S - (now - room.t0) / 1000;
      if (left <= 0) { finishRace(room); continue; }
      roomSend(room, {
        t: 's', left: Math.round(left * 10) / 10,
        p: [...room.players.values()].map((p) => [p.c.id, Math.round(p.d * 10) / 10, Math.round(p.x * 100) / 100, Math.round(p.v), p.f])
      });
    }
  }
}, 100).unref();

// Alle 20 Sekunden anpingen. Wer nicht antwortet, fliegt raus.
setInterval(() => {
  for (const c of [...clients]) {
    if (!c.alive) { c.kill(); continue; }
    c.alive = false;
    c.raw(wsFrame(9, Buffer.alloc(0)));
  }
}, 20000).unref();

async function shutdown() {
  saveNow();
  for (const c of clients) { try { c.raw(closeFrame(1001)); c.sock.destroy(); } catch (e) {} }
  if (KV_URL && kvReady && kvIsDirty) { try { await Promise.race([kv(['SET', KV_KEY, JSON.stringify(scores)]), new Promise((r) => setTimeout(r, 4000))]); } catch (e) {} }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('uncaughtException', (e) => { console.error('Fehler:', e && e.stack || e); });

kvInit().then(() => server.listen(PORT, () => {
  const url = 'http://localhost:' + PORT;
  console.log('Sakura Highway ist bereit: ' + url);
  console.log(KV_URL ? 'Speicher: Upstash' + (kvReady ? '' : ' (noch nicht erreichbar)') : 'Speicher: Datei ' + DATA);
  console.log(adminFromEnv ? 'Admin-Passwort: aus ADMIN_TOKEN' : 'Admin-Passwort: ' + ADMIN + tokenNote);
  if (process.env.OPEN_BROWSER) { // nur über die START-Dateien: Browser von selbst öffnen
    const cmd = process.platform === 'win32' ? ['cmd', ['/c', 'start', '', url]] : process.platform === 'darwin' ? ['open', [url]] : ['xdg-open', [url]];
    try { require('child_process').spawn(cmd[0], cmd[1], { stdio: 'ignore', detached: true }).on('error', () => {}).unref(); } catch (e) {}
  }
}));
