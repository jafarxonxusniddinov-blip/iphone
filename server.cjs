"use strict";
/* ===== Виртуальный iPhone · сервер (Node.js 18+, без зависимостей) =====
   1) отдаёт собранный сайт (папка dist/);
   2) регистрирует «телефонные номера»: ПЕРВЫЙ зарегистрированный (владелец) может выбрать любой номер,
      все остальные получают случайный номер выбранной страны;
   3) хранит и доставляет сообщения между номерами (SSE-канал в реальном времени);
   4) передаёт сигналы WebRTC для голосовых звонков между номерами.
   Запуск: node server/server.js  →  http://localhost:3000
   Переменные: PORT, DATA_FILE, UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN (чтобы данные не терялись на бесплатных хостингах),
               OWNER_KEY (необязательно: секрет, который делает владельцем того, кто его ввёл при регистрации). */
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "data", "db.json");
const R_URL = process.env.UPSTASH_REDIS_REST_URL || "", R_TOK = process.env.UPSTASH_REDIS_REST_TOKEN || "";
const R_KEY = "vphone_db_v1", OWNER_KEY = process.env.OWNER_KEY || "";

let DB = { users: {}, msgs: [], seq: 0 };
let timer = null;
async function redis(cmd) {
  const r = await fetch(R_URL, { method: "POST", headers: { Authorization: "Bearer " + R_TOK, "Content-Type": "application/json" }, body: JSON.stringify(cmd) });
  const j = await r.json(); if (!r.ok || j.error) throw new Error(j.error || "redis"); return j.result;
}
async function load() {
  try {
    if (R_URL && R_TOK) { const s = await redis(["GET", R_KEY]); if (s) DB = JSON.parse(s); }
    else if (fs.existsSync(DATA_FILE)) DB = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch (e) { console.error("load:", e.message); }
  DB.users = DB.users || {}; DB.msgs = DB.msgs || []; DB.seq = DB.seq || 0;
}
function save() {
  clearTimeout(timer);
  timer = setTimeout(async () => {
    try {
      const s = JSON.stringify(DB);
      if (R_URL && R_TOK) await redis(["SET", R_KEY, s]);
      else { fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true }); fs.writeFileSync(DATA_FILE, s); }
    } catch (e) { console.error("save:", e.message); }
  }, 600);
}

const COUNTRIES = { UZ: ["998", 9], RU: ["7", 10], KZ: ["7", 10], UA: ["380", 9], BY: ["375", 9], US: ["1", 10], GB: ["44", 10], DE: ["49", 10], TR: ["90", 10], AE: ["971", 9], KG: ["996", 9], TJ: ["992", 9], AZ: ["994", 9], AM: ["374", 8], GE: ["995", 9], PL: ["48", 9], FR: ["33", 9], IT: ["39", 10], ES: ["34", 9], CN: ["86", 11], IN: ["91", 10], JP: ["81", 10], KR: ["82", 10], BR: ["55", 11], EG: ["20", 10], IL: ["972", 9], SA: ["966", 9], PK: ["92", 10], TM: ["993", 8], MD: ["373", 8] };

const clients = new Map(); // phone -> Set(res)
const rate = new Map();
const json = (res, code, obj) => { res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Authorization, Content-Type", "Access-Control-Allow-Methods": "GET, POST, OPTIONS" }); res.end(JSON.stringify(obj)); };
const hashTok = t => crypto.createHash("sha256").update(t).digest("hex");
const clean = (s, n) => String(s == null ? "" : s).replace(/[\u0000-\u001f]/g, " ").trim().slice(0, n);
function limited(ip, key, max, ms) { const k = ip + key, now = Date.now(); const a = (rate.get(k) || []).filter(t => now - t < ms); a.push(now); rate.set(k, a); return a.length > max; }
function authed(req, url) {
  const h = req.headers.authorization || ""; const t = h.startsWith("Bearer ") ? h.slice(7) : url.searchParams.get("token") || "";
  if (!t) return null; const hh = hashTok(t);
  for (const u of Object.values(DB.users)) if (u.th === hh) return u;
  return null;
}
function pub(u) { return { phone: u.phone, name: u.name, country: u.country, owner: !!u.owner }; }
function push(phone, ev) { const set = clients.get(phone); if (!set) return false; const d = "data: " + JSON.stringify(ev) + "\n\n"; for (const r of set) r.write(d); return set.size > 0; }

function randomNumber(country) {
  const [cc, len] = COUNTRIES[country] || COUNTRIES.UZ;
  for (let i = 0; i < 200; i++) {
    let n = String(1 + crypto.randomInt(9)); while (n.length < len) n += crypto.randomInt(10);
    const phone = "+" + cc + n; if (!DB.users[phone]) return phone;
  }
  return null;
}
function body(req, max = 400000) {
  return new Promise((ok, no) => { let b = "", n = 0; req.on("data", c => { n += c.length; if (n > max) { no(new Error("big")); req.destroy(); } else b += c; }); req.on("end", () => { try { ok(b ? JSON.parse(b) : {}); } catch (e) { no(e); } }); });
}

async function api(req, res, url) {
  const ip = req.socket.remoteAddress || "?", p = url.pathname;
  if (req.method === "OPTIONS") return json(res, 204, {});
  if (p === "/api/ping") return json(res, 200, { ok: true, users: Object.keys(DB.users).length, first: Object.keys(DB.users).length === 0 });

  if (p === "/api/register" && req.method === "POST") {
    if (limited(ip, "reg", 8, 3600e3)) return json(res, 429, { error: "Слишком много регистраций. Попробуйте позже." });
    const b = await body(req); const country = String(b.country || "UZ").toUpperCase(); if (!COUNTRIES[country]) return json(res, 400, { error: "Страна не поддерживается" });
    const first = Object.keys(DB.users).length === 0;
    const owner = first || (OWNER_KEY && b.ownerKey === OWNER_KEY);
    let phone = null;
    if (owner && b.desiredNumber) {
      const d = String(b.desiredNumber).replace(/\D/g, "");
      if (d.length < 7 || d.length > 15) return json(res, 400, { error: "Номер должен содержать от 7 до 15 цифр" });
      phone = "+" + d; if (DB.users[phone]) return json(res, 409, { error: "Этот номер уже занят" });
    } else phone = randomNumber(country);
    if (!phone) return json(res, 503, { error: "Не удалось выдать номер" });
    const token = crypto.randomBytes(24).toString("hex");
    DB.users[phone] = { phone, name: clean(b.name, 40) || "Без имени", country, owner: !!owner, th: hashTok(token), created: Date.now() };
    save(); return json(res, 200, { token, user: pub(DB.users[phone]) });
  }
  const me = authed(req, url);
  if (p === "/api/events") {
    if (!me) return json(res, 401, { error: "auth" });
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no" });
    res.write("retry: 2000\n\n"); let set = clients.get(me.phone); if (!set) clients.set(me.phone, (set = new Set())); set.add(res);
    const hb = setInterval(() => res.write(": hb\n\n"), 20000);
    req.on("close", () => { clearInterval(hb); set.delete(res); if (!set.size) clients.delete(me.phone); });
    return;
  }
  if (!me) return json(res, 401, { error: "Нужно войти" });
  if (p === "/api/me") return json(res, 200, { user: pub(me) });
  if (p === "/api/profile" && req.method === "POST") { const b = await body(req); me.name = clean(b.name, 40) || me.name; save(); return json(res, 200, { user: pub(me) }); }
  if (p === "/api/lookup") { const ph = "+" + String(url.searchParams.get("phone") || "").replace(/\D/g, ""); const u = DB.users[ph]; return json(res, 200, { exists: !!u, name: u ? u.name : null, online: clients.has(ph) }); }
  if (p === "/api/users" && me.owner) return json(res, 200, { users: Object.values(DB.users).map(u => ({ ...pub(u), created: u.created, online: clients.has(u.phone) })) });

  if (p === "/api/send" && req.method === "POST") {
    if (limited(ip + me.phone, "send", 60, 60e3)) return json(res, 429, { error: "Слишком быстро" });
    const b = await body(req, 600000); const to = "+" + String(b.to || "").replace(/\D/g, "");
    if (!DB.users[to]) return json(res, 404, { error: "Такого номера нет в сети" });
    const text = clean(b.text, 4000); const img = typeof b.img === "string" && b.img.startsWith("data:image/") && b.img.length < 450000 ? b.img : undefined;
    if (!text && !img) return json(res, 400, { error: "Пустое сообщение" });
    const m = { id: ++DB.seq, from: me.phone, to, text, img, ts: Date.now() }; DB.msgs.push(m); if (DB.msgs.length > 20000) DB.msgs.splice(0, 2000);
    save(); push(to, { type: "msg", msg: m }); return json(res, 200, { msg: m });
  }
  if (p === "/api/sync") {
    const since = Number(url.searchParams.get("since")) || 0;
    return json(res, 200, { msgs: DB.msgs.filter(m => m.id > since && (m.to === me.phone || m.from === me.phone)).slice(0, 500), seq: DB.seq });
  }
  if (p === "/api/call" && req.method === "POST") {
    const b = await body(req, 100000); const to = "+" + String(b.to || "").replace(/\D/g, "");
    if (!DB.users[to]) return json(res, 404, { error: "Такого номера нет в сети" });
    const ev = { type: "call-" + String(b.kind || "").slice(0, 12), from: me.phone, name: me.name, callId: clean(b.callId, 40), data: b.data };
    const delivered = push(to, ev); return json(res, 200, { delivered });
  }
  return json(res, 404, { error: "not found" });
}

const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png", ".webmanifest": "application/manifest+json", ".woff2": "font/woff2", ".ico": "image/x-icon" };
function serve(req, res, url) {
  const f = path.join(ROOT, "index.html");
  if (!fs.existsSync(f)) { res.writeHead(404); return res.end("Сначала выполните npm run build"); }
  const ext = path.extname(f); const long = /\/assets\//.test(f);
  res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream", "Cache-Control": long ? "public, max-age=31536000, immutable" : ext === ".html" || f.endsWith("sw.js") ? "no-cache" : "public, max-age=3600" });
  fs.createReadStream(f).pipe(res);
}
load().then(() => http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname.startsWith("/api/")) api(req, res, url).catch(e => json(res, 400, { error: e.message === "big" ? "Слишком большой запрос" : "Ошибка запроса" }));
  else serve(req, res, url);
}).listen(PORT, () => console.log("Виртуальный iPhone: http://localhost:" + PORT)));
