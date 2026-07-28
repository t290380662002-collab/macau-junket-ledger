// 洗碼記帳表 + 訂房管理 — 零依賴版本（僅用 Node 內建模組）
// 即時同步用 SSE，無需安裝套件。啟動：node server.js → http://localhost:3000

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DATA_FILE = path.join(DATA_DIR, 'data.json');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon'
};

// ---------- 資料存取 ----------
function loadData() {
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch { return { entries: [], bookings: [] }; }
}
function saveData() {
  try {
    const dir = path.dirname(DATA_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error('⚠️ 寫入資料失敗:', err.message);
  }
}
let data = loadData();
if (!data.bookings) data.bookings = [];

// ---------- SSE 客戶端 ----------
const clients = new Set();
function broadcast(msg) {
  const payload = `data: ${JSON.stringify(msg)}\n\n`;
  clients.forEach(res => { try { res.write(payload); } catch {} });
}

// ---------- 工具 ----------
function sendJSON(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve) => {
    let buf = '';
    req.on('data', c => { buf += c; if (buf.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(buf || '{}')); } catch { resolve({}); } });
  });
}
function serveStatic(req, res) {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(PUBLIC_DIR, path.normalize(urlPath).replace(/^(\.\.[/\\])+/, ''));
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(filePath, (err, content) => {
    if (err) { res.writeHead(404); return res.end('Not Found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(content);
  });
}

// ==================== 路由 ====================
const server = http.createServer(async (req, res) => {
  const url = req.url.split('?')[0];

  // SSE 即時串流
  if (url === '/api/stream' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write(`data: ${JSON.stringify({ type: 'init', entries: data.entries, bookings: data.bookings })}\n\n`);
    clients.add(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
    req.on('close', () => { clearInterval(ping); clients.delete(res); });
    return;
  }

  // ─── 洗碼 entries ───
  if (url === '/api/entries' && req.method === 'GET') {
    return sendJSON(res, 200, data.entries);
  }

  if (url === '/api/entries' && req.method === 'POST') {
    const e = await readBody(req);
    const entry = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      date: e.date || new Date().toISOString().slice(0, 10),
      property: ['金沙', '新濠'].includes(e.property) ? e.property : '金沙',
      customer: (e.customer || '').toString().trim(),
      rolling: Number(e.rolling) || 0,
      amount: Number(e.amount) || 0,
      note: (e.note || '').toString().trim(),
      createdAt: Date.now()
    };
    data.entries.push(entry);
    saveData();
    broadcast({ type: 'add', entry });
    return sendJSON(res, 200, entry);
  }

  // ─── 訂房 bookings ───
  if (url === '/api/bookings' && req.method === 'GET') {
    return sendJSON(res, 200, data.bookings);
  }

  if (url === '/api/bookings' && req.method === 'POST') {
    const b = await readBody(req);
    const booking = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      status: b.status || '',
      confirmNo: (b.confirmNo || '').toString().trim(),
      agent: (b.agent || '').toString().trim(),
      guest: (b.guest || '').toString().trim(),
      hotel: (b.hotel || '').toString().trim(),
      roomType: (b.roomType || '').toString().trim(),
      checkin: b.checkin || '',
      checkout: b.checkout || '',
      nights: Number(b.nights) || 0,
      weekdayNights: Number(b.weekdayNights) || 0,
      weekendNights: Number(b.weekendNights) || 0,
      price: Number(b.price) || 0,
      cost: Number(b.cost) || 0,
      rolling: Number(b.rolling) || 0,
      profit: Number(b.profit) || 0,
      note: (b.note || '').toString().trim(),
      createdAt: Date.now()
    };
    data.bookings.push(booking);
    saveData();
    broadcast({ type: 'addBooking', booking });
    return sendJSON(res, 200, booking);
  }

  // ─── 共用：單筆 修改/刪除 (entries & bookings) ───
  const updEntry = url.match(/^\/api\/entries\/([\w-]+)$/);
  const updBooking = url.match(/^\/api\/bookings\/([\w-]+)$/);

  // 修改 entry
  if (updEntry && req.method === 'PUT') {
    const e = await readBody(req);
    const idx = data.entries.findIndex(x => x.id === updEntry[1]);
    if (idx === -1) return sendJSON(res, 404, { error: 'not found' });
    const cur = data.entries[idx];
    data.entries[idx] = {
      ...cur,
      date: e.date ?? cur.date,
      property: ['金沙', '新濠'].includes(e.property) ? e.property : cur.property,
      customer: e.customer !== undefined ? e.customer.toString().trim() : cur.customer,
      rolling: e.rolling !== undefined ? (Number(e.rolling) || 0) : cur.rolling,
      amount: e.amount !== undefined ? (Number(e.amount) || 0) : cur.amount,
      note: e.note !== undefined ? e.note.toString().trim() : cur.note
    };
    saveData();
    broadcast({ type: 'update', entry: data.entries[idx] });
    return sendJSON(res, 200, data.entries[idx]);
  }

  // 刪除 entry
  if (updEntry && req.method === 'DELETE') {
    const idx = data.entries.findIndex(x => x.id === updEntry[1]);
    if (idx === -1) return sendJSON(res, 404, { error: 'not found' });
    const [removed] = data.entries.splice(idx, 1);
    saveData();
    broadcast({ type: 'delete', id: removed.id });
    return sendJSON(res, 200, { ok: true });
  }

  // 修改 booking
  if (updBooking && req.method === 'PUT') {
    const b = await readBody(req);
    const idx = data.bookings.findIndex(x => x.id === updBooking[1]);
    if (idx === -1) return sendJSON(res, 404, { error: 'not found' });
    const cur = data.bookings[idx];
    const numFields = ['nights','weekdayNights','weekendNights','price','cost','rolling','profit'];
    const strFields = ['status','confirmNo','agent','guest','hotel','roomType','checkin','checkout','note'];
    const updated = { ...cur };
    numFields.forEach(f => { if (b[f] !== undefined) updated[f] = Number(b[f]) || 0; });
    strFields.forEach(f => { if (b[f] !== undefined) updated[f] = b[f].toString().trim(); });
    data.bookings[idx] = updated;
    saveData();
    broadcast({ type: 'updateBooking', booking: data.bookings[idx] });
    return sendJSON(res, 200, data.bookings[idx]);
  }

  // 刪除 booking
  if (updBooking && req.method === 'DELETE') {
    const idx = data.bookings.findIndex(x => x.id === updBooking[1]);
    if (idx === -1) return sendJSON(res, 404, { error: 'not found' });
    const [removed] = data.bookings.splice(idx, 1);
    saveData();
    broadcast({ type: 'deleteBooking', id: removed.id });
    return sendJSON(res, 200, { ok: true });
  }

  // 診斷端點
  if (url === '/api/status' && req.method === 'GET') {
    return sendJSON(res, 200, {
      uptime: process.uptime(),
      dataDir: DATA_DIR,
      dataFile: DATA_FILE,
      dataDirExists: fs.existsSync(path.dirname(DATA_FILE)),
      dataFileExists: fs.existsSync(DATA_FILE),
      entriesCount: data.entries.length,
      bookingsCount: data.bookings.length,
      clientsCount: clients.size,
      env: { DATA_DIR: process.env.DATA_DIR, PORT: process.env.PORT, RAILWAY_VOLUME_MOUNT_PATH: process.env.RAILWAY_VOLUME_MOUNT_PATH }
    });
  }

  // 其餘 → 靜態檔案
  serveStatic(req, res);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`洗碼記帳表已啟動：http://localhost:${PORT}`);
});
