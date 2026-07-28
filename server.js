// 洗碼記帳表 — 零依賴版本（僅用 Node 內建模組）
// 即時同步改用 Server-Sent Events (SSE)，無需安裝任何套件。
// 啟動：node server.js   →   http://localhost:3000

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || __dirname;   // Railway 掛載 Volume 時指到此目錄以持久化
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
  catch { return { entries: [] }; }
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

// ---------- SSE 客戶端清單 ----------
const clients = new Set();
function broadcast(msg) {
  const payload = `data: ${JSON.stringify(msg)}\n\n`;
  clients.forEach(res => { try { res.write(payload); } catch {} });
}

// ---------- 工具 ----------
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
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

// ---------- 路由 ----------
const server = http.createServer(async (req, res) => {
  const url = req.url.split('?')[0];

  // SSE 即時串流
  if (url === '/api/stream' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write(`data: ${JSON.stringify({ type: 'init', entries: data.entries })}\n\n`);
    clients.add(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
    req.on('close', () => { clearInterval(ping); clients.delete(res); });
    return;
  }

  // 取得全部
  if (url === '/api/entries' && req.method === 'GET') {
    return sendJSON(res, 200, data.entries);
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
      clientsCount: clients.size,
      env: { DATA_DIR: process.env.DATA_DIR, PORT: process.env.PORT, RAILWAY_VOLUME_MOUNT_PATH: process.env.RAILWAY_VOLUME_MOUNT_PATH }
    });
  }

  // 新增
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

  // 修改
  const upd = url.match(/^\/api\/entries\/([\w-]+)$/);
  if (upd && req.method === 'PUT') {
    const e = await readBody(req);
    const idx = data.entries.findIndex(x => x.id === upd[1]);
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

  // 刪除
  if (upd && req.method === 'DELETE') {
    const idx = data.entries.findIndex(x => x.id === upd[1]);
    if (idx === -1) return sendJSON(res, 404, { error: 'not found' });
    const [removed] = data.entries.splice(idx, 1);
    saveData();
    broadcast({ type: 'delete', id: removed.id });
    return sendJSON(res, 200, { ok: true });
  }

  // 其餘 → 靜態檔案
  serveStatic(req, res);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`洗碼記帳表已啟動：http://localhost:${PORT}`);
});
