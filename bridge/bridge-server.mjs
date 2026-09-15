// bridge-server.mjs — Codex ↔ ChatGPT 网页会话桥接服务（零依赖）
//
// 架构：
//   Codex CLI/桌面端 ──(Responses API, 本地 HTTP/SSE)──▶ 本服务 (127.0.0.1:8788)
//   本服务 ──(WebSocket /relay)──▶ Chrome 扩展 relay 内容脚本（chatgpt.com 页面内）
//   relay ──(同源 fetch, 真实浏览器指纹+Cookie)──▶ chatgpt.com/backend-api/f|/conversation
//
// 存在浏览器中继的原因：OpenAI 对 POST 型推理接口启用了设备指纹风控，本地进程
// 即使携带有效 token 也会被 403（"Unusual activity has been detected"）拒绝，
// 只有真实浏览器标签页的同源请求可稳定通过。
//
// 用法： node bridge/bridge-server.mjs [--port 8788] [--web-model gpt-5-5]

import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------- 配置 ----------
const argv = process.argv.slice(2);
const argVal = (n, d) => { const i = argv.indexOf(n); return i >= 0 && argv[i + 1] ? argv[i + 1] : d; };
const PORT = parseInt(argVal('--port', process.env.BRIDGE_PORT || '8788'), 10);
const WEB_MODEL = argVal('--web-model', process.env.BRIDGE_WEB_MODEL || 'gpt-5-5');
const HOST = '127.0.0.1';
const UPSTREAM_F = '/backend-api/f/conversation';
const UPSTREAM_LEGACY = '/backend-api/conversation';
const UPSTREAM_TIMEOUT_MS = 300000;
const CATALOG_PATH = path.join(__dirname, 'models-codex.json');

// ---------- 日志 ----------
const logs = [];
function log(...parts) {
  const line = `[${new Date().toISOString().slice(11, 19)}] ${parts.map(p => (typeof p === 'string' ? p : JSON.stringify(p))).join(' ')}`;
  console.log(line);
  logs.push(line);
  if (logs.length > 800) logs.splice(0, logs.length - 800);
}

// ---------- WebSocket 服务端（RFC6455 最小子集） ----------
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const wsAccept = key => crypto.createHash('sha1').update(key + WS_GUID).digest('base64');

function encodeFrame(text, opcode = 1) {
  const payload = Buffer.from(String(text), 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x80 | opcode;
  return Buffer.concat([header, payload]);
}

function createWsParser(onFrame) {
  let buf = Buffer.alloc(0);
  let fragOpcode = null, frags = [];
  return function feed(chunk) {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 2) return;
      const b0 = buf[0], b1 = buf[1];
      const fin = (b0 & 0x80) !== 0, opcode = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (buf.length < off + 2) return; len = buf.readUInt16BE(off); off += 2; }
      else if (len === 127) { if (buf.length < off + 8) return; len = Number(buf.readBigUInt64BE(off)); off += 8; }
      let mask = null;
      if (masked) { if (buf.length < off + 4) return; mask = Buffer.from(buf.subarray(off, off + 4)); off += 4; }
      if (buf.length < off + len) return;
      const payload = Buffer.from(buf.subarray(off, off + len));
      buf = buf.subarray(off + len);
      if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      if (opcode === 0x8) return;
      if (opcode === 0x9 || opcode === 0xA) { onFrame(opcode, payload); continue; }
      if (fin && frags.length === 0) { onFrame(opcode, payload); continue; }
      if (!fin && frags.length === 0) { fragOpcode = opcode; frags = [payload]; continue; }
      frags.push(payload);
      if (fin) { onFrame(fragOpcode, Buffer.concat(frags)); fragOpcode = null; frags = []; }
    }
  };
}

// ---------- relay 状态 ----------
let relaySocket = null;
let relayInfo = null;
let reqSeq = 0;
const pending = new Map();
const wsSenders = new WeakMap();
const relayOnline = () => !!(relaySocket && relaySocket.writable);

/**
 * 通过 relay 发一次上游请求。
 * 返回 ctl = { head: Promise<{status,headers}>, finished: Promise<{ok,error}>, entry }
 * onChunk(text) 在状态未知前也会收到数据（调用方自行缓冲）。
 */
function relayFetch({ method, urlPath, headers, body }, onChunk) {
  const id = ++reqSeq;
  const entry = { id, raw: '', headSettled: false, done: false };
  entry.head = new Promise((resolve, reject) => { entry.resolveHead = resolve; entry.rejectHead = reject; });
  entry.finished = new Promise(resolve => { entry.resolveFinished = resolve; });
  entry.finish = (ok, error) => {
    if (entry.done) return;
    entry.done = true;
    clearTimeout(entry.timer);
    pending.delete(id);
    if (!entry.headSettled) { entry.headSettled = true; ok ? entry.resolveHead({ status: 200, headers: {} }) : entry.rejectHead(error); }
    entry.resolveFinished({ ok, error: error ? String(error.message || error) : null });
  };
  entry.fail = err => entry.finish(false, err instanceof Error ? err : new Error(String(err)));
  entry.push = text => { entry.raw += text; try { onChunk && onChunk(text); } catch (e) { log('onChunk error', e.message); } };

  if (!relayOnline()) { entry.fail(new Error('NO_RELAY')); return entry; }
  pending.set(id, entry);
  try {
    wsSenders.get(relaySocket)(JSON.stringify({ t: 'http', id, method, path: urlPath, headers: headers || {}, body: body ?? null }));
    entry.timer = setTimeout(() => entry.fail(new Error('relay timeout')), UPSTREAM_TIMEOUT_MS);
  } catch (e) { entry.fail(e); }
  return entry;
}

function handleRelayMessage(raw) {
  let msg; try { msg = JSON.parse(raw); } catch { return; }
  if (msg.t === 'hello') {
    relayInfo = msg;
    log('relay hello', { version: msg.version, plan: msg.session?.plan, hasToken: !!msg.session?.accessToken, expires: msg.session?.expires });
    return;
  }
  const p = pending.get(msg.id);
  if (!p) return;
  if (msg.t === 'capture') { if (p.onCapture) p.onCapture(msg.data); return; }
  if (msg.t === 'probe') { if (p.onCapture) p.onCapture(msg.data); return; }
  if (msg.t === 'head') { if (!p.headSettled) { p.headSettled = true; p.resolveHead({ status: msg.status, headers: msg.headers || {} }); } }
  else if (msg.t === 'data') { p.push(msg.text || ''); }
  else if (msg.t === 'end') { p.finish(true); }
  else if (msg.t === 'err') { p.fail(new Error('relay fetch failed: ' + (msg.message || 'unknown'))); }
}

// ---------- /v1/responses 处理（f/conversation 优先，legacy 回退） ----------
async function handleResponses(reqBody, res, depth = 0) {
  const noTools = reqBody.__noTools;
  const upstreamBody = { ...reqBody, model: mapModel(reqBody.model), stream: true };
  delete upstreamBody.background; delete upstreamBody.__noTools;
  if (noTools) { delete upstreamBody.tools; delete upstreamBody.tool_choice; delete upstreamBody.parallel_tool_calls; }

  log('responses → f/conversation', { model: reqBody.model, mapped: upstreamBody.model, keys: Object.keys(reqBody), noTools: !!noTools });

  const preHead = [];
  let pipe = null;
  const ctl = relayFetch(
    { method: 'POST', urlPath: UPSTREAM_F,
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify(upstreamBody) },
    text => { pipe ? res.write(text) : preHead.push(text); }
  );

  let head;
  try { head = await ctl.head; }
  catch (e) {
    if (e.message === 'NO_RELAY') return write502NoRelay(res);
    log('f head error', e.message);
    return beginFallback(reqBody, res, 'f fetch error: ' + e.message);
  }
  log('f/conversation status', head.status);

  if (head.status === 200) {
    pipe = true;
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
    for (const c of preHead) res.write(c);
    const fin = await ctl.finished;
    if (!res.writableEnded) {
      if (!fin.ok) res.write(`data: ${JSON.stringify({ type: 'response.failed', response: { status: 'failed', error: { message: '中继中断: ' + fin.error } } })}\n\n`);
      res.end();
    }
    return;
  }

  await ctl.finished;
  const bodyText = (ctl.raw || '').slice(0, 800);
  log('f non-200 body preview', bodyText.slice(0, 400));

  if (head.status === 404 || head.status === 405) return beginFallback(reqBody, res, 'f endpoint missing');
  if (!noTools && head.status === 400 && /\btools?\b|\bfunction/i.test(bodyText) && depth === 0) {
    log('f 400 mentions tools — retry without tools');
    reqBody.__noTools = true;
    return handleResponses(reqBody, res, depth + 1);
  }
  res.writeHead(head.status >= 500 ? 502 : head.status, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: { type: 'bridge_upstream_error', message: `上游 ${head.status}: ${bodyText.slice(0, 400)}` } }));
}

function beginFallback(reqBody, res, reason) {
  if (res.headersSent) return;
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  handleLegacyConversation(reqBody, res, reason).catch(e => { log('legacy fatal', e.message); if (!res.writableEnded) res.end(); });
}

function write502NoRelay(res) {
  if (res.headersSent) { !res.writableEnded && res.end(); return; }
  res.writeHead(502, { 'content-type': 'application/json' });
  res.end(JSON.stringify({
    error: {
      type: 'bridge_offline',
      message: '浏览器中继未连接。请在 Chrome 中登录并停留一个 chatgpt.com 标签页（保持打开），且已启用带 relay 的 Codex认证助手扩展。',
    },
  }));
}

function mapModel(m) {
  if (!m) return WEB_MODEL;
  if (/5[.-]6/.test(m)) return m.includes('thinking') ? 'gpt-5-6-thinking' : (/mini/.test(m) ? 'gpt-5-6-mini' : 'gpt-5-6');
  return WEB_MODEL;
}

// ---------- 旧版 /conversation 回退（chat-only） ----------
function makeSSELineProcessor(onLine) {
  let lineBuf = '';
  return text => {
    lineBuf += text;
    let idx;
    while ((idx = lineBuf.indexOf('\n')) >= 0) {
      const line = lineBuf.slice(0, idx).replace(/\r$/, '');
      lineBuf = lineBuf.slice(idx + 1);
      if (line) onLine(line);
    }
  };
}

async function handleLegacyConversation(reqBody, res, reason) {
  log('legacy fallback engaged:', reason || 'f unavailable');
  const msgs = extractUserAssistantMessages(reqBody);
  const text = msgs.map(m => m.text).join('\n\n');
  if (!text) { writeSSEError(res, 400, '空输入'); return; }
  const rid = 'resp_' + crypto.randomUUID().replace(/-/g, '');
  const msgId = 'msg_' + crypto.randomUUID().replace(/-/g, '');
  res.write(`data: ${JSON.stringify({ type: 'response.created', response: { id: rid, object: 'response', status: 'in_progress', model: reqBody.model || WEB_MODEL } })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'response.output_item.added', item: { id: msgId, type: 'message', role: 'assistant', content: [] } })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'response.content_part.added', item_id: msgId, part: { type: 'output_text', text: '' } })}\n\n`);

  const body = {
    action: 'next',
    messages: [{ id: crypto.randomUUID(), author: { role: 'user' }, content: { content_type: 'text', parts: [text] }, metadata: {} }],
    parent_message_id: '00000000-0000-0000-0000-000000000000',
    model: WEB_MODEL, prompt: null, timezone: 'Asia/Shanghai', group_id: null, sandbox_id: null,
    allowed_tools: [], system_hints: [''], journal_id: null, stream: true,
  };
  let full = '';
  const feed = makeSSELineProcessor(line => {
    if (!line.startsWith('data:')) return;
    try {
      const j = JSON.parse(line.slice(5));
      const m = j.message;
      if (m && m.author?.role === 'assistant' && m.content?.content_type === 'text') {
        const parts = Array.isArray(m.content.parts) ? m.content.parts.join('') : '';
        const delta = parts.startsWith(full) ? parts.slice(full.length) : parts;
        if (delta) { full += delta; res.write(`data: ${JSON.stringify({ type: 'response.output_text.delta', item_id: msgId, delta })}\n\n`); }
      }
    } catch {}
  });
  const ctl = relayFetch(
    { method: 'POST', urlPath: UPSTREAM_LEGACY,
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify(body) },
    feed
  );
  try {
    const head = await ctl.head;
    if (head.status !== 200) { await ctl.finished; throw new Error(`legacy ${head.status}: ${(ctl.raw || '').slice(0, 300)}`); }
    await ctl.finished;
  } catch (e) {
    res.write(`data: ${JSON.stringify({ type: 'response.failed', response: { status: 'failed', error: { message: '网页上游失败: ' + e.message } } })}\n\n`);
    !res.writableEnded && res.end();
    return;
  }
  res.write(`data: ${JSON.stringify({ type: 'response.output_text.done', item_id: msgId, text: full })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'response.output_item.done', item: { id: msgId, type: 'message', role: 'assistant', content: [{ type: 'output_text', text: full }] } })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'response.completed', response: { id: rid, status: 'completed', model: reqBody.model || WEB_MODEL, output: [{ id: msgId, type: 'message', role: 'assistant', content: [{ type: 'output_text', text: full }] }] } })}\n\n`);
  !res.writableEnded && res.end();
}

function extractUserAssistantMessages(reqBody) {
  const out = [];
  const items = Array.isArray(reqBody.input) ? reqBody.input : (typeof reqBody.input === 'string' ? [{ role: 'user', text: reqBody.input }] : []);
  for (const it of items) {
    if (it.type && it.type !== 'message') continue;
    if (it.role !== 'user' && it.role !== 'assistant') continue;
    let text = '';
    const c = it.content;
    if (typeof c === 'string') text = c;
    else if (Array.isArray(c)) text = c.map(x => (x.type === 'input_text' || x.type === 'output_text') ? x.text : '').join('\n');
    if (text) out.push({ role: it.role, text });
  }
  return out;
}

function writeSSEError(res, status, detail) {
  if (!res.headersSent) {
    res.writeHead(status >= 500 ? 502 : status, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: `桥接错误 (${status}): ${detail}` } }));
  } else if (!res.writableEnded) {
    res.write(`data: ${JSON.stringify({ type: 'response.failed', response: { status: 'failed', error: { message: detail } } })}\n\n`);
    res.end();
  }
}

// ---------- HTTP ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${HOST}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': req.headers.origin || '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': req.headers['access-control-request-headers'] || '*',
      'access-control-allow-private-network': 'true',
      'access-control-max-age': '600',
    });
    return res.end();
  }

  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      ok: true, relay: relayOnline(),
      session: relayInfo?.session ? { plan: relayInfo.session.plan, expires: relayInfo.session.expires, hasToken: !!relayInfo.session.accessToken } : null,
      webModel: WEB_MODEL,
    }));
  }

  if (url.pathname === '/v1/models' || url.pathname === '/models') {
    try {
      const cat = JSON.parse(fs.readFileSync(CATALOG_PATH, 'utf8'));
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify(cat));
    } catch (e) {
      res.writeHead(500, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: { message: 'catalog missing: ' + e.message } }));
    }
  }

  if (url.pathname === '/v1/responses' && req.method === 'POST') {
    let raw = '';
    req.on('data', c => (raw += c));
    req.on('end', () => {
      let body;
      try { body = JSON.parse(raw); } catch { res.writeHead(400); return res.end('bad json'); }
      handleResponses(body, res).catch(e => { log('handleResponses fatal', e.stack || e.message); writeSSEError(res, 500, e.message); });
    });
    return;
  }

  if (url.pathname === '/logs') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end(logs.join('\n'));
  }

  if (url.pathname === '/debug/probe' && req.method === 'GET') {
    if (!relayOnline()) { res.writeHead(502); return res.end('no relay'); }
    const id = ++reqSeq;
    const entry = { id, headSettled: true, done: false };
    let resolveWrap;
    const p = new Promise(r => (resolveWrap = r));
    entry.onCapture = data => { if (!entry.done) { entry.done = true; pending.delete(id); resolveWrap(data || {}); } };
    pending.set(id, entry);
    try { wsSenders.get(relaySocket)(JSON.stringify({ t: 'probe_sentinel', id })); } catch (e) { entry.onCapture({}); }
    setTimeout(() => entry.onCapture({}), 8000);
    const data = await p;
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(data));
    return;
  }

  if (url.pathname === '/debug/capture' && req.method === 'GET') {
    if (!relayOnline()) { res.writeHead(502); return res.end('no relay'); }
    const id = ++reqSeq;
    const entry = { id, headSettled: true, done: false };
    let resolveWrap;
    const p = new Promise(r => (resolveWrap = r));
    entry.onCapture = data => { if (!entry.done) { entry.done = true; pending.delete(id); resolveWrap(data || []); } };
    pending.set(id, entry);
    try { wsSenders.get(relaySocket)(JSON.stringify({ t: 'get_capture', id })); } catch (e) { entry.onCapture([]); }
    setTimeout(() => entry.onCapture([]), 8000);
    const data = await p;
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(data));
    return;
  }

  if (url.pathname === '/debug/dofetch' && req.method === 'POST') {
    let raw = '';
    req.on('data', c => (raw += c));
    req.on('end', async () => {
      if (!relayOnline()) { res.writeHead(502); return res.end('no relay'); }
      const spec = JSON.parse(raw);
      const id = ++reqSeq;
      const entry = { id, raw: '', headSettled: false, done: false };
      entry.head = new Promise((resolve) => { entry.resolveHead = resolve; });
      entry.finished = new Promise(res2 => { entry.resolveFinished = res2; });
      entry.finish = () => { if (!entry.done) { entry.done = true; clearTimeout(entry.timer); pending.delete(id); if (!entry.headSettled) { entry.headSettled = true; entry.resolveHead({ status: 0 }); } entry.resolveFinished(entry.raw); } };
      entry.push = t => { if (!entry.headSettled) { entry.headSettled = true; entry.resolveHead({ status: 200 }); } entry.raw += t; };
      pending.set(id, entry);
      log('dofetch →', spec.path);
      try { wsSenders.get(relaySocket)(JSON.stringify({ t: 'do_fetch', id, path: spec.path, method: spec.method || 'POST', headers: spec.headers || {}, body: spec.body, stream: true })); } catch (e) { res.writeHead(500); return res.end(String(e)); }
      entry.timer = setTimeout(() => { log('dofetch timeout', spec.path); entry.finish(); }, 25000);
      const head = await entry.head;
      await entry.finished;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: head && head.status, body: entry.raw.slice(0, 6000) }));
    });
    return;
  }

  if (url.pathname === '/debug/fetch' && req.method === 'POST') {
    let raw = '';
    req.on('data', c => (raw += c));
    req.on('end', async () => {
      try {
        const spec = JSON.parse(raw);
        let head = null;
        const ctl = relayFetch(spec, t => {});
        try { head = await ctl.head; } catch (e) { res.writeHead(502, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ error: e.message })); }
        await ctl.finished;
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ status: head.status, headers: head.headers, body: (ctl.raw || '').slice(0, 4000) }));
      } catch (e) {
        res.writeHead(500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: { message: 'not found' } }));
});

// ---------- WS upgrade ----------
server.on('upgrade', (req, socket) => {
  if (!req.url.startsWith('/relay')) { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${wsAccept(key)}\r\n\r\n`
  );
  socket.setNoDelay(true);
  if (relaySocket && relaySocket !== socket) {
    log('relay replaced by newer connection');
    const old = relaySocket;
    for (const [, p] of pending) p.fail(new Error('relay replaced'));
    try { old.destroy(); } catch {}
  }
  relaySocket = socket;
  const feed = createWsParser((opcode, payload) => {
    if (opcode === 1) handleRelayMessage(payload.toString('utf8'));
    else if (opcode === 9) { try { socket.write(encodeFrame(payload.toString('binary'), 10)); } catch {} }
    else if (opcode === 8) { try { socket.destroy(); } catch {} }
  });
  wsSenders.set(socket, text => { if (socket.writable) socket.write(encodeFrame(text)); });
  socket.on('data', chunk => { try { feed(chunk); } catch (e) { log('ws parse error', e.message); socket.destroy(); } });
  const drop = () => {
    if (relaySocket === socket) { relaySocket = null; relayInfo = null; log('relay disconnected'); }
    for (const [, p] of pending) p.fail(new Error('relay lost'));
  };
  socket.on('close', drop);
  socket.on('error', drop);
  log('relay socket upgraded');
});

server.listen(PORT, HOST, () => {
  log(`Codex Web bridge on http://${HOST}:${PORT}`);
  log(`  web model: ${WEB_MODEL} | relay: ws://${HOST}:${PORT}/relay`);
});

process.on('uncaughtException', e => log('uncaught', e.stack || e.message));
process.on('unhandledRejection', e => log('unhandled', String((e && e.message) || e)));
