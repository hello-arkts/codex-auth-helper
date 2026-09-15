// relay.js — Codex 认证助手 浏览器中继内容脚本
// 运行在 chatgpt.com 页面内（同源），把本地桥接服务（bridge-server.mjs）的
// HTTP 请求转发到 ChatGPT 后端接口，并用 WebSocket 把 SSE 流式响应逐块回传。
// 只有 WebSocket 控制消息会经过本机 127.0.0.1，不向任何第三方传输数据。

(() => {
  if (window.__codexAuthHelperRelay) return;
  window.__codexAuthHelperRelay = true;

  const BRIDGE_WS = 'ws://127.0.0.1:8788/relay';
  const RECONNECT_MS = 4000;
  const RELAY_VERSION = '1.2.0';

  let ws = null;
  let session = null; // { accessToken, accountId, expires, plan }

  async function refreshSession() {
    try {
      const r = await fetch('/api/auth/session', { credentials: 'include', headers: { 'cache-control': 'no-cache' } });
      const j = await r.json().catch(() => null);
      if (j && j.accessToken) {
        session = {
          accessToken: j.accessToken,
          accountId: j.account && j.account.id,
          plan: j.account && j.account.planType,
          expires: j.expires,
        };
      } else {
        session = null;
      }
    } catch (e) {
      session = null;
    }
    return session;
  }

  // 定期刷新会话 token（网页端会自动轮换 accessToken）
  setInterval(refreshSession, 10 * 60 * 1000);

  function connect() {
    try {
      ws = new WebSocket(BRIDGE_WS);
    } catch (e) {
      setTimeout(connect, RECONNECT_MS);
      return;
    }

    ws.onopen = async () => {
      if (!session) await refreshSession();
      if (!session) return; // 未登录的页面不作为中继
      ws.send(JSON.stringify({ t: 'hello', kind: 'codex-auth-helper-relay', version: RELAY_VERSION, session }));
    };

    ws.onmessage = ev => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch { return; }
      if (msg.t === 'get_capture') { getCapture(msg.id); return; }
      if (msg.t === 'probe_sentinel') { probeSentinel(msg.id); return; }
      if (msg.t === 'do_fetch') { doFetchViaPage(msg); return; }
      if (msg.t !== 'http') return;
      handle(msg).catch(err => send({ id: msg.id, t: 'err', message: String((err && err.message) || err) }));
    };

    ws.onclose = () => setTimeout(connect, RECONNECT_MS);
    ws.onerror = () => { try { ws.close(); } catch {} };
  }

  function send(obj) {
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
  }

  function getCapture(id) {
    const onResult = ev => {
      if (ev.source !== window) return;
      const d = ev.data;
      if (d && d.__codexCaptureResult) {
        window.removeEventListener('message', onResult);
        send({ id, t: 'capture', data: d.__codexCaptureResult });
      }
    };
    window.addEventListener('message', onResult);
    window.postMessage({ __codexRelayCmd: 'get_capture' }, '*');
    setTimeout(() => { window.removeEventListener('message', onResult); send({ id, t: 'capture', data: [] }); }, 3000);
  }

  function probeSentinel(id) {
    const onResult = ev => {
      if (ev.source !== window) return;
      const d = ev.data;
      if (d && d.__codexProbeResult) {
        window.removeEventListener('message', onResult);
        send({ id, t: 'probe', data: d.__codexProbeResult });
      }
    };
    window.addEventListener('message', onResult);
    window.postMessage({ __codexRelayCmd: 'probe_sentinel' }, '*');
    setTimeout(() => { window.removeEventListener('message', onResult); send({ id, t: 'probe', data: { timeout: true } }); }, 3000);
  }

  // 通过页面 MAIN world 的 window.fetch 发请求（让 app 的 sentinel 注入生效），
  // 把 head/chunk/end 转成 WS 消息回传桥接服务。
  function doFetchViaPage(msg) {
    const id = msg.id;
    const onMsg = ev => {
      if (ev.source !== window) return;
      const d = ev.data;
      if (!d) return;
      if (d.__codexFetchResult && d.__codexFetchResult.id === id) {
        send({ id, t: 'head', status: d.__codexFetchResult.status, headers: d.__codexFetchResult.headers });
      } else if (d.__codexFetchChunk && d.__codexFetchChunk.id === id) {
        send({ id, t: 'data', text: d.__codexFetchChunk.text });
      } else if (d.__codexFetchEnd && d.__codexFetchEnd.id === id) {
        window.removeEventListener('message', onMsg);
        send({ id, t: 'end' });
      } else if (d.__codexFetchErr && d.__codexFetchErr.id === id) {
        window.removeEventListener('message', onMsg);
        send({ id, t: 'err', message: d.__codexFetchErr.message });
      }
    };
    window.addEventListener('message', onMsg);
    const headers = Object.assign({}, msg.headers || {});
    if (!headers['authorization'] && session && session.accessToken) headers['authorization'] = 'Bearer ' + session.accessToken;
    if (!headers['chatgpt-account-id'] && session && session.accountId) headers['chatgpt-account-id'] = session.accountId;
    window.postMessage({ __codexRelayCmd: 'do_fetch', req: { id, url: msg.path, method: msg.method, headers, body: msg.body, stream: msg.stream } }, '*');
    setTimeout(() => { window.removeEventListener('message', onMsg); }, 180000);
  }

  async function handle(msg) {
    const headers = Object.assign({}, msg.headers || {});
    // 由中继端注入鉴权信息（保持同源请求的 Cookie 自动携带）
    if (session && session.accessToken && !headers['authorization']) {
      headers['authorization'] = 'Bearer ' + session.accessToken;
    }
    if (session && session.accountId && !headers['chatgpt-account-id']) {
      headers['chatgpt-account-id'] = session.accountId;
    }

    const res = await fetch(msg.path, {
      method: msg.method || 'GET',
      headers,
      body: msg.body != null ? msg.body : undefined,
      credentials: 'include',
    });

    const outHeaders = {};
    res.headers.forEach((v, k) => { outHeaders[k] = v; });
    send({ id: msg.id, t: 'head', status: res.status, headers: outHeaders });

    if (!res.body) {
      const t = await res.text();
      if (t) send({ id: msg.id, t: 'data', text: t });
      send({ id: msg.id, t: 'end' });
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder('utf-8');
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      const text = decoder.decode(value, { stream: true });
      if (text) send({ id: msg.id, t: 'data', text });
    }
    send({ id: msg.id, t: 'end' });
  }

  // 页面从离线切回时若未登录则不重连逻辑，简单起见保持常驻重连
  connect();
})();
