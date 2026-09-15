// capture.js — MAIN world 抓取脚本（临时诊断用）
// hook 页面自身的 fetch，记录发往 /backend-api/f/conversation 的真实请求（headers + body 前 60KB）。
// 由 ISOLATED world 的 relay.js 通过 postMessage 读取。

(() => {
  if (window.__codexCaptureInstalled) return;
  window.__codexCaptureInstalled = true;
  window.__codexCapture = [];

  const origFetch = window.fetch.bind(window);
  window.fetch = async (input, init) => {
    try {
      const url = typeof input === 'string' ? input : (input && input.url) || '';
      const method = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
      if (method === 'POST' && /backend-api\/f\/conversation/.test(url)) {
        const headers = {};
        try {
          const h = (init && init.headers) || (input && input.headers);
          if (h) {
            if (typeof h.forEach === 'function') h.forEach((v, k) => (headers[k] = v));
            else Object.assign(headers, h);
          }
        } catch {}
        let body = null;
        try {
          const b = init && init.body;
          if (typeof b === 'string') body = b.slice(0, 60000);
          else if (b) body = '[non-string body: ' + (b.constructor && b.constructor.name) + ']';
        } catch {}
        window.__codexCapture.push({ t: Date.now(), url, method, headers, body });
        if (window.__codexCapture.length > 20) window.__codexCapture.shift();
      }
    } catch {}
    return origFetch(input, init);
  };

  window.addEventListener('message', ev => {
    if (ev.source !== window) return;
    const d = ev.data;
    if (d && d.__codexRelayCmd === 'get_capture') {
      window.postMessage({ __codexCaptureResult: window.__codexCapture }, '*');
    }
    if (d && d.__codexRelayCmd === 'probe_sentinel') {
      const r = {};
      try {
        r.keys = Object.keys(window).filter(k => /sentinel|turnstile|oai|conduit|challenge/i.test(k));
        r.hasSo = typeof window.__oai_so_web;
        if (window.__oai_so_web) {
          try { r.soKeys = Object.keys(window.__oai_so_web).slice(0, 60); } catch {}
        }
        for (const cand of ['__oai_sentinel', 'sentinel', '__sentinel', 'turnstile']) {
          try { if (window[cand]) r['found_' + cand] = typeof window[cand]; } catch {}
        }
      } catch (e) { r.error = String(e); }
      window.postMessage({ __codexProbeResult: r }, '*');
    }
    // do_fetch：用页面当前（可能被 app 包装过的）window.fetch 发请求，
    // 让 app 的 sentinel 注入逻辑自动补齐风控头。
    if (d && d.__codexRelayCmd === 'do_fetch') {
      doFetch(d.req);
    }
  });

  async function doFetch(req) {
    const rid = req.id;
    try {
      const res = await window.fetch(req.url, {
        method: req.method || 'POST',
        headers: req.headers || {},
        body: req.body || undefined,
        credentials: 'include',
      });
      window.postMessage({ __codexFetchResult: { id: rid, status: res.status, headers: Object.fromEntries(res.headers.entries()) } }, '*');
      if (res.body && req.stream) {
        const reader = res.body.getReader();
        const dec = new TextDecoder();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          const text = dec.decode(value, { stream: true });
          if (text) window.postMessage({ __codexFetchChunk: { id: rid, text } }, '*');
        }
      } else {
        const t = await res.text();
        window.postMessage({ __codexFetchChunk: { id: rid, text: t } }, '*');
      }
      window.postMessage({ __codexFetchEnd: { id: rid } }, '*');
    } catch (e) {
      window.postMessage({ __codexFetchErr: { id: rid, message: String((e && e.message) || e) } }, '*');
    }
  }
})();
