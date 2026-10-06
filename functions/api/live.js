// Cloudflare Pages Function: GET /api/live?tickers=T1,T2,...   (add &debug=1 to see Kalshi's raw status)
// Live Kalshi bid/ask for the Sharp Props tab's flagged tickers. Read-only; places nothing.
// Kalshi rate-limits Cloudflare's shared IPs (HTTP 429), so: retry with backoff, then a per-ticker fallback,
// and cache each answer at the edge for 30 s so repeat page loads don't hit Kalshi again.
const KALSHI = 'https://api.elections.kalshi.com/trade-api/v2/markets';
const HDRS = { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; bartolo-sharp-props/1.0)' };
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getJSON(u, errors, tag, debug) {
  for (const wait of [0, 400, 1000, 2000]) {
    if (wait) await sleep(wait);
    try {
      const r = await fetch(u, { headers: HDRS });
      if (r.ok) return await r.json();
      if (r.status !== 429 && r.status < 500) { errors.push({ ...tag, status: r.status, body: debug ? (await r.text()).slice(0, 200) : undefined }); return null; }
      if (debug) errors.push({ ...tag, status: r.status, retry_after_ms: wait });
    } catch (e) { if (debug) errors.push({ ...tag, error: String(e) }); }
  }
  errors.push({ ...tag, status: 'gave up after retries' });
  return null;
}
function put(out, m) {
  out[m.ticker] = {
    bid: parseFloat(m.yes_bid_dollars || 0), ask: parseFloat(m.yes_ask_dollars || 0),
    last: parseFloat(m.last_price_dollars || 0), status: m.status, volume: parseFloat(m.volume_fp || 0),
  };
}

export async function onRequestGet(ctx) {
  const { request } = ctx;
  const url = new URL(request.url);
  const debug = url.searchParams.get('debug') === '1';
  const tickers = [...new Set((url.searchParams.get('tickers') || '').split(',').map(s => s.trim()).filter(Boolean))].sort().slice(0, 300);
  const cache = caches.default;
  const key = new Request('https://bartolo-live-cache/' + encodeURIComponent(tickers.join(',')));
  if (!debug) { const hit = await cache.match(key); if (hit) return hit; }

  const out = {}, errors = [];
  for (let i = 0; i < tickers.length; i += 100) {
    const chunk = tickers.slice(i, i + 100);
    const d = await getJSON(`${KALSHI}?limit=1000&tickers=${chunk.join(',')}`, errors, { batch: i / 100 }, debug);
    for (const m of (d && d.markets) || []) put(out, m);
  }
  const missing = tickers.filter(t => !out[t]).slice(0, 25);   // fallback, one market at a time
  for (const t of missing) {
    const d = await getJSON(`${KALSHI}/${t}`, errors, { ticker: t }, debug);
    if (d && d.market) put(out, d.market);
  }
  const resp = new Response(JSON.stringify({ ts: new Date().toISOString(), markets: out, missing: tickers.filter(t => !out[t]).length, errors }), {
    headers: { 'content-type': 'application/json', 'cache-control': 'public, max-age=30', 'access-control-allow-origin': '*' },
  });
  if (!debug && Object.keys(out).length) ctx.waitUntil(cache.put(key, resp.clone()));
  return resp;
}
