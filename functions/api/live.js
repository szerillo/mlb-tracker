// Cloudflare Pages Function: GET /api/live?tickers=T1,T2,...   (add &debug=1 to see Kalshi's raw status)
// Live Kalshi bid/ask for the Sharp Props tab's flagged tickers (server-side, so no browser CORS issues).
// Returns {ts, markets:{ticker:{bid, ask, last, status, volume}}, errors:[...]}. Read-only; places nothing.
const KALSHI = 'https://api.elections.kalshi.com/trade-api/v2/markets';
const HDRS = { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; bartolo-sharp-props/1.0)' };

function put(out, m) {
  out[m.ticker] = {
    bid: parseFloat(m.yes_bid_dollars || 0), ask: parseFloat(m.yes_ask_dollars || 0),
    last: parseFloat(m.last_price_dollars || 0), status: m.status, volume: parseFloat(m.volume_fp || 0),
  };
}

export async function onRequestGet({ request }) {
  const url = new URL(request.url);
  const debug = url.searchParams.get('debug') === '1';
  const tickers = [...new Set((url.searchParams.get('tickers') || '').split(',').map(s => s.trim()).filter(Boolean))].slice(0, 300);
  const out = {}, errors = [];
  for (let i = 0; i < tickers.length; i += 100) {
    const chunk = tickers.slice(i, i + 100);
    // raw commas (tickers are A-Z, 0-9, '-', '.', so nothing needs escaping)
    const u = `${KALSHI}?limit=1000&tickers=${chunk.join(',')}`;
    try {
      const r = await fetch(u, { headers: HDRS });
      if (!r.ok) { errors.push({ batch: i / 100, status: r.status, body: debug ? (await r.text()).slice(0, 300) : undefined }); continue; }
      const d = await r.json();
      for (const m of d.markets || []) put(out, m);
      if (debug) errors.push({ batch: i / 100, status: r.status, returned: (d.markets || []).length });
    } catch (e) { errors.push({ batch: i / 100, error: String(e) }); }
  }
  // fallback: anything still missing, one market at a time (max 25)
  const missing = tickers.filter(t => !out[t]).slice(0, 25);
  await Promise.all(missing.map(async t => {
    try {
      const r = await fetch(`${KALSHI}/${t}`, { headers: HDRS });
      if (!r.ok) { if (debug) errors.push({ ticker: t, status: r.status }); return; }
      const d = await r.json(); if (d.market) put(out, d.market);
    } catch (e) { if (debug) errors.push({ ticker: t, error: String(e) }); }
  }));
  return new Response(JSON.stringify({ ts: new Date().toISOString(), markets: out, errors }), {
    headers: { 'content-type': 'application/json', 'cache-control': 'public, max-age=15', 'access-control-allow-origin': '*' },
  });
}
