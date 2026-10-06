// Cloudflare Pages Function: GET /api/live?tickers=T1,T2,...
// Live Kalshi bid/ask for the Sharp Props tab's flagged tickers (server-side, so no browser CORS issues).
// Returns {ts, markets:{ticker:{bid, ask, last, status, volume}}}. Read-only; places nothing.
export async function onRequestGet({ request }) {
  const url = new URL(request.url);
  const tickers = [...new Set((url.searchParams.get('tickers') || '').split(',').map(s => s.trim()).filter(Boolean))].slice(0, 300);
  const out = {};
  for (let i = 0; i < tickers.length; i += 100) {
    const chunk = tickers.slice(i, i + 100);
    try {
      const r = await fetch('https://api.elections.kalshi.com/trade-api/v2/markets?limit=1000&tickers=' + encodeURIComponent(chunk.join(',')),
                            { headers: { 'User-Agent': 'bartolo-sharp-props' }, cf: { cacheTtl: 15 } });
      if (!r.ok) continue;
      const d = await r.json();
      for (const m of d.markets || []) {
        out[m.ticker] = {
          bid: parseFloat(m.yes_bid_dollars || 0), ask: parseFloat(m.yes_ask_dollars || 0),
          last: parseFloat(m.last_price_dollars || 0), status: m.status, volume: parseFloat(m.volume_fp || 0),
        };
      }
    } catch (e) { /* skip chunk */ }
  }
  return new Response(JSON.stringify({ ts: new Date().toISOString(), markets: out }), {
    headers: { 'content-type': 'application/json', 'cache-control': 'public, max-age=15', 'access-control-allow-origin': '*' },
  });
}
