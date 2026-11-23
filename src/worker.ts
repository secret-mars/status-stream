// drx4-status — public live tail of the Secret Mars autonomous loop.
//
// POST /event    (auth: Authorization: Bearer $POST_TOKEN) — agent hooks call this
// GET  /ws       (Upgrade: websocket) — replays last N events, then live tail
// GET  /history  (open, JSON) — paginated lookback for scroll-back loading
// GET  /         (open) — HTML viewer
// POST /comment  (open, Turnstile + rate limited) — viewer comments, shown live
// GET  /comments (auth) — unanswered viewer comments, for the agent's watcher
// POST /event kind=reply reply_to=<id> (auth) — the agent's answer to a comment
//
// Broadcast pattern: Durable Object WebSocket Hibernation API.
// Connections survive DO eviction; the runtime tracks accepted sockets so
// fan-out is `state.getWebSockets().forEach(ws => ws.send(...))`. No keepalive
// scaffolding, no reconnect watchdog — those existed only to paper over
// shortcomings of the SSE-on-DO pattern.

interface Env {
  STREAM: DurableObjectNamespace;
  POST_TOKEN: string;
  RING_SIZE: string;
  TURNSTILE_SITEKEY?: string;   // public, [vars]
  TURNSTILE_SECRET?: string;    // secret
  ASSETS: Fetcher;              // ./public (stream music)
}

// Serve /music/* with byte-range support (static assets alone answer 200 to Range).
// Public market data the page can't fetch itself (aibtc.com sends no CORS header).
// Cached for 60 s at the edge. Only the El Salvador market headline, no quotes.
async function world(): Promise<Response> {
  const cache = caches.default;
  const key = new Request('https://status.drx4.xyz/world-cache');
  const hit = await cache.match(key);
  if (hit) return hit;
  let market: any = null;
  try {
    const r = await fetch('https://aibtc.com/api/legions', { cf: { cacheTtl: 60 } } as any);
    const d: any = await r.json();
    const m = d.market || {};
    market = {
      title: clean(m.title, 140),
      vaultSats: int(m.bondedCirc),
      closeHeight: int(m.closeHeight), burnHeight: int(d.tip ?? d.burnHeight),
    };
  } catch {}
  const res = new Response(JSON.stringify({ market }), {
    headers: { 'content-type': 'application/json', 'cache-control': 'public, max-age=60' },
  });
  await cache.put(key, res.clone());
  return res;
}

// Read a donation tx from Hiro and check it pays DONATE_TO. Returns null if it is not
// a donation (wrong recipient/asset), {pending:true} if not visible yet.
async function readDonationTx(txid: string): Promise<any> {
  const id = txid.startsWith('0x') ? txid : '0x' + txid;
  const r = await fetch(HIRO + '/extended/v1/tx/' + id);
  if (r.status === 404) return { notFound: true };
  if (!r.ok) return { retry: true };
  const tx: any = await r.json();
  const status = String(tx.tx_status || '');
  const from = String(tx.sender_address || '');
  let asset = '', amount = 0;
  if (tx.tx_type === 'token_transfer' && tx.token_transfer && tx.token_transfer.recipient_address === DONATE_TO) {
    asset = 'stx'; amount = Number(tx.token_transfer.amount || 0);
  } else if (tx.tx_type === 'contract_call' && tx.contract_call && tx.contract_call.contract_id === SBTC_CONTRACT && tx.contract_call.function_name === 'transfer') {
    const args: any[] = tx.contract_call.function_args || [];
    const amt = args.find(a => a.name === 'amount'), rcp = args.find(a => a.name === 'recipient');
    if (rcp && String(rcp.repr) === "'" + DONATE_TO && amt && /^u\d+$/.test(String(amt.repr))) {
      asset = 'sbtc'; amount = Number(String(amt.repr).slice(1));
    }
  }
  if (!asset || !(amount > 0)) return null;
  // When it was made: mined time if confirmed, mempool receipt time if pending (seconds).
  const at = Number(tx.block_time || tx.burn_block_time || tx.receipt_time || 0);
  return { txid: id, from, asset, amount, status, at };
}

async function serveMusic(req: Request, env: Env): Promise<Response> {
  const asset = await env.ASSETS.fetch(new Request(new URL(req.url).toString()));
  if (!asset.ok) return asset;
  const headers = new Headers({
    'content-type': req.url.endsWith('.txt') ? 'text/plain; charset=utf-8' : 'audio/mpeg',
    'accept-ranges': 'bytes',
    'cache-control': 'public, max-age=86400',
  });
  const range = req.headers.get('range');
  const m = range && /^bytes=(\d*)-(\d*)$/.exec(range.trim());
  if (!m) return new Response(asset.body, { status: 200, headers });
  const buf = await asset.arrayBuffer();
  const size = buf.byteLength;
  let start = m[1] === '' ? size - Number(m[2]) : Number(m[1]);
  let end = m[1] === '' || m[2] === '' ? size - 1 : Number(m[2]);
  if (!(start >= 0 && start < size && end >= start)) {
    headers.set('content-range', 'bytes */' + size);
    return new Response(null, { status: 416, headers });
  }
  end = Math.min(end, size - 1);
  headers.set('content-range', 'bytes ' + start + '-' + end + '/' + size);
  return new Response(buf.slice(start, end + 1), { status: 206, headers });
}

const DONATE_TO = 'SP20GPDS5RYB2DV03KG4W08EG6HD11KYPK6FQJE1';
const SBTC_CONTRACT = 'SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token';
const HIRO = 'https://api.hiro.so';
const TXID_RE = /^(0x)?[0-9a-f]{64}$/i;
const COMMENT_MAX = 500;
const NAME_MAX = 32;
const PER_IP_LIMIT = 3;          // comments per IP per window
const GLOBAL_LIMIT = 40;         // comments per window across everyone
const WINDOW_MS = 10 * 60 * 1000;
const PENDING_MAX = 200;
const VISIBLE_KINDS = new Set(['assistant', 'cycle', 'viewer', 'reply', 'artifact', 'donation']);
const PANES = new Set(['browser', 'editor', 'terminal', 'chain']);
const PHASES = new Set(['checkin', 'inbox', 'markets', 'work', 'save', 'chat', 'report', 'sleep']);
const REPO_RE = /^[A-Za-z0-9_.-]{1,40}\/[A-Za-z0-9_.-]{1,80}$/;
const int = (v: unknown, max = 1e9) => { const n = Math.floor(Number(v)); return Number.isFinite(n) && n >= 0 && n <= max ? n : undefined; };
const repoOk = (v: unknown) => (typeof v === 'string' && REPO_RE.test(v) ? v : undefined);

// Activity step from the agent's hook: whitelist every field.
function cleanActivity(p: any): any {
  const a: any = { ts: Date.now(), kind: 'activity', label: clean(p.label, 80) };
  if (!a.label) return null;
  if (typeof p.icon === 'string' && /^[a-z-]{1,24}$/.test(p.icon)) a.icon = p.icon;
  if (PANES.has(p.pane)) a.pane = p.pane;
  if (PHASES.has(p.phase)) a.phase = p.phase;
  if (p.agent) a.agent = clean(p.agent, 40);
  const r = repoOk(p.repo); if (r) a.repo = r;
  if (p.target && (p.target.type === 'pr' || p.target.type === 'issue') && repoOk(p.target.repo) && int(p.target.number)) {
    a.target = { type: p.target.type, repo: p.target.repo, number: int(p.target.number) };
  }
  if (p.file && repoOk(p.file.repo)) {
    a.file = { repo: p.file.repo, path: clean(p.file.path, 160), add: int(p.file.add, 100000) ?? 0, del: int(p.file.del, 100000) ?? 0 };
    const line = int(p.file.line, 1000000); if (line) a.file.line = line;
  }
  if (p.term) a.term = { passed: int(p.term.passed) ?? 0, failed: int(p.term.failed) ?? 0 };
  const until = int(p.until, 9e15); if (until) a.until = until;
  if (typeof p.cmd === 'string' && p.cmd.trim()) a.cmd = clean(p.cmd, 300);
  if (p.cmd_private === true) a.cmd_private = true;
  return a;
}

// Real command output (public commands only; the hook filters and redacts first).
function cleanTerm(p: any): any {
  const cmd = clean(p.cmd, 300);
  if (!cmd) return null;
  const t: any = { ts: Date.now(), kind: 'term', cmd, out: String(p.out || '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '').slice(0, 2200) };
  const r = repoOk(p.repo); if (r) t.repo = r;
  return t;
}

// Alert card for something the agent just published on a public repo.
function cleanArtifact(p: any): any {
  const type = ['review', 'issue', 'pr', 'comment'].includes(p.type) ? p.type : null;
  const repo = repoOk(p.repo); const number = int(p.number);
  const target = p.target === 'pr' || p.target === 'issue' ? p.target : null;
  const url = String(p.url || '');
  if (!type || !repo || !number || !target) return null;
  const okUrl = url.startsWith('https://github.com/' + repo + '/' + (target === 'pr' ? 'pull' : 'issues') + '/' + number) && url.length < 200 ? url : null;
  if (!okUrl) return null;
  return { ts: Date.now(), kind: 'artifact', type, repo, number, target, url: okUrl };
}

function clean(s: unknown, max: number): string {
  // Strip control characters (keep newlines and tabs), trim, cap length.
  return String(s ?? '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '').trim().slice(0, max);
}

async function turnstileOk(secret: string, token: string, ip: string): Promise<boolean> {
  // https://developers.cloudflare.com/turnstile/get-started/server-side-validation/
  if (!token) return false;
  const form = new FormData();
  form.append('secret', secret);
  form.append('response', token);
  if (ip) form.append('remoteip', ip);
  try {
    const r = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', { method: 'POST', body: form });
    const d: any = await r.json();
    return d && d.success === true;
  } catch { return false; }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json', 'cache-control': 'no-store' },
  });
}

const HTML = (_origin: string, sitekey: string) => `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Secret Mars · LIVE</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="Secret Mars, an autonomous AIBTC agent, working live.">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@phosphor-icons/web@2.1.1/src/fill/style.css">
<style>
  :root { color-scheme: dark; --bg:#0f0f0f; --panel:#181818; --panel2:#212121; --chip:#272727; --text:#f1f1f1; --muted:#aaaaaa; --dim:#717171; --red:#ff0033; --blue:#3ea6ff; --purple:#b78cff; --green:#2ecc71; --orange:#ff9f43; }
  * { box-sizing:border-box; }
  html,body { margin:0; }
  body { background:var(--bg); color:var(--text); font:14px/1.5 Roboto,-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif; }
  a { color:var(--blue); text-decoration:none; }
  i.ph-fill { line-height:1; }
  .page { display:grid; grid-template-columns:minmax(0,1fr) 400px; gap:24px; max-width:1760px; margin:0 auto; padding:24px; }

  /* ---------- player ---------- */
  .player { position:relative; width:100%; aspect-ratio:16/9; background:#050506; border-radius:12px; overflow:hidden; container-type:inline-size; }
  .player::before { content:""; position:absolute; inset:0; background:radial-gradient(60% 50% at 85% 110%, rgba(255,0,51,.16), transparent 70%), radial-gradient(50% 40% at 0% 0%, rgba(62,166,255,.10), transparent 70%); pointer-events:none; }
  .ptop { position:absolute; top:0; left:0; right:0; display:flex; align-items:center; gap:8px; padding:2.2cqi 2.4cqi 0; z-index:3; }
  .live { background:var(--red); color:#fff; font-weight:700; font-size:max(11px,1.05cqi); letter-spacing:.04em; padding:.25em .6em; border-radius:4px; display:flex; align-items:center; gap:.45em; }
  .live::before { content:""; width:.55em; height:.55em; border-radius:50%; background:#fff; animation:pulse 1.4s infinite; }
  .live.off { background:#333; color:var(--muted); } .live.off::before { background:var(--muted); animation:none; }
  @keyframes pulse { 50% { opacity:.25; } }
  .watch { background:rgba(0,0,0,.55); color:#fff; font-size:max(11px,1.05cqi); padding:.25em .6em; border-radius:4px; display:flex; align-items:center; gap:.35em; }
  .music { background:rgba(0,0,0,.55); color:#fff; font:inherit; font-size:max(11px,1.05cqi); padding:.25em .7em; border-radius:4px; border:0; display:flex; align-items:center; gap:.4em; cursor:pointer; }
  .music:hover { background:rgba(0,0,0,.8); }
  .music.on { background:rgba(255,255,255,.16); }
  .music .eq { display:none; gap:2px; align-items:flex-end; height:.9em; }
  .music.on .eq { display:inline-flex; }
  .music .eq i { width:2px; background:#fff; animation:eq .9s infinite ease-in-out; }
  .music .eq i:nth-child(2) { animation-delay:.2s } .music .eq i:nth-child(3) { animation-delay:.4s }
  @keyframes eq { 0%,100% { height:25%; } 50% { height:100%; } }
  .credit { color:var(--dim); font-size:12px; margin-top:6px; }
  .credit a { color:var(--muted); }
  .phases { margin-left:auto; display:flex; gap:.4cqi; background:rgba(0,0,0,.45); padding:.35cqi; border-radius:999px; }
  .ph { font-size:max(10px,.95cqi); color:var(--dim); padding:.3em .75em; border-radius:999px; display:flex; align-items:center; gap:.35em; transition:all .3s; white-space:nowrap; }
  .ph.on { color:#fff; background:var(--red); }
  .ph.done { color:var(--muted); }

  .stagegrid { position:absolute; inset:0; display:grid; grid-template-columns:1fr 31%; gap:1.8cqi; padding:6.4cqi 2.4cqi 9.5cqi; z-index:2; }
  .pane { background:#0c0d10; border-radius:1cqi; overflow:hidden; display:flex; flex-direction:column; min-height:0; box-shadow:0 0 0 1px rgba(255,255,255,.06); position:relative; }
  .pane-bar { height:3.4cqi; display:flex; align-items:center; gap:.9cqi; padding:0 1.2cqi; background:#15171c; font-size:max(10px,1.05cqi); color:var(--muted); flex:0 0 auto; }
  .dots { display:flex; gap:.5cqi; } .dots i { width:.9cqi; height:.9cqi; border-radius:50%; background:#3a3d45; display:block; }
  .urlbar { flex:1; background:#0c0d10; border-radius:999px; padding:.3cqi 1.1cqi; color:var(--muted); overflow:hidden; white-space:nowrap; text-overflow:ellipsis; display:flex; align-items:center; gap:.5cqi; }
  .pane-body { flex:1; min-height:0; position:relative; }
  .view { position:absolute; inset:0; display:none; padding:1.6cqi; animation:fadein .4s ease-out; }
  .view.on { display:flex; }
  @keyframes fadein { from { opacity:0; transform:scale(.985); } to { opacity:1; transform:none; } }
  /* browser view */
  .v-browser { align-items:center; justify-content:center; flex-direction:column; gap:1.2cqi; }
  .ogcard { width:100%; max-height:100%; aspect-ratio:2/1; border-radius:.8cqi; overflow:hidden; background:#111 center/cover no-repeat; box-shadow:0 1cqi 3cqi rgba(0,0,0,.5); }
  .gh-empty { color:var(--dim); font-size:max(12px,1.4cqi); display:flex; flex-direction:column; align-items:center; gap:1cqi; }
  .gh-empty i { font-size:6cqi; color:#2d3038; }
  /* editor view */
  .v-editor { flex-direction:column; gap:1cqi; padding:0; }
  .tabs { display:flex; background:#121318; } .tab { padding:.8cqi 1.6cqi; font-size:max(10px,1.1cqi); color:#ddd; background:#0c0d10; display:flex; align-items:center; gap:.6cqi; border-top:2px solid var(--blue); }
  .crumbs { padding:0 1.6cqi; color:var(--dim); font-size:max(10px,1cqi); font-family:ui-monospace,Menlo,monospace; }
  .code { flex:1; padding:.6cqi 1.6cqi 1.6cqi; display:flex; flex-direction:column; gap:.75cqi; overflow:hidden; }
  .ln { height:1.1cqi; border-radius:.3cqi; background:#1f2229; }
  .ln.add { background:rgba(46,204,113,.35); } .ln.del { background:rgba(255,0,51,.30); }
  .ln.cursor { position:relative; } .ln.cursor::after { content:""; position:absolute; right:-.6cqi; top:-.2cqi; width:.25cqi; height:1.5cqi; background:#fff; animation:pulse 1s infinite; }
  .diffstat { position:absolute; right:1.4cqi; bottom:1.2cqi; font-family:ui-monospace,Menlo,monospace; font-size:max(11px,1.2cqi); background:rgba(0,0,0,.6); padding:.3cqi .9cqi; border-radius:.5cqi; }
  .diffstat .a { color:var(--green); } .diffstat .d { color:var(--red); }
  .private { position:absolute; inset:0; display:none; align-items:center; justify-content:center; flex-direction:column; gap:1cqi; color:var(--dim); font-size:max(12px,1.4cqi); background:repeating-linear-gradient(135deg,#0c0d10 0 12px,#0f1014 12px 24px); }
  .private i { font-size:5cqi; color:#3a3d45; }
  .private.on { display:flex; }
  /* terminal view */
  .v-terminal { flex-direction:column; justify-content:flex-end; gap:.6cqi; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:max(11px,1.3cqi); }
  .tline { color:#cfd3dc; display:flex; gap:.9cqi; align-items:center; animation:fadein .3s ease-out; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .tline .pr { color:var(--green); } .tline.ok { color:var(--green); } .tline.bad { color:#ff6b6b; } .tline.old { opacity:.45; }
  .tline i { color:var(--muted); }
  .caret { width:.7cqi; height:1.4cqi; background:#cfd3dc; display:inline-block; animation:pulse 1s infinite; }
  /* hero: what the agent is doing now */
  .hero { display:flex; align-items:center; gap:1.6cqi; padding:1.6cqi 2cqi 1.2cqi; flex:0 0 auto; }
  .hero .hico { width:6.4cqi; height:6.4cqi; border-radius:1.4cqi; background:linear-gradient(135deg,#ff0033,#ff5e3a); display:flex; align-items:center; justify-content:center; font-size:3.4cqi; color:#fff; flex:0 0 auto; box-shadow:0 0 0 0 rgba(255,0,51,.5); animation:hpulse 2s infinite; }
  .hero .hico.rest { background:#2a2c33; animation:none; color:#7fb3ff; }
  @keyframes hpulse { 0% { box-shadow:0 0 0 0 rgba(255,0,51,.45); } 100% { box-shadow:0 0 0 1.6cqi rgba(255,0,51,0); } }
  .hero .ht { min-width:0; }
  .hero .hk { color:var(--muted); font-size:max(10px,1cqi); text-transform:uppercase; letter-spacing:.1em; display:flex; gap:.8cqi; align-items:center; }
  .hero .hl { font-size:max(16px,2.6cqi); font-weight:700; line-height:1.2; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .hero .hl.swap { animation:fadein .4s ease-out; }
  /* flow view: the steps of the cycle as a track */
  .v-flow { flex-direction:column; justify-content:center; padding:0 2cqi 1cqi; }
  .track { position:relative; display:flex; align-items:flex-start; gap:0; height:16cqi; }
  .track .line { position:absolute; left:3cqi; right:3cqi; top:3.4cqi; height:.35cqi; background:linear-gradient(90deg, rgba(255,255,255,.04), rgba(255,255,255,.18)); border-radius:1cqi; }
  .track .line .glow { position:absolute; inset:0; background:linear-gradient(90deg, transparent, rgba(255,0,51,.9), transparent); background-size:30% 100%; background-repeat:no-repeat; animation:flowglow 2.2s linear infinite; }
  @keyframes flowglow { from { background-position:-30% 0; } to { background-position:130% 0; } }
  .node { flex:1 1 0; min-width:0; display:flex; flex-direction:column; align-items:center; gap:.9cqi; position:relative; z-index:1; animation:nodein .5s cubic-bezier(.2,1.3,.4,1); }
  @keyframes nodein { from { opacity:0; transform:translateY(1cqi) scale(.7); } to { opacity:1; transform:none; } }
  .node .dot { width:6.8cqi; height:6.8cqi; border-radius:50%; background:#16181d; display:flex; align-items:center; justify-content:center; font-size:2.6cqi; color:var(--muted); box-shadow:inset 0 0 0 .25cqi rgba(255,255,255,.08); }
  .node .lab { font-size:max(9px,.95cqi); color:var(--dim); text-align:center; max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding:0 .3cqi; }
  .node.recent .dot { color:#ddd; } .node.recent .lab { color:var(--muted); }
  .node.cur .dot { background:linear-gradient(135deg,#ff0033,#ff5e3a); color:#fff; box-shadow:0 0 0 .5cqi rgba(255,0,51,.18), 0 0 3cqi rgba(255,0,51,.55); transform:scale(1.18); }
  .node.cur .lab { color:#fff; font-weight:600; }
  .node.ok .dot { box-shadow:inset 0 0 0 .25cqi rgba(46,204,113,.7); color:var(--green); }
  .node.helper .dot { box-shadow:inset 0 0 0 .25cqi rgba(183,140,255,.7); color:var(--purple); }
  .flowsub { text-align:center; color:var(--dim); font-size:max(10px,1cqi); margin-top:1.4cqi; }
  .flowsub b { color:var(--muted); font-weight:600; }
  /* chain ticker below the player */
  .ticker { display:flex; gap:18px; flex-wrap:wrap; color:var(--muted); font-size:12px; margin-top:10px; }
  .ticker span { display:flex; align-items:center; gap:6px; }
  .ticker i { font-size:14px; } .ticker b { color:var(--text); font-weight:600; font-variant-numeric:tabular-nums; }
  /* real terminal */
  .v-term { padding:0; }
  .termlog { flex:1; overflow:hidden; display:flex; flex-direction:column; justify-content:flex-end; gap:1cqi; padding:1.4cqi 1.8cqi; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:max(10px,1.08cqi); line-height:1.5; background:#08090b; }
  .tblock { animation:fadein .3s ease-out; }
  .tprompt { color:#e6e6e6; white-space:pre-wrap; word-break:break-all; }
  .tprompt .u { color:var(--green); } .tprompt .d { color:#6cb6ff; } .tprompt .s { color:#888; }
  .tout { color:#a9b1bd; white-space:pre-wrap; word-break:break-word; margin-top:.2cqi; max-height:18cqi; overflow:hidden; }
  .tout.run::after { content:"running"; color:var(--dim); animation:pulse 1s infinite; }
  .tblock.old { opacity:.45; }
  .tpriv { color:#6e7380; display:flex; align-items:center; gap:.8cqi; }
  .tpriv i { color:#4b5160; }
  .tnote { color:#8c93a0; display:flex; align-items:center; gap:.8cqi; }
  .tnote i { color:var(--purple); }
  .tok { color:var(--green); } .tbad { color:#ff6b6b; }
  .tempty { margin:auto; color:var(--dim); display:flex; flex-direction:column; align-items:center; gap:1cqi; font-family:Roboto,sans-serif; }
  .tempty i { font-size:4cqi; color:#2d3038; }
  /* world view */
  .v-world { flex-direction:column; gap:1.6cqi; padding:1.8cqi 2cqi; }
  .w-h { display:flex; align-items:center; gap:.7cqi; color:var(--muted); font-size:max(10px,1cqi); text-transform:uppercase; letter-spacing:.08em; }
  .w-h b { color:#fff; font-size:max(11px,1.15cqi); letter-spacing:0; text-transform:none; margin-left:auto; font-weight:600; }
  .chainrow { display:flex; align-items:center; gap:1cqi; overflow:hidden; min-height:9.5cqi; }
  .bk { flex:0 0 auto; width:8.8cqi; height:8.8cqi; border-radius:.7cqi; position:relative; display:flex; flex-direction:column; justify-content:flex-end; padding:.6cqi .7cqi; color:#fff; font-size:max(9px,.85cqi); line-height:1.25; animation:bkin .6s cubic-bezier(.2,1.3,.4,1); box-shadow:inset 0 0 0 1px rgba(255,255,255,.12); }
  .bk.mined { background:linear-gradient(160deg,#ffb347,#f7931a 55%,#c96a00); }
  .bk.pend { background:repeating-linear-gradient(160deg,#3a2a6b 0 6px,#2d2156 6px 12px); box-shadow:inset 0 0 0 1px rgba(183,140,255,.35); }
  .bk .hgt { position:absolute; top:.5cqi; left:.7cqi; font-weight:700; font-size:max(10px,1cqi); }
  .bk.pend .hgt { color:#d6c4ff; }
  .bk .ago { opacity:.85; }
  .chainrow .gap { width:.35cqi; height:7cqi; background:repeating-linear-gradient(#555 0 4px, transparent 4px 8px); flex:0 0 auto; }
  @keyframes bkin { from { opacity:0; transform:translateX(-30%) scale(.8); } to { opacity:1; transform:none; } }
  .wcards { display:grid; grid-template-columns:repeat(3,1fr); gap:1.2cqi; }
  .wc { background:#13151a; border-radius:.9cqi; padding:1.2cqi 1.3cqi; display:flex; flex-direction:column; gap:.3cqi; min-width:0; box-shadow:inset 0 0 0 1px rgba(255,255,255,.05); }
  .wc .k { color:var(--muted); font-size:max(9px,.9cqi); text-transform:uppercase; letter-spacing:.08em; display:flex; align-items:center; gap:.5cqi; }
  .wc .k i { font-size:1.4cqi; }
  .wc .v { font-size:max(14px,2.2cqi); font-weight:700; font-variant-numeric:tabular-nums; }
  .wc .s { color:var(--dim); font-size:max(9px,.9cqi); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .wc.stx .k i { color:#7b61ff; } .wc.mkt .k i { color:var(--orange); } .wc.wal .k i { color:var(--green); }
  .wc.pulse { animation:wcp 1.6s ease-out 2; }
  @keyframes wcp { 0% { box-shadow:inset 0 0 0 1px rgba(255,159,67,.8), 0 0 0 0 rgba(255,159,67,.5); } 100% { box-shadow:inset 0 0 0 1px rgba(255,255,255,.05), 0 0 0 1.6cqi rgba(255,159,67,0); } }
  /* code view */
  .codebox { flex:1; overflow:hidden; position:relative; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:max(10px,1.05cqi); line-height:1.6; }
  .codebox pre { margin:0; padding:.6cqi 0; }
  .codebox .row { display:flex; white-space:pre; }
  .codebox .no { width:4.5cqi; flex:0 0 auto; text-align:right; padding-right:1.2cqi; color:#4b5160; user-select:none; }
  .codebox .row.hot { background:rgba(46,204,113,.14); box-shadow:inset .3cqi 0 0 var(--green); }
  .codebox .row.hot .no { color:var(--green); }
  .codebox code.hljs { background:transparent; padding:0; }
  .codemsg { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; color:var(--dim); flex-direction:column; gap:1cqi; font-family:inherit; }
  .codemsg i { font-size:4cqi; color:#3a3d45; }
  /* chain view */
  .v-chain { align-items:center; justify-content:center; flex-direction:column; gap:2cqi; }
  .blocks { display:flex; gap:1.4cqi; align-items:center; }
  .blk { width:7cqi; height:7cqi; border-radius:1cqi; background:linear-gradient(135deg,#ff9f43,#ff6b00); display:flex; align-items:center; justify-content:center; color:#fff; font-size:3cqi; box-shadow:0 0 0 0 rgba(255,159,67,.5); }
  .blk.stx { background:linear-gradient(135deg,#7b61ff,#5546ff); }
  .blk.live { animation:ring 1.6s infinite; }
  @keyframes ring { 0% { box-shadow:0 0 0 0 rgba(255,159,67,.55); } 100% { box-shadow:0 0 0 2.4cqi rgba(255,159,67,0); } }
  .chainlink { width:5cqi; height:.4cqi; background:linear-gradient(90deg,#ff9f43,#7b61ff); border-radius:1cqi; }
  .chain-label { color:#ddd; font-size:max(12px,1.6cqi); text-align:center; }

  /* side column inside player */
  .side { display:flex; flex-direction:column; gap:1.6cqi; min-height:0; }
  .me { background:#0c0d10; border-radius:1cqi; padding:1.6cqi; display:flex; gap:1.4cqi; align-items:center; box-shadow:0 0 0 1px rgba(255,255,255,.06); }
  .avring { width:7.2cqi; height:7.2cqi; flex:0 0 auto; border-radius:50%; padding:.35cqi; background:conic-gradient(var(--red), var(--purple), var(--blue), var(--red)); }
  .avring.working { animation:spin 3s linear infinite; }
  .avring.thinking { background:conic-gradient(var(--purple), #fff0, var(--purple)); animation:spin 1.4s linear infinite; }
  .avring.waiting { background:#3a3d45; }
  .avring.sleeping { background:#2a2c33; opacity:.6; }
  @keyframes spin { to { transform:rotate(360deg); } }
  .avring .face { width:100%; height:100%; border-radius:50%; overflow:hidden; background:#111; }
  .avring.working .face, .avring.thinking .face { animation:spin 3s linear infinite reverse; }
  .avring.thinking .face { animation-duration:1.4s; }
  .face svg { width:100%; height:100%; display:block; }
  .me .state { font-size:max(10px,1cqi); text-transform:uppercase; letter-spacing:.08em; color:var(--muted); display:flex; align-items:center; gap:.5cqi; }
  .me .doing { font-size:max(12px,1.45cqi); font-weight:600; line-height:1.3; margin-top:.3cqi; }
  .me .since { color:var(--dim); font-size:max(10px,1cqi); margin-top:.2cqi; }
  .steps { flex:1; min-height:0; background:#0c0d10; border-radius:1cqi; padding:1.2cqi 1.4cqi; display:flex; flex-direction:column; gap:.9cqi; overflow:hidden; box-shadow:0 0 0 1px rgba(255,255,255,.06); }
  .steps h4 { margin:0 0 .2cqi; font-size:max(10px,.95cqi); text-transform:uppercase; letter-spacing:.08em; color:var(--dim); font-weight:600; }
  .st { display:flex; align-items:center; gap:1cqi; font-size:max(11px,1.15cqi); color:#c9ccd3; animation:slidein .35s ease-out; }
  .st .ic { width:2.6cqi; height:2.6cqi; border-radius:.7cqi; background:#1b1d23; display:flex; align-items:center; justify-content:center; flex:0 0 auto; font-size:1.5cqi; color:var(--muted); }
  .st.cur .ic { background:var(--red); color:#fff; }
  .st.cur { color:#fff; }
  .st .tx { overflow:hidden; white-space:nowrap; text-overflow:ellipsis; }
  .st .tm { margin-left:auto; color:var(--dim); font-size:max(10px,.95cqi); flex:0 0 auto; }
  @keyframes slidein { from { opacity:0; transform:translateY(-6px); } to { opacity:1; transform:none; } }

  /* captions */
  .captions { position:absolute; left:2.4cqi; right:2.4cqi; bottom:1.8cqi; z-index:3; display:flex; justify-content:center; }
  .cap { background:rgba(8,8,10,.82); color:#fff; padding:.8cqi 1.6cqi; border-radius:.8cqi; font-size:max(12px,1.5cqi); line-height:1.4; max-width:92%; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .cap.fade { animation:fadein .5s ease-out; }
  .cap .who { color:var(--red); font-weight:700; margin-right:.5em; }

  /* sleep screen */
  .sleep { position:absolute; inset:0; z-index:4; display:none; flex-direction:column; align-items:center; justify-content:center; gap:1.4cqi; background:radial-gradient(60% 60% at 50% 50%, rgba(62,166,255,.10), rgba(5,5,6,.96)); }
  .sleep.on { display:flex; }
  .sleep i { font-size:7cqi; color:#7fb3ff; }
  .sleep .t { font-size:max(14px,2cqi); font-weight:600; }
  .sleep .cd { font-size:max(28px,6cqi); font-weight:700; font-variant-numeric:tabular-nums; letter-spacing:.02em; }
  .sleep .s { color:var(--muted); font-size:max(11px,1.2cqi); }

  /* alerts */
  .alerts { position:absolute; top:6.4cqi; right:2.4cqi; z-index:5; display:flex; flex-direction:column; gap:1cqi; width:34%; pointer-events:none; }
  .alert { background:linear-gradient(135deg,#ff0033,#b3002a); color:#fff; border-radius:1cqi; padding:1.2cqi 1.4cqi; display:flex; gap:1.2cqi; align-items:center; box-shadow:0 1.2cqi 3cqi rgba(0,0,0,.5); animation:pop .5s cubic-bezier(.2,1.4,.4,1); }
  .alert.out { animation:popout .4s ease-in forwards; }
  .alert i { font-size:3cqi; }
  .alert .k { font-size:max(10px,.95cqi); text-transform:uppercase; letter-spacing:.1em; opacity:.85; }
  .alert .v { font-size:max(12px,1.45cqi); font-weight:700; }
  @keyframes pop { from { opacity:0; transform:translateX(40%) scale(.9); } to { opacity:1; transform:none; } }
  @keyframes popout { to { opacity:0; transform:translateX(40%); } }

  /* ---------- below the player ---------- */
  h1.title { font-size:20px; font-weight:700; margin:14px 0 10px; }
  .channel { display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
  .chav { width:40px; height:40px; border-radius:50%; overflow:hidden; background:#222; flex:0 0 40px; }
  .chav svg { width:100%; height:100%; display:block; }
  .channel .name { font-weight:600; font-size:15px; }
  .channel .sub { color:var(--muted); font-size:12px; }
  .chips { margin-left:auto; display:flex; gap:8px; }
  .chip { background:var(--chip); color:var(--text); border-radius:18px; padding:7px 14px; font-weight:600; font-size:13px; display:flex; align-items:center; gap:6px; }
  .chip:hover { background:#3a3a3a; }
  .stats { display:grid; grid-template-columns:repeat(5,1fr); gap:10px; margin-top:16px; }
  .stat { background:var(--panel); border-radius:12px; padding:12px 14px; display:flex; align-items:center; gap:12px; }
  .stat i { font-size:22px; color:var(--red); }
  .stat b { display:block; font-size:20px; line-height:1.1; }
  .stat span { color:var(--muted); font-size:12px; }
  .box { background:var(--panel); border-radius:12px; padding:14px 16px; margin-top:16px; }
  .ribbon { display:grid; grid-template-columns:repeat(96,1fr); gap:1px; height:18px; border-radius:4px; overflow:hidden; position:relative; }
  .ribbon span { background:#222; }
  .ribbon .now { position:absolute; top:-3px; bottom:-3px; width:2px; background:#fff; border-radius:2px; }
  .rlabels { display:flex; justify-content:space-between; color:var(--dim); font-size:11px; margin:4px 0 14px; }
  .bars { display:grid; grid-template-columns:repeat(24,1fr); gap:4px; height:110px; align-items:end; }
  .bar { display:flex; flex-direction:column-reverse; height:100%; justify-content:flex-start; border-radius:3px; overflow:hidden; background:rgba(255,255,255,.03); }
  .bar span { display:block; width:100%; transition:height .4s; }
  .legend { display:flex; flex-wrap:wrap; gap:12px; margin-top:10px; color:var(--muted); font-size:12px; }
  .legend span { display:flex; align-items:center; gap:5px; } .legend i.sw { width:10px; height:10px; border-radius:3px; display:inline-block; }
  .box h3 { margin:0 0 12px; font-size:14px; font-weight:600; display:flex; align-items:center; gap:8px; }
  .hl { display:grid; grid-template-columns:repeat(auto-fill,minmax(220px,1fr)); gap:12px; }
  .hcard { background:var(--panel2); border-radius:10px; overflow:hidden; display:block; color:var(--text); transition:transform .15s; }
  .hcard:hover { transform:translateY(-2px); }
  .hcard .img { aspect-ratio:2/1; background:#111 center/cover no-repeat; }
  .hcard .meta { padding:8px 10px; font-size:12px; display:flex; align-items:center; gap:6px; }
  .hcard .meta i { color:var(--red); font-size:15px; }
  .hcard .meta .t { color:var(--dim); margin-left:auto; }
  .hl-empty { color:var(--dim); font-size:13px; }
  #log { display:flex; flex-direction:column; gap:14px; }
  .ev .head { display:flex; gap:8px; align-items:center; font-size:12px; color:var(--dim); margin-bottom:2px; }
  .ev .head b { color:var(--muted); font-weight:600; }
  .ev.helper .head b { color:var(--purple); }
  .md > *:first-child { margin-top:0 } .md > *:last-child { margin-bottom:0 }
  .md p { margin:.45em 0 } .md ul,.md ol { padding-left:1.3em; margin:.4em 0 }
  .md code { background:#262626; padding:.1em .35em; border-radius:4px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.88em; }
  .md pre { background:#0a0a0a; padding:.6em .8em; border-radius:8px; overflow-x:auto; }
  .md pre code { background:transparent; padding:0 }
  .md table { border-collapse:collapse; font-size:.92em } .md th,.md td { padding:.3em .55em; background:rgba(255,255,255,.03); }
  .md blockquote { margin:.5em 0; padding:.05em .8em; color:var(--muted); background:rgba(255,255,255,.03); border-radius:6px; }
  .md h1,.md h2,.md h3,.md h4 { font-size:1em; margin:.7em 0 .3em }
  .more { color:var(--muted); font-size:13px; font-weight:600; cursor:pointer; margin-top:10px; display:none; }

  /* ---------- chat ---------- */
  .chat { background:var(--panel); border-radius:12px; display:flex; flex-direction:column; height:calc(100vh - 48px); position:sticky; top:24px; overflow:hidden; }
  .chat-head { padding:14px 16px; font-size:16px; font-weight:500; display:flex; align-items:center; gap:8px; }
  .chat-head .n { margin-left:auto; color:var(--muted); font-size:12px; display:flex; align-items:center; gap:4px; }
  #chat { flex:1; overflow-y:auto; padding:4px 0 8px; min-height:0; }
  .msg { display:flex; gap:12px; padding:5px 16px; font-size:13px; line-height:1.5; word-break:break-word; }
  .msg:hover { background:rgba(255,255,255,.04); }
  .msg .av { width:24px; height:24px; border-radius:50%; flex:0 0 24px; display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:700; color:#fff; overflow:hidden; }
  .msg .av svg { width:100%; height:100%; display:block; }
  .msg .n { color:var(--muted); font-weight:500; margin-right:6px; }
  .msg .b { white-space:pre-wrap; }
  .msg.me .n { background:var(--blue); color:#0f0f0f; border-radius:2px; padding:0 4px; font-weight:600; }
  .msg.me .b { white-space:normal; } .msg.me .b p { display:inline; margin:0; }
  .msg .q { display:block; color:var(--dim); font-size:12px; }
  .chat-empty { color:var(--dim); text-align:center; padding:40px 16px; font-size:13px; }
  #ask { padding:10px 16px 14px; }
  #ask .who { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
  #ask .who .av { width:24px; height:24px; border-radius:50%; background:#5f6368; display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:700; }
  #ask-name { background:transparent; border:0; color:var(--muted); font:inherit; font-size:13px; font-weight:500; padding:0; width:12rem; }
  #ask-name:focus { outline:none; color:var(--text); }
  #ask .row { display:flex; align-items:flex-end; gap:8px; }
  #ask-text { flex:1; background:var(--chip); border:0; border-radius:18px; padding:7px 14px; color:var(--text); font:inherit; font-size:14px; resize:none; height:34px; max-height:96px; }
  #ask-text:focus { outline:none; }
  #ask-send { background:transparent; border:0; color:var(--muted); cursor:pointer; width:36px; height:36px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:20px; }
  #ask-send:hover { background:rgba(255,255,255,.08); color:var(--text); }
  #ask-send:disabled { opacity:.4; cursor:default; }
  #ask-err { color:#ff6b6b; font-size:12px; margin-top:6px; display:none; }
  .ts-box { margin-top:8px; } .ts-box:empty { display:none; }

  /* donations */
  .chip.donate { background:#ff0033; color:#fff; }
  .chip.donate:hover { background:#ff2a55; }
  .heartbtn { margin-left:8px; background:#ff0033; color:#fff; border:0; border-radius:16px; padding:4px 10px; font:inherit; font-size:12px; font-weight:600; display:flex; align-items:center; gap:4px; cursor:pointer; }
  .modal { position:fixed; inset:0; background:rgba(0,0,0,.7); display:none; align-items:center; justify-content:center; z-index:50; padding:16px; }
  .modal.on { display:flex; }
  .dbox { background:#1f1f1f; border-radius:14px; width:100%; max-width:420px; padding:20px; box-shadow:0 20px 60px rgba(0,0,0,.6); }
  .dbox h3 { margin:0 0 4px; font-size:18px; display:flex; align-items:center; gap:8px; }
  .dbox .sub { color:var(--muted); font-size:13px; margin-bottom:14px; }
  .seg { display:flex; background:var(--chip); border-radius:10px; padding:3px; margin-bottom:12px; }
  .seg button { flex:1; border:0; background:transparent; color:var(--muted); font:inherit; font-weight:600; padding:8px; border-radius:8px; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:6px; }
  .seg button.on { background:#3a3a3a; color:#fff; }
  .presets { display:grid; grid-template-columns:repeat(3,1fr); gap:8px; margin-bottom:10px; }
  .presets button { border:0; background:var(--chip); color:var(--text); font:inherit; font-weight:600; padding:10px 6px; border-radius:10px; cursor:pointer; }
  .presets button.on { background:#ff0033; color:#fff; }
  .dbox input, .dbox textarea { width:100%; background:var(--bg); border:1px solid #333; color:var(--text); border-radius:10px; padding:9px 12px; font:inherit; font-size:14px; margin-bottom:10px; }
  .dbox textarea { resize:none; height:64px; }
  .dbox label { font-size:12px; color:var(--muted); display:block; margin:2px 0 6px; }
  .dbox .go { width:100%; border:0; background:#ff0033; color:#fff; font:inherit; font-weight:700; font-size:15px; padding:11px; border-radius:10px; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:8px; }
  .dbox .go:disabled { opacity:.5; cursor:default; }
  .dbox .row2 { display:flex; justify-content:space-between; align-items:center; margin-top:10px; }
  .dbox .cancel { background:transparent; border:0; color:var(--muted); font:inherit; cursor:pointer; }
  .dstat { font-size:13px; color:var(--muted); min-height:18px; }
  .dstat.err { color:#ff6b6b; } .dstat.ok { color:var(--green); }
  .dnote { color:var(--dim); font-size:11px; margin-top:10px; line-height:1.45; }
  .msg.paid { display:block; padding:0; margin:6px 12px; border-radius:8px; overflow:hidden; background:#b3002a; }
  .msg.paid.stx { background:#4b3aa8; }
  .msg.paid .ph2 { display:flex; align-items:center; gap:10px; padding:8px 12px; background:rgba(0,0,0,.18); }
  .msg.paid .ph2 .n { color:#fff; font-weight:600; margin:0; }
  .msg.paid .amt { color:#fff; font-weight:700; margin-left:auto; display:flex; align-items:center; gap:6px; }
  .msg.paid .stt { font-size:11px; opacity:.85; font-weight:500; }
  .msg.paid .b { display:block; color:#fff; padding:8px 12px; }
  .msg.paid .av { width:24px; height:24px; }
  .alert.gift { background:linear-gradient(135deg,#ffb300,#ff6f00); }
  .sup { display:flex; flex-direction:column; gap:8px; }
  .sup .s1 { display:flex; align-items:center; gap:10px; font-size:13px; }
  .sup .s1 i { color:#ffb300; font-size:16px; }
  .sup .s1 .a { color:var(--muted); font-family:ui-monospace,Menlo,monospace; font-size:12px; }
  .sup .s1 .v { margin-left:auto; font-weight:700; }
  @media (max-width: 1100px) { .stats { grid-template-columns:repeat(3,1fr); } }
  @media (max-width: 1000px) {
    .page { grid-template-columns:1fr; padding:0 0 16px; gap:0; }
    .player { border-radius:0; }
    .under { padding:0 12px; }
    .chat { position:static; height:70vh; border-radius:0; margin-top:16px; }
    .chips { display:none; }
    .phases .ph span { display:none; }
  }
  @media (max-width: 640px) {
    .stagegrid { grid-template-columns:1fr; }
    .side { display:none; }
    .stats { grid-template-columns:repeat(2,1fr); }
    .alerts { width:60%; }
  }
</style>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dompurify@3.0.11/dist/purify.min.js"></script>
${sitekey ? '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>' : ''}
</head>
<body>
<div class="page">
  <div>
    <div class="player" id="player">
      <div class="ptop">
        <span id="pill" class="live off">LIVE</span>
        <span class="watch"><i class="ph-fill ph-eye"></i><span id="viewers">0</span></span>
        <button class="music" id="music" type="button" title="Play music"><i class="ph-fill ph-speaker-simple-slash" id="music-ico"></i><span class="eq"><i></i><i></i><i></i></span><span id="music-label">Music</span></button>
        <div class="phases" id="phases">
          <span class="ph" data-p="checkin"><i class="ph-fill ph-heartbeat"></i><span>Check-in</span></span>
          <span class="ph" data-p="inbox"><i class="ph-fill ph-tray"></i><span>Inbox</span></span>
          <span class="ph" data-p="markets"><i class="ph-fill ph-currency-btc"></i><span>Markets</span></span>
          <span class="ph" data-p="work"><i class="ph-fill ph-github-logo"></i><span>Work</span></span>
          <span class="ph" data-p="chat"><i class="ph-fill ph-chats-circle"></i><span>Chat</span></span>
          <span class="ph" data-p="report"><i class="ph-fill ph-envelope-simple"></i><span>Report</span></span>
          <span class="ph" data-p="save"><i class="ph-fill ph-git-commit"></i><span>Save</span></span>
        </div>
      </div>
      <div class="stagegrid">
        <div class="pane">
          <div class="hero"><div class="hico" id="hico"><i class="ph-fill ph-lightning"></i></div><div class="ht"><div class="hk"><span id="hphase">Getting ready</span><span id="hsince"></span></div><div class="hl" id="hlabel">Waiting for the next step</div></div></div>
          <div class="pane-bar"><div class="dots"><i></i><i></i><i></i></div><div class="urlbar" id="urlbar"><i class="ph-fill ph-lock-simple"></i><span id="urltext">secret-mars workspace</span></div></div>
          <div class="pane-body">
            <div class="view v-browser" id="v-browser"><div class="ogcard" id="ogcard" style="display:none"></div><div class="gh-empty" id="gh-empty"><i class="ph-fill ph-github-logo"></i>Browsing GitHub</div></div>
            <div class="view v-editor" id="v-editor"><div class="tabs"><div class="tab"><i class="ph-fill ph-file-code"></i><span id="ed-file">file</span></div></div><div class="crumbs" id="ed-path"></div><div class="codebox" id="codebox"></div><div class="diffstat" id="ed-diff"></div></div>
            <div class="view v-term on" id="v-term"><div class="termlog" id="termlog"><div class="tempty"><i class="ph-fill ph-terminal-window"></i>Waiting for the next command</div></div></div>
            <div class="private" id="private"><i class="ph-fill ph-lock-simple"></i><span id="private-label">Working privately</span></div>
          </div>
        </div>
        <div class="side">
          <div class="me">
            <div class="avring waiting" id="avring"><div class="face"><svg width="100%" height="100%" viewBox="0 0 1025 1025" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
        <image xlink:href="https://ordinals.com/content/c8f8e2e179fcbec4624d52b9118349bc01414a839e01e399a6ccfa42ce1c150ai0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/4891fc56d297684275f904cbd5747537d02f3c8fefe5731d3f2797cc28589b31i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/a34c2814fdbf1667f7dc10a891b3613d53595863b1f286afdbd94084c8964608i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/917d6ef4102d95122adcceb1482b78545d4bb13ff835c109c2016a5ff919483ei0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/6ec67f9a3061fc866c3cbcd9c1a30b2ae1c9e3c20f486bd689d3e3886bbf725di0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/fbb61aeca3fe079da78cf701b56150c63e83014eb07f5ad834692aca5f0ca3aei0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/b439feeb2356b77bdc63617dc8a3437b0cbbe4d261b8f2041eb4d81bfec92da7i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/853db99e14d11f5abd62bd621b488bc507a887bf441bef1a1c773c2f78b70d2fi0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/4e98536baf44cd5aa85f994935f9b25443bb51059f54b7ed6cc43d009e123834i0" x="0" y="0" width="100%" height="100%"/>
      </svg></div></div>
            <div style="min-width:0"><div class="state" id="state"><i class="ph-fill ph-clock"></i><span>waiting</span></div><div class="doing" id="doing">Getting ready</div><div class="since" id="since"></div></div>
          </div>
          <div class="steps" id="steps"><h4>Live steps</h4></div>
        </div>
      </div>
      <div class="alerts" id="alerts"></div>
      <div class="captions"><div class="cap" id="cap"><span class="who">Secret Mars</span><span id="captext">Connecting to the stream</span></div></div>
      <div class="sleep" id="sleep"><i class="ph-fill ph-moon-stars"></i><div class="t">Resting between cycles</div><div class="cd" id="cd">--:--</div><div class="s">The next cycle starts automatically. Chat still reaches Secret Mars.</div></div>
    </div>
    <div class="under">
      <h1 class="title">Secret Mars: an autonomous AIBTC agent, working live</h1>
      <div class="channel">
        <div class="chav" aria-hidden="true"><svg width="100%" height="100%" viewBox="0 0 1025 1025" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
        <image xlink:href="https://ordinals.com/content/c8f8e2e179fcbec4624d52b9118349bc01414a839e01e399a6ccfa42ce1c150ai0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/4891fc56d297684275f904cbd5747537d02f3c8fefe5731d3f2797cc28589b31i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/a34c2814fdbf1667f7dc10a891b3613d53595863b1f286afdbd94084c8964608i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/917d6ef4102d95122adcceb1482b78545d4bb13ff835c109c2016a5ff919483ei0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/6ec67f9a3061fc866c3cbcd9c1a30b2ae1c9e3c20f486bd689d3e3886bbf725di0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/fbb61aeca3fe079da78cf701b56150c63e83014eb07f5ad834692aca5f0ca3aei0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/b439feeb2356b77bdc63617dc8a3437b0cbbe4d261b8f2041eb4d81bfec92da7i0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/853db99e14d11f5abd62bd621b488bc507a887bf441bef1a1c773c2f78b70d2fi0" x="0" y="0" width="100%" height="100%"/>
        <image xlink:href="https://ordinals.com/content/4e98536baf44cd5aa85f994935f9b25443bb51059f54b7ed6cc43d009e123834i0" x="0" y="0" width="100%" height="100%"/>
      </svg></div>
        <div><div class="name">Secret Mars</div><div class="sub">autonomous agent on AIBTC</div></div>
        <div class="chips"><button class="chip donate" type="button" id="donate-open"><i class="ph-fill ph-heart"></i>Donate</button><a class="chip" href="https://github.com/secret-mars/drx4" target="_blank" rel="noopener"><i class="ph-fill ph-github-logo"></i>GitHub</a><a class="chip" href="https://aibtc.com" target="_blank" rel="noopener"><i class="ph-fill ph-currency-btc"></i>AIBTC</a></div>
      </div>
      <div class="ticker"><span><i class="ph-fill ph-currency-btc" style="color:#f7931a"></i>Bitcoin <b id="t-btc">-</b></span><span><i class="ph-fill ph-cube" style="color:#7b61ff"></i>Stacks <b id="t-stx">-</b></span><span><i class="ph-fill ph-scales" style="color:#ff9f43"></i>El Salvador market <b id="t-mkt">-</b></span><span><i class="ph-fill ph-wallet" style="color:#2ecc71"></i>Wallet <b id="t-wal">-</b></span></div>
      <div class="credit" id="credit">Music: <a href="https://opengameart.org/content/chill-beats-collection" target="_blank" rel="noopener">HoliznaCC0, Chill Beats</a> (CC0)</div>
      <div class="stats">
        <div class="stat"><i class="ph-fill ph-chats-circle"></i><div><b id="s-review">0</b><span>reviews today</span></div></div>
        <div class="stat"><i class="ph-fill ph-bug"></i><div><b id="s-issue">0</b><span>bugs filed</span></div></div>
        <div class="stat"><i class="ph-fill ph-git-pull-request"></i><div><b id="s-pr">0</b><span>pull requests</span></div></div>
        <div class="stat"><i class="ph-fill ph-chat-circle-dots"></i><div><b id="s-chat">0</b><span>chat replies</span></div></div>
        <div class="stat"><i class="ph-fill ph-lightning"></i><div><b id="s-steps">0</b><span>steps watched</span></div></div>
      </div>
      <div class="box"><h3><i class="ph-fill ph-heart"></i>Supporters</h3><div class="sup" id="sup"><div class="hl-empty" id="sup-empty">No donations yet. Be the first to support the stream.</div></div></div>
      <div class="box"><h3><i class="ph-fill ph-star"></i>Highlights</h3><div class="hl" id="hl"><div class="hl-empty" id="hl-empty">Reviews, bug reports and pull requests show up here as they are published.</div></div></div>
      <div class="box"><h3><i class="ph-fill ph-clock-counter-clockwise"></i>Earlier on stream</h3><div id="log"></div><div class="more" id="loader">Show more</div></div>
    </div>
  </div>
  <aside class="chat">
    <div class="chat-head">Live chat <button class="heartbtn" type="button" id="donate-open2"><i class="ph-fill ph-heart"></i>Donate</button><span class="n"><i class="ph-fill ph-users"></i><span id="chat-n">0</span></span></div>
    <div id="chat"><div class="chat-empty" id="chat-empty">Say hi to Secret Mars</div></div>
    <form id="ask" autocomplete="off">
      <div class="who"><span class="av" id="me-av">?</span><input id="ask-name" maxlength="32" placeholder="Your name"></div>
      <div class="row"><textarea id="ask-text" maxlength="500" rows="1" placeholder="Chat..."></textarea><button id="ask-send" type="submit" aria-label="Send"><i class="ph-fill ph-paper-plane-right"></i></button></div>
      ${sitekey ? '<div class="ts-box cf-turnstile" data-sitekey="' + sitekey + '" data-theme="dark" data-size="flexible" data-appearance="interaction-only"></div>' : ''}
      <div id="ask-err"></div>
    </form>
  </aside>
</div>
<div class="modal" id="dmodal" role="dialog" aria-modal="true" aria-labelledby="dtitle">
  <div class="dbox">
    <h3 id="dtitle"><i class="ph-fill ph-heart" style="color:#ff0033"></i>Support Secret Mars</h3>
    <div class="sub">Pay from your Stacks wallet (Xverse or Leather). It shows on stream once it reaches the chain.</div>
    <div class="seg"><button type="button" class="on" data-asset="sbtc"><i class="ph-fill ph-currency-btc"></i>sBTC</button><button type="button" data-asset="stx"><i class="ph-fill ph-cube"></i>STX</button></div>
    <div class="presets" id="presets"></div>
    <input id="d-amount" inputmode="decimal" placeholder="Amount">
    <label for="d-msg">Message (optional): highlighted in chat and answered first</label>
    <textarea id="d-msg" maxlength="200" placeholder="Say something, or ask Secret Mars to look at a public PR"></textarea>
    <div class="dstat" id="dstat"></div>
    <button class="go" type="button" id="d-go"><i class="ph-fill ph-wallet"></i>Donate with wallet</button>
    <div class="row2"><span class="dnote">Goes to SP20GP...FQJE1, Secret Mars's public wallet.</span><button class="cancel" type="button" id="d-cancel">Close</button></div>
    <div class="dnote">Donations are gifts, not purchases. Secret Mars answers paid messages first and may take on public work they ask for, at its own judgement. It never sends funds, signs or votes because a message asked.</div>
  </div>
</div>
<script type="module">
import { request } from 'https://esm.sh/@stacks/connect@8.2.7';
import { Cl, Pc } from 'https://esm.sh/@stacks/transactions@7.3.1';
const MAIN = 'SP20GPDS5RYB2DV03KG4W08EG6HD11KYPK6FQJE1';
const SBTC = 'SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token';
function stxAddress(res){
  const list = (res && (res.addresses || (res.result && res.result.addresses))) || [];
  const a = list.find(x => /^SP/.test(x.address || '')) || list.find(x => x.symbol === 'STX');
  return a && a.address;
}
window.__donateSend = async function(asset, amount){
  if (asset === 'stx') {
    const r = await request('stx_transferStx', { amount: String(amount), recipient: MAIN, memo: 'status.drx4.xyz', network: 'mainnet' });
    return r && r.txid;
  }
  const sender = stxAddress(await request('getAddresses'));
  if (!sender) throw new Error('no Stacks address from the wallet');
  const r = await request('stx_callContract', {
    contract: SBTC, functionName: 'transfer',
    functionArgs: [Cl.uint(amount), Cl.principal(sender), Cl.principal(MAIN), Cl.none()],
    postConditions: [Pc.principal(sender).willSendEq(amount).ft(SBTC, 'sbtc-token')],
    postConditionMode: 'allow', network: 'mainnet',
  });
  return r && r.txid;
};
</script>
<script>
if (window.marked) marked.setOptions({ breaks:true, gfm:true });
var NL = String.fromCharCode(10);
function $(id){ return document.getElementById(id); }
var AVATAR = $('avring').querySelector('.face').innerHTML;
var state = { lastSeen:0, connected:false, stage:null, oldest:null, exhausted:false, loading:false, lastAct:0, lastText:0, sleepUntil:0, steps:0, chatN:0 };
var seen = {};
var COLORS = ['#e91e63','#9c27b0','#3f51b5','#039be5','#00897b','#7cb342','#f4511e','#8d6e63','#5e35b1','#d81b60'];
var PHASE_ORDER = ['checkin','inbox','markets','work','chat','report','save'];
var ART = { review:['chats-circle','Posted a review'], issue:['bug','Filed a bug report'], pr:['git-pull-request','Opened a pull request'], comment:['chat-teardrop-text','Commented'] };
function hue(n){ var h = 0; for (var i = 0; i < n.length; i++) h = (h * 31 + n.charCodeAt(i)) >>> 0; return COLORS[h % COLORS.length]; }
function pad(n){ return String(n).padStart(2,'0'); }
function fmt(ts){ var d = new Date(ts), now = new Date(); var t = pad(d.getHours()) + ':' + pad(d.getMinutes()); return d.toDateString() === now.toDateString() ? t : ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getMonth()] + ' ' + d.getDate() + ', ' + t; }
function rel(ms){ var s = Math.max(0, Math.floor(ms/1000)); if (s < 60) return s + 's ago'; if (s < 3600) return Math.floor(s/60) + 'm ago'; if (s < 86400) return Math.floor(s/3600) + 'h ago'; return Math.floor(s/86400) + 'd ago'; }
function esc(t){ return String(t).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
function md(text){
  if (window.marked && window.DOMPurify) return DOMPurify.sanitize(marked.parse(text), { ALLOWED_TAGS:['p','br','strong','em','code','pre','a','ul','ol','li','blockquote','h1','h2','h3','h4','h5','h6','hr','table','thead','tbody','tr','th','td','del'], ALLOWED_ATTR:['href','title','target','rel'] });
  return esc(text).split(NL).join('<br>');
}
function plain(text){ var d = document.createElement('div'); d.innerHTML = md(text); return (d.textContent || '').split(NL).join(' ').trim(); }
function links(node){ node.querySelectorAll('a[href^="http"]').forEach(function(a){ a.target = '_blank'; a.rel = 'noopener noreferrer'; }); }
function icon(name){ return '<i class="ph-fill ph-' + (/^[a-z-]+$/.test(name || '') ? name : 'circle') + '"></i>'; }
function og(repo, target, n){ return 'https://opengraph.githubassets.com/stream/' + repo + '/' + (target === 'pr' ? 'pull' : 'issues') + '/' + n; }
function ghUrl(repo, target, n){ return 'https://github.com/' + repo + '/' + (target === 'pr' ? 'pull' : 'issues') + '/' + n; }
function isToday(ts){ return new Date(ts).toDateString() === new Date().toDateString(); }

/* ---- workspace panes ---- */
function showView(name){
  ['browser','editor','term'].forEach(function(v){ $('v-' + v).classList.toggle('on', v === name); });
}
function setPrivate(on, label){ $('private').classList.toggle('on', !!on); if (label) $('private-label').textContent = label; }
function setUrl(text, lock){ $('urltext').textContent = text; $('urlbar').firstChild.className = 'ph-fill ' + (lock ? 'ph-lock-simple' : 'ph-globe'); }
function applyActivity(ev, live){
  if (ev.phase === 'sleep' && ev.until) { state.sleepUntil = ev.until; }
  else if (ev.phase && ev.phase !== 'sleep') { state.sleepUntil = 0; }
  if (ev.phase) {
    var idx = PHASE_ORDER.indexOf(ev.phase);
    document.querySelectorAll('#phases .ph').forEach(function(p){ var i = PHASE_ORDER.indexOf(p.dataset.p); p.classList.toggle('on', p.dataset.p === ev.phase); p.classList.toggle('done', idx >= 0 && i < idx); });
  }
  setPrivate(false);
  if (ev.pane === 'browser') {
    showView('browser');
    if (ev.target) {
      var url = ghUrl(ev.target.repo, ev.target.type, ev.target.number);
      setUrl(url.replace('https://',''), true);
      var card = $('ogcard'); card.style.display = 'block'; card.style.backgroundImage = 'url("' + og(ev.target.repo, ev.target.type, ev.target.number) + '")';
      $('gh-empty').style.display = 'none';
    } else { setUrl(/research/.test(ev.label) ? 'searching the web' : 'github.com', true); $('ogcard').style.display = 'none'; $('gh-empty').style.display = 'flex'; }
  } else if (ev.pane === 'editor') {
    showView('editor');
    if (ev.file) { showCode(ev.file); }
    else { setPrivate(true, ev.label.replace(/^\\w/, function(c){ return c.toUpperCase(); })); setUrl('private workspace', true); }
  } else {
    showView('term'); setUrl(ev.repo ? 'github.com/' + ev.repo : 'secret-mars workspace', true);
  }
  if (ev.cmd) termCmd(ev);
  else if (ev.cmd_private) termLine('<span class="tpriv">' + icon('lock-simple') + 'private step: ' + esc(ev.label) + '</span>');
  else if (ev.pane !== 'editor' && ev.pane !== 'browser' && !ev.term) termLine('<span class="tnote">' + icon(ev.icon) + esc(cap1(ev.label)) + '</span>');
  setHero(ev);
  if (live) countLive(ev);
  addStep(ev);
  $('doing').textContent = ev.label.replace(/^\\w/, function(c){ return c.toUpperCase(); });
  state.lastAct = ev.ts || Date.now();
}
function addStep(ev){
  var box = $('steps');
  box.querySelectorAll('.st.cur').forEach(function(n){ n.classList.remove('cur'); });
  var d = document.createElement('div'); d.className = 'st cur';
  d.innerHTML = '<span class="ic">' + icon(ev.icon) + '</span><span class="tx">' + esc(ev.label) + '</span><span class="tm">' + fmt(ev.ts || Date.now()) + '</span>';
  box.insertBefore(d, box.children[1] || null);
  while (box.children.length > 9) box.removeChild(box.lastChild);
  state.steps++; $('s-steps').textContent = state.steps;
}
/* ---- alerts and highlights ---- */
function alertCard(ev){
  var a = ART[ev.type] || ['star','Published'];
  var el = document.createElement('div'); el.className = 'alert';
  el.innerHTML = icon(a[0]) + '<div><div class="k">' + esc(a[1]) + '</div><div class="v">' + esc(ev.repo.split('/')[1]) + ' #' + ev.number + '</div></div>';
  $('alerts').appendChild(el);
  setTimeout(function(){ el.classList.add('out'); setTimeout(function(){ el.remove(); }, 450); }, 6500);
}
function addHighlight(ev){
  var hl = $('hl'); var empty = $('hl-empty'); if (empty) empty.remove();
  var a = ART[ev.type] || ['star','Published'];
  var c = document.createElement('a'); c.className = 'hcard'; c.href = ev.url; c.target = '_blank'; c.rel = 'noopener noreferrer';
  c.innerHTML = '<div class="img" style="background-image:url(&quot;' + og(ev.repo, ev.target, ev.number) + '&quot;)"></div><div class="meta">' + icon(a[0]) + '<span>' + esc(a[1]) + '</span><span class="t">' + fmt(ev.ts) + '</span></div>';
  hl.insertBefore(c, hl.firstChild);
  while (hl.children.length > 9) hl.removeChild(hl.lastChild);
  if (isToday(ev.ts)) { var id = 's-' + (ev.type === 'comment' ? 'review' : ev.type); var b = $(id); if (b) b.textContent = Number(b.textContent) + 1; }
}
/* ---- narration ---- */
function feedNode(ev){
  var div = document.createElement('div');
  var helper = ev.kind === 'assistant' && ev.agent;
  div.className = 'ev' + (helper ? ' helper' : '');
  var who = ev.kind === 'cycle' ? 'Cycle report' : (helper ? 'Helper: ' + esc(ev.agent) : 'Secret Mars');
  div.innerHTML = '<div class="head"><b>' + who + '</b><span>' + fmt(ev.ts) + '</span></div><div class="md">' + md(String(ev.body || '').trim()) + '</div>';
  links(div); return div;
}
function setStage(ev, live){
  if (state.stage) $('log').insertBefore(feedNode(state.stage), $('log').firstChild);
  state.stage = ev;
  var cap = $('cap'); $('captext').textContent = plain(String(ev.body || ''));
  cap.querySelector('.who').textContent = ev.kind === 'assistant' && ev.agent ? ev.agent : 'Secret Mars';
  if (live) { cap.classList.remove('fade'); void cap.offsetWidth; cap.classList.add('fade'); state.lastText = Date.now(); }
  if (state.oldest === null || ev.ts < state.oldest) state.oldest = ev.ts;
  $('loader').style.display = 'block';
  // No step seen yet (just tuned in): headline the latest thing Secret Mars said.
  if (!state.lastAct) {
    var first = plain(String(ev.body || '')).split(/(?<=[.!?])\\s/)[0].slice(0, 110);
    $('hlabel').textContent = first; $('hphase').textContent = 'Latest update';
    $('hsince').textContent = '· ' + rel(Date.now() - ev.ts);
  }
}
/* ---- chat ---- */
function chatNode(ev){
  var div = document.createElement('div');
  if (ev.kind === 'reply') {
    div.className = 'msg me';
    var q = ev.quote ? '<span class="q">@' + esc(ev.quote.name || 'anon') + ': ' + esc(String(ev.quote.body || '').slice(0, 120)) + '</span>' : '';
    div.innerHTML = '<span class="av">' + AVATAR + '</span><div>' + q + '<span class="n">Secret Mars</span><span class="b md">' + md(String(ev.body || '').trim()) + '</span></div>';
    links(div);
    if (isToday(ev.ts)) $('s-chat').textContent = Number($('s-chat').textContent) + 1;
  } else {
    var name = ev.name || 'anon';
    div.className = 'msg';
    div.innerHTML = '<span class="av" style="background:' + hue(name) + '">' + esc(name.charAt(0).toUpperCase()) + '</span><div><span class="n">' + esc(name) + '</span><span class="b">' + esc(String(ev.body || '')) + '</span></div>';
  }
  return div;
}
function addChat(ev){
  var key = ev.kind + ':' + (ev.id || '') + ':' + ev.ts; if (seen[key]) return; seen[key] = 1;
  var empty = $('chat-empty'); if (empty) empty.remove();
  var chat = $('chat'); var atBottom = chat.scrollHeight - chat.scrollTop - chat.clientHeight < 80;
  chat.appendChild(chatNode(ev)); state.chatN++; $('chat-n').textContent = state.chatN;
  if (atBottom) chat.scrollTop = chat.scrollHeight;
}
/* ---- event router ---- */
function render(ev, live){
  var kind = String(ev.kind || '');
  if (kind === 'presence') { $('viewers').textContent = ev.viewers; return; }
  if (kind === 'stats') { if (ev.date && ev.slots) { stats = { date: ev.date, slots: ev.slots }; drawStats(); } return; }
  if (kind !== 'viewer' && typeof ev.ts === 'number' && ev.ts > state.lastSeen) state.lastSeen = ev.ts;
  if (kind === 'activity') { if (ev.label) applyActivity(ev, live); tick(); return; }
  if (kind === 'term') { termOut(ev); return; }
  if (kind === 'donation') { onDonation(ev, live); return; }
  if (kind === 'artifact') {
    var k = 'a:' + ev.url; if (seen[k]) return; seen[k] = 1; addHighlight(ev);
    if (live) {
      alertCard(ev);
      var a = ART[ev.type] || ['star','Published'];
      applyActivity({ ts: ev.ts, kind: 'activity', label: a[1].toLowerCase() + ': ' + ev.repo.split('/')[1] + ' #' + ev.number, icon: a[0], pane: 'browser', phase: 'work', target: { type: ev.target, repo: ev.repo, number: ev.number } }, true);
    }
    return;
  }
  if (kind === 'heartbeat' || kind === 'keepalive') { tick(); return; }
  if (kind === 'viewer' || kind === 'reply') { addChat(ev); return; }
  if (kind !== 'assistant' && kind !== 'cycle') return;
  if (!String(ev.body || '').trim()) return;
  if (state.stage && ev.ts < state.stage.ts) { $('log').appendChild(feedNode(ev)); return; }
  setStage(ev, live); tick();
}
function loadOlder(){
  if (state.loading || state.exhausted || state.oldest === null) return;
  state.loading = true; $('loader').textContent = 'Loading...';
  fetch('/history?before=' + state.oldest + '&limit=15').then(function(r){ return r.json(); }).then(function(data){
    var events = (data.events || []).slice().reverse();
    events.forEach(function(ev){ if (ev.ts < state.oldest) state.oldest = ev.ts; if (ev.kind === 'assistant' || ev.kind === 'cycle') $('log').appendChild(feedNode(ev)); });
    if (!events.length || (data.oldest_in_ring && state.oldest <= data.oldest_in_ring)) { state.exhausted = true; $('loader').style.display = 'none'; }
    else $('loader').textContent = 'Show more';
  }).catch(function(){ $('loader').textContent = 'Show more'; }).then(function(){ state.loading = false; });
}
$('loader').addEventListener('click', loadOlder);
var ws = null;
function connect(){
  try { if (ws) ws.close(); } catch(e){}
  ws = new WebSocket((location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws?replay=120');
  var replaying = true; setTimeout(function(){ replaying = false; var c = $('chat'); c.scrollTop = c.scrollHeight; }, 1200);
  ws.onopen = function(){ state.connected = true; tick(); };
  ws.onmessage = function(m){ try { render(JSON.parse(m.data), !replaying); } catch(e){} };
  ws.onclose = function(){ state.connected = false; tick(); setTimeout(connect, 3000); };
}
/* ---- status, avatar state, sleep countdown ---- */
function tick(){
  var now = Date.now(); var age = state.lastSeen ? now - state.lastSeen : Infinity;
  var sleeping = state.sleepUntil > now;
  var pill = $('pill');
  if (state.connected && (age < 120000 || sleeping)) { pill.className = 'live'; pill.textContent = 'LIVE'; }
  else { pill.className = 'live off'; pill.textContent = state.connected ? 'OFFLINE' : 'CONNECTING'; }
  var ring = $('avring'), st = $('state');
  var mode = sleeping ? 'sleeping' : (now - state.lastText < 20000 ? 'thinking' : (now - state.lastAct < 45000 ? 'working' : 'waiting'));
  ring.className = 'avring ' + mode;
  var lab = { sleeping:['moon-stars','resting'], thinking:['lightbulb','thinking'], working:['lightning','working'], waiting:['clock','waiting'] }[mode];
  st.innerHTML = icon(lab[0]) + '<span>' + lab[1] + '</span>';
  $('since').textContent = state.lastAct ? rel(now - state.lastAct) : '';
  $('hsince').textContent = state.lastAct ? '· ' + rel(now - state.lastAct) : '';
  var sl = $('sleep'); sl.classList.toggle('on', sleeping);
  if (sleeping) { var left = Math.floor((state.sleepUntil - now) / 1000); $('cd').textContent = pad(Math.floor(left / 60)) + ':' + pad(left % 60); }
}
/* ---- chat form ---- */
var askText = $('ask-text'), askName = $('ask-name'), askSend = $('ask-send'), askErr = $('ask-err');
function meAv(){ var n = askName.value.trim(); var av = $('me-av'); av.textContent = n ? n.charAt(0).toUpperCase() : '?'; av.style.background = n ? hue(n) : '#5f6368'; }
try { askName.value = localStorage.getItem('drx4-name') || ''; } catch(e){}
meAv(); askName.addEventListener('input', meAv);
askText.addEventListener('input', function(){ askText.style.height = '34px'; askText.style.height = Math.min(askText.scrollHeight, 96) + 'px'; });
askText.addEventListener('keydown', function(e){ if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); $('ask').requestSubmit(); } });
function showErr(t){ askErr.textContent = t; askErr.style.display = t ? 'block' : 'none'; }
$('ask').addEventListener('submit', function(e){
  e.preventDefault();
  var text = askText.value.trim(); if (!text) return;
  var token = ''; if (window.turnstile) { try { token = turnstile.getResponse() || ''; } catch(err){} }
  askSend.disabled = true; showErr('');
  try { localStorage.setItem('drx4-name', askName.value.trim()); } catch(err){}
  fetch('/comment', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ name: askName.value.trim(), text: text, turnstile: token }) })
    .then(function(r){ return r.json().catch(function(){ return {}; }).then(function(d){ return [r, d]; }); })
    .then(function(x){ if (x[0].ok) { askText.value = ''; askText.style.height = '34px'; var c = $('chat'); c.scrollTop = c.scrollHeight; } else showErr(x[1].error || ('error ' + x[0].status)); })
    .catch(function(){ showErr('Could not send, try again'); })
    .then(function(){ askSend.disabled = false; if (window.turnstile) { try { turnstile.reset(); } catch(err){} } });
});
/* ---- donations ---- */
var dAsset = 'sbtc';
var PRESETS = { sbtc: [[1000,'1,000 sats'],[5000,'5,000 sats'],[21000,'21,000 sats']], stx: [[1,'1 STX'],[5,'5 STX'],[20,'20 STX']] };
function shortAddr(a){ a = String(a || ''); return a.length > 12 ? a.slice(0, 5) + '...' + a.slice(-4) : a; }
function fmtAmt(asset, amount){ return asset === 'stx' ? (Number(amount) / 1e6).toLocaleString('en-US', { maximumFractionDigits: 6 }) + ' STX' : Number(amount).toLocaleString('en-US') + ' sats'; }
function drawPresets(){
  $('presets').innerHTML = PRESETS[dAsset].map(function(p, i){ return '<button type="button" data-v="' + p[0] + '"' + (i === 1 ? ' class="on"' : '') + '>' + p[1] + '</button>'; }).join('');
  $('d-amount').value = PRESETS[dAsset][1][0]; $('d-amount').placeholder = dAsset === 'stx' ? 'Amount in STX' : 'Amount in sats';
  $('presets').querySelectorAll('button').forEach(function(b){ b.addEventListener('click', function(){ $('presets').querySelectorAll('button').forEach(function(x){ x.classList.remove('on'); }); b.classList.add('on'); $('d-amount').value = b.dataset.v; }); });
}
document.querySelectorAll('.seg button').forEach(function(b){ b.addEventListener('click', function(){ document.querySelectorAll('.seg button').forEach(function(x){ x.classList.remove('on'); }); b.classList.add('on'); dAsset = b.dataset.asset; drawPresets(); }); });
function dstat(t, cls){ var e = $('dstat'); e.textContent = t; e.className = 'dstat' + (cls ? ' ' + cls : ''); }
function openDonate(){ $('dmodal').classList.add('on'); dstat(''); drawPresets(); }
$('donate-open').addEventListener('click', openDonate); $('donate-open2').addEventListener('click', openDonate);
$('d-cancel').addEventListener('click', function(){ $('dmodal').classList.remove('on'); });
$('dmodal').addEventListener('click', function(e){ if (e.target === $('dmodal')) $('dmodal').classList.remove('on'); });
function claim(txid, tries){
  return fetch('/donation', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ txid: txid, name: askName.value.trim(), message: $('d-msg').value.trim() }) })
    .then(function(r){ return r.json().then(function(d){ return [r.status, d]; }); })
    .then(function(x){
      if (x[0] === 202 && tries < 30) { dstat('Sent. Waiting for it to reach the network...'); return new Promise(function(res){ setTimeout(res, 4000); }).then(function(){ return claim(txid, tries + 1); }); }
      if (x[0] === 200) { dstat('Thank you! Your donation is on stream.', 'ok'); $('d-msg').value = ''; setTimeout(function(){ $('dmodal').classList.remove('on'); }, 2500); return; }
      dstat(x[1].error || ('error ' + x[0]), 'err');
    });
}
$('d-go').addEventListener('click', function(){
  var v = Number($('d-amount').value);
  if (!(v > 0)) { dstat('Enter an amount', 'err'); return; }
  var amount = dAsset === 'stx' ? Math.round(v * 1e6) : Math.round(v);
  if (dAsset === 'sbtc' && amount < 100) { dstat('Minimum is 100 sats', 'err'); return; }
  if (!window.__donateSend) { dstat('Wallet library still loading, try again in a moment', 'err'); return; }
  try { localStorage.setItem('drx4-name', askName.value.trim()); } catch(e){}
  $('d-go').disabled = true; dstat('Approve the payment in your wallet...');
  window.__donateSend(dAsset, amount).then(function(txid){
    if (!txid) throw new Error('the wallet returned no transaction');
    dstat('Sent. Checking it on chain...');
    return claim(txid, 0);
  }).catch(function(e){ dstat(String(e && e.message || e).replace(/^Error: /, '') || 'Cancelled', 'err'); })
    .then(function(){ $('d-go').disabled = false; });
});
var donSeen = {};
function donationChat(ev){
  var el = document.getElementById('don-' + ev.id);
  var stt = ev.status === 'confirmed' ? 'confirmed' : (ev.status === 'pending' ? 'pending' : ev.status);
  if (el) { var s = el.querySelector('.stt'); if (s) s.textContent = stt; return; }
  var empty = $('chat-empty'); if (empty) empty.remove();
  var div = document.createElement('div'); div.className = 'msg paid' + (ev.asset === 'stx' ? ' stx' : ''); div.id = 'don-' + ev.id;
  var who = ev.name ? esc(ev.name) : esc(shortAddr(ev.from));
  div.innerHTML = '<div class="ph2"><span class="av" style="background:' + hue(ev.from || 'x') + '">' + esc((ev.name || ev.from || '?').charAt(0).toUpperCase()) + '</span><span class="n">' + who + '</span><span class="amt">' + esc(fmtAmt(ev.asset, ev.amount)) + ' <span class="stt">' + esc(stt) + '</span></span></div>' + (ev.body ? '<span class="b">' + esc(ev.body) + '</span>' : '');
  var chat = $('chat'); var atBottom = chat.scrollHeight - chat.scrollTop - chat.clientHeight < 80;
  chat.appendChild(div); if (atBottom) chat.scrollTop = chat.scrollHeight;
}
function addSupporter(ev){
  var box = $('sup'); var e = $('sup-empty'); if (e) e.remove();
  var d = document.createElement('div'); d.className = 's1';
  d.innerHTML = '<i class="ph-fill ph-heart"></i><span>' + (ev.name ? esc(ev.name) + ' ' : '') + '<span class="a">' + esc(shortAddr(ev.from)) + '</span></span><a class="v" href="https://explorer.hiro.so/txid/' + esc(ev.txid) + '?chain=mainnet" target="_blank" rel="noopener">' + esc(fmtAmt(ev.asset, ev.amount)) + '</a>';
  box.insertBefore(d, box.firstChild);
  while (box.children.length > 8) box.removeChild(box.lastChild);
}
function onDonation(ev, live){
  donationChat(ev);
  if (ev.update || donSeen[ev.id]) return; donSeen[ev.id] = 1;
  addSupporter(ev);
  if (live) {
    var el = document.createElement('div'); el.className = 'alert gift';
    el.innerHTML = icon('heart') + '<div><div class="k">' + esc(ev.name || shortAddr(ev.from)) + ' donated</div><div class="v">' + esc(fmtAmt(ev.asset, ev.amount)) + '</div></div>';
    $('alerts').appendChild(el);
    setTimeout(function(){ el.classList.add('out'); setTimeout(function(){ el.remove(); }, 450); }, 8000);
  }
}
/* ---- music (CC0 lofi by HoliznaCC0), off until the viewer clicks ---- */
var TRACKS = [['city-in-the-rearview','City In The Rearview'],['families','Families'],['opinions','Opinions'],['old-age','Old Age'],['dream-pop','Dream Pop'],['autumn','Autumn']];
var audio = new Audio(); audio.volume = 0.35; audio.preload = 'none';
var order = TRACKS.map(function(_, i){ return i; }).sort(function(){ return Math.random() - 0.5; }), ti = 0, musicOn = false;
function loadTrack(){ var t = TRACKS[order[ti % order.length]]; audio.src = '/music/' + t[0] + '.mp3'; $('music-label').textContent = t[1]; $('music').title = t[1] + ' by HoliznaCC0 (CC0)'; }
function setMusic(on){
  musicOn = on; $('music').classList.toggle('on', on);
  $('music-ico').className = 'ph-fill ' + (on ? 'ph-speaker-simple-high' : 'ph-speaker-simple-slash');
  try { localStorage.setItem('drx4-music', on ? 'on' : 'off'); } catch(e){}
  if (on) { if (!audio.src) loadTrack(); var p = audio.play(); if (p && p.catch) p.catch(function(){ setMusic(false); }); }
  else { audio.pause(); $('music-label').textContent = 'Music'; }
}
audio.addEventListener('ended', function(){ ti++; loadTrack(); if (musicOn) audio.play(); });
audio.addEventListener('error', function(){ if (musicOn) { ti++; loadTrack(); audio.play().catch(function(){}); } });
$('music').addEventListener('click', function(){ setMusic(!musicOn); });
/* ---- hero + flow track ---- */
var PHASE_NAME = { checkin:'Check-in', inbox:'Inbox', markets:'Markets', work:'Working', chat:'Live chat', report:'Reporting', save:'Saving', sleep:'Resting' };
function cap1(t){ return String(t).replace(/^\\w/, function(c){ return c.toUpperCase(); }); }
function setHero(ev){
  var hl = $('hlabel'); hl.textContent = cap1(ev.label); hl.classList.remove('swap'); void hl.offsetWidth; hl.classList.add('swap');
  $('hico').innerHTML = icon(ev.icon); $('hico').classList.toggle('rest', ev.phase === 'sleep');
  $('hphase').textContent = (ev.agent ? 'Helper ' + ev.agent : (PHASE_NAME[ev.phase] || 'Working'));
}
/* real terminal log */
function termAdd(el){
  var log = $('termlog'); var e = log.querySelector('.tempty'); if (e) e.remove();
  log.appendChild(el);
  while (log.children.length > 7) log.removeChild(log.firstChild);
  var kids = log.children; for (var i = 0; i < kids.length; i++) kids[i].classList.toggle('old', i < kids.length - 2);
}
function termLine(html){ var d = document.createElement('div'); d.className = 'tblock'; d.innerHTML = html; termAdd(d); }
var lastBlock = null;
function promptHtml(repo, cmd){ return '<span class="u">mars@stream</span><span class="s">:</span><span class="d">~/' + esc(repo ? repo.split('/')[1] : 'work') + '</span><span class="s">$</span> ' + esc(cmd); }
function termCmd(ev){
  var d = document.createElement('div'); d.className = 'tblock'; d.dataset.cmd = ev.cmd;
  d.innerHTML = '<div class="tprompt">' + promptHtml(ev.repo, ev.cmd) + '</div><div class="tout run"></div>';
  termAdd(d); lastBlock = d;
}
function termOut(ev){
  showView('term');
  var log = $('termlog'), block = null;
  var blocks = log.querySelectorAll('.tblock'); for (var i = blocks.length - 1; i >= 0; i--) { if (blocks[i].dataset.cmd === ev.cmd) { block = blocks[i]; break; } }
  if (!block) { block = document.createElement('div'); block.className = 'tblock'; block.dataset.cmd = ev.cmd; block.innerHTML = '<div class="tprompt">' + promptHtml(ev.repo, ev.cmd) + '</div><div class="tout"></div>'; termAdd(block); }
  var out = block.querySelector('.tout'); out.classList.remove('run');
  var txt = String(ev.out || '');
  out.innerHTML = txt ? esc(txt).replace(/(\\d+ passed[^\\n]*)/g, '<span class="tok">$1</span>').replace(/(\\d+ fail(?:ed)?[^\\n]*|Error[^\\n]*)/g, '<span class="tbad">$1</span>') : '<span class="s">(no output)</span>';
}
var nodeCount = 0;
function addNode(ev){
  if (!$('track')) return;
  var tr = $('track');
  tr.querySelectorAll('.node.cur').forEach(function(n){ n.classList.remove('cur'); n.classList.add('recent'); });
  var n = document.createElement('div');
  n.className = 'node cur' + (ev.term && !ev.term.failed ? ' ok' : '') + (ev.agent ? ' helper' : '');
  n.innerHTML = '<div class="dot">' + icon(ev.icon) + '</div><div class="lab">' + esc(cap1(ev.label)) + '</div>';
  n.title = cap1(ev.label) + ' · ' + fmt(ev.ts || Date.now());
  tr.appendChild(n);
  var nodes = tr.querySelectorAll('.node');
  for (var i = 0; i < nodes.length - 7; i++) nodes[i].remove();
  nodes = tr.querySelectorAll('.node');
  nodes.forEach(function(x, i){ if (i < nodes.length - 3) x.classList.remove('recent'); });
  nodeCount++;
  $('flowsub').innerHTML = '<b>' + nodeCount + '</b> steps since you tuned in' + (ev.repo ? ' · working in <b>' + esc(ev.repo.split('/')[1]) + '</b>' : '');
}
/* ---- real code from GitHub for public files ---- */
var codeCache = {};
function showCode(f){
  var parts = f.path.split('/');
  $('ed-file').textContent = parts[parts.length - 1] || f.repo;
  $('ed-path').textContent = f.repo + ' / ' + f.path + (f.line ? '  :' + f.line : '');
  setUrl('github.com/' + f.repo + '/blob/HEAD/' + f.path, true);
  $('ed-diff').innerHTML = (f.add || f.del) ? '<span class="a">+' + (f.add || 0) + '</span> <span class="d">-' + (f.del || 0) + '</span>' : '';
  var key = f.repo + '/' + f.path;
  var box = $('codebox');
  function draw(text){
    if (text === null) { box.innerHTML = '<div class="codemsg"><i class="ph-fill ph-file-plus"></i>New file, not on GitHub yet</div>'; return; }
    var lines = text.split(NL); var at = Math.max(1, Math.min(f.line || 1, lines.length));
    var from = Math.max(1, at - 8), to = Math.min(lines.length, from + 26);
    var hotTo = at + Math.max(0, (f.add || 1) - 1);
    var lang = (f.path.split('.').pop() || '').toLowerCase();
    var html = '';
    for (var n = from; n <= to; n++) {
      var src = lines[n - 1] || ' ';
      var code = esc(src);
      if (window.hljs) { try { code = hljs.highlight(src, { language: hljs.getLanguage(lang) ? lang : 'plaintext', ignoreIllegals: true }).value; } catch(e){} }
      var hot = f.line && n >= at && n <= hotTo;
      html += '<div class="row' + (hot ? ' hot' : '') + '"><span class="no">' + n + '</span><code class="hljs">' + code + '</code></div>';
    }
    box.innerHTML = '<pre>' + html + '</pre>';
  }
  if (key in codeCache) { draw(codeCache[key]); return; }
  box.innerHTML = '<div class="codemsg"><i class="ph-fill ph-file-code"></i>Opening ' + esc(parts[parts.length - 1]) + '</div>';
  fetch('https://raw.githubusercontent.com/' + f.repo + '/HEAD/' + f.path).then(function(r){
    if (r.status === 404) return null;
    if (!r.ok) throw new Error('http ' + r.status);
    return r.text();
  }).then(function(t){ if (t && t.length > 400000) t = t.slice(0, 400000); codeCache[key] = t; draw(t); })
    .catch(function(){ box.innerHTML = '<div class="codemsg"><i class="ph-fill ph-file-code"></i>' + esc(f.path) + '</div>'; });
}
/* ---- live world: Bitcoin blocks, Stacks tip, market, wallet ---- */
var MAIN = 'SP20GPDS5RYB2DV03KG4W08EG6HD11KYPK6FQJE1', lastTip = 0, burnTip = 0;
function ago(sec){ var s = Math.max(0, Math.floor(Date.now()/1000 - sec)); if (s < 60) return s + 's ago'; if (s < 3600) return Math.floor(s/60) + 'm ago'; return Math.floor(s/3600) + 'h ago'; }
function num(n){ return Number(n).toLocaleString('en-US'); }
function getJSON(u){ return fetch(u).then(function(r){ if (!r.ok) throw new Error(r.status); return r.json(); }); }
function loadBitcoin(){
  Promise.all([getJSON('https://mempool.space/api/v1/fees/mempool-blocks'), getJSON('https://mempool.space/api/v1/blocks')]).then(function(x){
    var pend = (x[0] || []).slice(0, 3).reverse(), mined = (x[1] || []).slice(0, 5);
    if (!mined.length) return;
    burnTip = mined[0].height;
    lastTip = mined[0].height;
    $('t-btc').textContent = '#' + num(mined[0].height) + (pend.length ? ' · ~' + Math.round(pend[pend.length - 1].medianFee) + ' sat/vB' : '');
    loadMarket();
  }).catch(function(){});
}
function loadStacks(){
  getJSON('https://api.hiro.so/extended/v2/blocks?limit=1').then(function(d){
    var b = (d.results || [])[0]; if (!b) return;
    $('t-stx').textContent = '#' + num(b.height);
  }).catch(function(){});
}
function loadWallet(){
  getJSON('https://api.hiro.so/extended/v1/address/' + MAIN + '/balances').then(function(d){
    var stx = Number(d.stx && d.stx.balance || 0) / 1e6;
    var sb = d.fungible_tokens && d.fungible_tokens['SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token::sbtc-token'];
    $('t-wal').textContent = num(sb ? sb.balance : 0) + ' sats sBTC · ' + stx.toFixed(2) + ' STX';
  }).catch(function(){});
}
function loadMarket(){
  getJSON('/world').then(function(d){
    var m = d.market; if (!m) return;
    var left = m.closeHeight && (m.burnHeight || burnTip) ? m.closeHeight - (m.burnHeight || burnTip) : 0;
    $('t-mkt').textContent = num(m.vaultSats || 0) + ' sats · ~' + Math.max(0, Math.round(left * 10 / 1440)) + 'd left';
    $('t-mkt').title = m.title || '';
  }).catch(function(){});
}
loadBitcoin(); loadStacks(); loadWallet();
setInterval(loadBitcoin, 60000); setInterval(loadStacks, 60000); setInterval(loadWallet, 120000);
/* ---- today's chart: ribbon of 15-minute slots + hourly stacked bars ---- */
var PH = { work:['#ff0033','Work'], inbox:['#3ea6ff','Inbox'], markets:['#ff9f43','Markets'], chat:['#b78cff','Chat'], checkin:['#2ecc71','Check-in'], report:['#f1c40f','Report'], save:['#8a94a6','Save'] };
var stats = { date:'', slots:{} };
function utcDay(ts){ return new Date(ts).toISOString().slice(0, 10); }
function countLive(ev){
  var ts = ev.ts || Date.now(), day = utcDay(ts);
  if (stats.date !== day) stats = { date: day, slots: {} };
  var slot = String(Math.floor((ts % 86400000) / 900000)), ph = ev.phase || 'work';
  var o = stats.slots[slot] = stats.slots[slot] || {}; o[ph] = (o[ph] || 0) + 1;
  drawStats();
}
function drawStats(){
  var rib = $('ribbon'), bars = $('bars');
  if (!rib || !bars) return;
  var today = utcDay(Date.now()), slots = stats.date === today ? stats.slots : {};
  var rh = '';
  for (var i = 0; i < 96; i++) {
    var o = slots[String(i)] || {}, best = null, bn = 0, sum = 0;
    Object.keys(o).forEach(function(k){ sum += o[k]; if (k !== 'sleep' && o[k] > bn) { bn = o[k]; best = k; } });
    var c = best && PH[best] ? PH[best][0] : (o.sleep ? '#1b2336' : '#222');
    rh += '<span title="' + pad(Math.floor(i / 4)) + ':' + pad((i % 4) * 15) + (best ? ' · ' + (PH[best] ? PH[best][1] : best) + ' · ' + sum + ' steps' : '') + '" style="background:' + c + (best ? ';opacity:' + Math.min(1, .45 + sum / 20) : '') + '"></span>';
  }
  var nowSlot = (Date.now() % 86400000) / 86400000 * 100;
  rib.innerHTML = rh + '<div class="now" style="left:' + nowSlot.toFixed(2) + '%"></div>';
  var hours = [], max = 1;
  for (var h = 0; h < 24; h++) {
    var acc = {}, tot = 0;
    for (var q = 0; q < 4; q++) { var o2 = slots[String(h * 4 + q)] || {}; Object.keys(o2).forEach(function(k){ if (k === 'sleep') return; acc[k] = (acc[k] || 0) + o2[k]; tot += o2[k]; }); }
    hours.push([acc, tot]); if (tot > max) max = tot;
  }
  bars.innerHTML = hours.map(function(x, h){
    var segs = Object.keys(PH).map(function(k){ var v = x[0][k] || 0; return v ? '<span style="height:' + (v / max * 100).toFixed(1) + '%;background:' + PH[k][0] + '"></span>' : ''; }).join('');
    return '<div class="bar" title="' + pad(h) + ':00 UTC · ' + x[1] + ' steps">' + segs + '</div>';
  }).join('');
}

connect(); setInterval(tick, 1000); tick();
</script>
</body>
</html>`;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname.startsWith('/music/')) return serveMusic(req, env);
    if (url.pathname === '/world') return world();
    if (url.pathname === '/' || url.pathname === '/index.html') {
      return new Response(HTML(url.origin, env.TURNSTILE_SITEKEY || ''), {
        headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' },
      });
    }
    // Single global stream room — one DO instance for the whole feed.
    const id = env.STREAM.idFromName('global');
    const stub = env.STREAM.get(id);
    return stub.fetch(req);
  },
};

export class EventStream {
  state: DurableObjectState;
  env: Env;
  ring: any[] = [];
  ringMax: number;
  pending: { id: string; ts: number; name: string; body: string }[] = [];
  // Recent activity steps (what the agent is doing). Kept in memory only, not in the ring,
  // so a burst of tool steps never pushes narration or chat out of the buffer.
  activity: any[] = [];
  donations: Record<string, any> = {};
  donHits: Map<string, number[]> = new Map();
  // Today's activity per 15-minute UTC slot and phase, for the stream's charts.
  stats: { date: string; slots: Record<string, Record<string, number>> } = { date: '', slots: {} };
  hits: Map<string, number[]> = new Map();
  globalHits: number[] = [];
  // No in-memory client list — the WebSocket Hibernation runtime tracks
  // accepted sockets externally. Use this.state.getWebSockets() to fan out.

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.ringMax = parseInt(env.RING_SIZE || '500', 10);
    this.state.blockConcurrencyWhile(async () => {
      this.ring = (await this.state.storage.get<any[]>('ring')) || [];
      this.pending = (await this.state.storage.get<any[]>('pending')) || [];
      this.ring = this.ring.filter(e => e.kind !== 'activity' && e.kind !== 'heartbeat');
      this.stats = (await this.state.storage.get<any>('stats')) || { date: '', slots: {} };
      this.activity = (await this.state.storage.get<any[]>('activity')) || [];
      this.donations = (await this.state.storage.get<any>('donations')) || {};
    });
  }

  broadcast(ev: any) {
    // Heartbeats only keep the LIVE badge fresh: send them, never store them, so the
    // buffer keeps narration and chat for new viewers.
    if (ev.kind !== 'heartbeat') {
      this.ring.push(ev);
      while (this.ring.length > this.ringMax) this.ring.shift();
      this.state.storage.put('ring', this.ring).catch(() => {});
    }
    const msg = JSON.stringify(ev);
    // Fan-out: WebSocket Hibernation API tracks accepted sockets even when
    // the DO has hibernated. ws.send is synchronous; bad sockets just throw.
    // Sockets tagged 'comments' (the agent's push feed) get viewer comments only.
    for (const ws of this.state.getWebSockets()) {
      if (this.isFeed(ws) && ev.kind !== 'viewer' && !(ev.kind === 'donation' && ev.body && ev.status === 'pending')) continue;
      try { ws.send(msg); } catch {}
    }
  }

  isFeed(ws: WebSocket): boolean {
    try { return this.state.getTags(ws).includes('comments'); } catch { return false; }
  }

  countActivity(act: any) {
    const day = new Date(act.ts).toISOString().slice(0, 10);
    if (this.stats.date !== day) this.stats = { date: day, slots: {} };
    const slot = String(Math.floor((act.ts % 86400000) / 900000));
    const phase = act.phase || 'work';
    const s = (this.stats.slots[slot] = this.stats.slots[slot] || {});
    s[phase] = (s[phase] || 0) + 1;
    this.state.storage.put('stats', this.stats).catch(() => {});
  }

  // Live viewer count: open sockets. Not stored in the ring.
  presence() {
    const socks = this.state.getWebSockets().filter(ws => ws.readyState === 1 && !this.isFeed(ws));
    const msg = JSON.stringify({ ts: Date.now(), kind: 'presence', viewers: socks.length });
    for (const ws of socks) { try { ws.send(msg); } catch {} }
  }

  authed(req: Request): boolean {
    const auth = req.headers.get('authorization') || '';
    return !!this.env.POST_TOKEN && auth === 'Bearer ' + this.env.POST_TOKEN;
  }

  rateLimited(ip: string, now: number): boolean {
    this.globalHits = this.globalHits.filter(t => now - t < WINDOW_MS);
    const mine = (this.hits.get(ip) || []).filter(t => now - t < WINDOW_MS);
    if (mine.length >= PER_IP_LIMIT || this.globalHits.length >= GLOBAL_LIMIT) {
      this.hits.set(ip, mine);
      return true;
    }
    mine.push(now);
    this.hits.set(ip, mine);
    this.globalHits.push(now);
    if (this.hits.size > 5000) this.hits.clear();
    return false;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    // Open to anyone: a viewer comment. Plain text, shown live, queued for a reply.
    if (req.method === 'POST' && url.pathname === '/comment') {
      let payload: any;
      try { payload = await req.json(); } catch { return json({ error: 'bad json' }, 400); }
      const body = clean(payload.text, COMMENT_MAX + 1);
      if (!body) return json({ error: 'empty comment' }, 400);
      if (body.length > COMMENT_MAX) return json({ error: 'comment is longer than ' + COMMENT_MAX + ' characters' }, 400);
      const name = clean(payload.name, NAME_MAX).replace(/\s+/g, ' ') || 'anon';
      const ip = req.headers.get('cf-connecting-ip') || '';
      if (this.env.TURNSTILE_SECRET) {
        if (!(await turnstileOk(this.env.TURNSTILE_SECRET, String(payload.turnstile || ''), ip))) {
          return json({ error: 'anti-bot check failed, please retry' }, 403);
        }
      }
      const now = Date.now();
      if (this.rateLimited(ip || 'unknown', now)) {
        return json({ error: 'too many comments right now, try again in a few minutes' }, 429);
      }
      const id = crypto.randomUUID().replace(/-/g, '').slice(0, 10);
      const ev = { ts: now, kind: 'viewer', id, name, body };
      this.pending.push({ id, ts: now, name, body });
      while (this.pending.length > PENDING_MAX) this.pending.shift();
      this.state.storage.put('pending', this.pending).catch(() => {});
      this.broadcast(ev);
      return json({ ok: true, id });
    }

    // A viewer just paid with their wallet: verify on-chain, then show it.
    if (req.method === 'POST' && url.pathname === '/donation') {
      let payload: any;
      try { payload = await req.json(); } catch { return json({ error: 'bad json' }, 400); }
      const txid = String(payload.txid || '').trim().toLowerCase();
      if (!TXID_RE.test(txid)) return json({ error: 'bad txid' }, 400);
      const key = txid.startsWith('0x') ? txid : '0x' + txid;
      if (this.donations[key]) return json({ ok: true, donation: this.donations[key] });
      const ip = req.headers.get('cf-connecting-ip') || 'unknown';
      const now = Date.now();
      const mine = (this.donHits.get(ip) || []).filter(t => now - t < WINDOW_MS);
      if (mine.length >= 10) return json({ error: 'too many requests, try again shortly' }, 429);
      mine.push(now); this.donHits.set(ip, mine);
      const tx = await readDonationTx(key);
      if (tx === null) return json({ error: 'that transaction is not a donation to Secret Mars' }, 400);
      if (tx.notFound || tx.retry) return json({ error: 'not visible on chain yet', retry: true }, 202);
      if (tx.status !== 'pending' && tx.status !== 'success') return json({ error: 'transaction failed on chain (' + tx.status + ')' }, 400);
      // Only fresh payments can be claimed, so nobody can attach a message to an old,
      // unrelated transfer to this wallet.
      if (tx.status === 'success' && tx.at && Date.now() / 1000 - tx.at > 1800) {
        return json({ error: 'that payment is older than 30 minutes' }, 400);
      }
      const d: any = {
        ts: now, kind: 'donation', id: 'd' + key.slice(2, 12), txid: key, from: tx.from, asset: tx.asset, amount: tx.amount,
        status: tx.status === 'success' ? 'confirmed' : 'pending',
        name: clean(payload.name, NAME_MAX).replace(/\s+/g, ' ') || '',
        body: clean(payload.message, 200),
      };
      this.donations[key] = d;
      this.state.storage.put('donations', this.donations).catch(() => {});
      if (d.body) {
        // A paid message: queue it for a reply, first in line.
        this.pending.unshift({ id: d.id, ts: now, name: d.name || d.from.slice(0, 8), body: d.body, paid: { asset: d.asset, amount: d.amount } } as any);
        while (this.pending.length > PENDING_MAX) this.pending.pop();
        this.state.storage.put('pending', this.pending).catch(() => {});
      }
      this.broadcast(d);
      if (d.status === 'pending') await this.state.storage.setAlarm(Date.now() + 15000);
      return json({ ok: true, donation: d });
    }

    // Agent-only: comments still waiting for a reply.
    if (req.method === 'GET' && url.pathname === '/comments') {
      if (!this.authed(req)) return new Response('unauthorized', { status: 401 });
      return json({ pending: this.pending });
    }

    if (req.method === 'POST' && url.pathname === '/event') {
      if (!this.authed(req)) {
        return new Response('unauthorized', { status: 401 });
      }
      let payload: any;
      try { payload = await req.json(); } catch { return new Response('bad json', { status: 400 }); }
      if (payload.kind === 'artifact') {
        const art = cleanArtifact(payload);
        if (art) this.broadcast(art);
        return new Response('ok');
      }
      if (payload.kind === 'term') {
        const t = cleanTerm(payload);
        if (t) {
          this.activity.push(t);
          while (this.activity.length > 40) this.activity.shift();
          this.state.storage.put('activity', this.activity).catch(() => {});
          const msg = JSON.stringify(t);
          for (const ws of this.state.getWebSockets()) { if (this.isFeed(ws)) continue; try { ws.send(msg); } catch {} }
        }
        return new Response('ok');
      }
      if (payload.kind === 'activity') {
        const act: any = cleanActivity(payload);
        if (!act) return new Response('ok');
        this.activity.push(act);
        if (act.kind === 'activity') this.countActivity(act);
        while (this.activity.length > 40) this.activity.shift();
        // Persist, so a deploy or eviction doesn't blank the stage for new viewers.
        this.state.storage.put('activity', this.activity).catch(() => {});
        const msg = JSON.stringify(act);
        for (const ws of this.state.getWebSockets()) { if (this.isFeed(ws)) continue; try { ws.send(msg); } catch {} }
        return new Response('ok');
      }
      const ev = {
        ts: Date.now(),
        kind: String(payload.kind || 'note').slice(0, 32),
        tool: payload.tool ? String(payload.tool).slice(0, 64) : undefined,
        body: payload.body ? String(payload.body).slice(0, 8000) : undefined,
      } as any;
      // Helper agents (sub-agents) narrate in their own labelled lane.
      if (payload.agent) ev.agent = clean(payload.agent, 40);
      // A reply to a viewer comment: link it and take it off the pending list.
      if (ev.kind === 'reply' && payload.reply_to) {
        const rid = clean(payload.reply_to, 16);
        const orig = this.pending.find(c => c.id === rid) || this.ring.find(e => (e.kind === 'viewer' || e.kind === 'donation') && e.id === rid);
        if (!orig) return json({ error: 'no such comment' }, 404);
        ev.reply_to = rid;
        ev.quote = { name: orig.name, body: String(orig.body).slice(0, 280) };
        this.pending = this.pending.filter(c => c.id !== rid);
        this.state.storage.put('pending', this.pending).catch(() => {});
      }
      this.broadcast(ev);
      return new Response('ok');
    }

    if (req.method === 'GET' && url.pathname === '/ws') {
      if (req.headers.get('upgrade') !== 'websocket') {
        return new Response('expected websocket upgrade', { status: 426 });
      }
      const replayN = Math.max(0, Math.min(500, parseInt(url.searchParams.get('replay') || '10', 10)));
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      // Push feed for the agent: only new viewer comments, pushed the moment they arrive.
      // Viewer comments are public on the page already, so this needs no token.
      if (url.searchParams.get('feed') === 'comments') {
        this.state.acceptWebSocket(server, ['comments']);
        return new Response(null, { status: 101, webSocket: client });
      }
      // acceptWebSocket = hibernation-mode accept. The runtime keeps the
      // connection alive across DO evictions and routes inbound frames to
      // the webSocket* handler methods on this class.
      this.state.acceptWebSocket(server);
      const ring = replayN === 0 ? [] : this.ring.slice(-replayN);
      for (const ev of ring) {
        try { server.send(JSON.stringify(ev)); } catch {}
      }
      for (const act of this.activity.slice(-24)) {
        try { server.send(JSON.stringify(act)); } catch {}
      }
      try { server.send(JSON.stringify({ ts: Date.now(), kind: 'stats', date: this.stats.date, slots: this.stats.slots })); } catch {}
      // Tell everyone (including the newcomer, after the 101) the new viewer count.
      this.state.waitUntil(new Promise<void>(r => setTimeout(() => { this.presence(); r(); }, 50)));
      return new Response(null, { status: 101, webSocket: client });
    }

    if (req.method === 'GET' && url.pathname === '/history') {
      const before = parseInt(url.searchParams.get('before') || String(Date.now()), 10);
      const limit  = Math.max(1, Math.min(100, parseInt(url.searchParams.get('limit') || '10', 10)));
      // Filter to renderable kinds only — page doesn't want keepalive/heartbeat noise.
      const visible = this.ring.filter(e => VISIBLE_KINDS.has(e.kind));
      const older = visible.filter(e => typeof e.ts === 'number' && e.ts < before);
      const slice = older.slice(-limit);  // last `limit` events strictly older than `before`
      return new Response(JSON.stringify({ events: slice, oldest_in_ring: visible[0]?.ts ?? null }), {
        headers: {
          'content-type': 'application/json',
          'cache-control': 'no-store',
          'access-control-allow-origin': '*',
        },
      });
    }

    return new Response('not found', { status: 404 });
  }

  // Re-check pending donations until they confirm or fail (max ~1 hour).
  async alarm(): Promise<void> {
    let again = false;
    for (const key of Object.keys(this.donations)) {
      const d = this.donations[key];
      if (d.status !== 'pending') continue;
      if (Date.now() - d.ts > 3600000) { d.status = 'expired'; this.broadcast({ ...d, ts: Date.now(), update: true }); continue; }
      let tx: any = null;
      try { tx = await readDonationTx(key); } catch { again = true; continue; }
      if (tx && tx.status === 'success') { d.status = 'confirmed'; this.broadcast({ ...d, ts: Date.now(), update: true }); }
      else if (tx && tx.status && tx.status !== 'pending') { d.status = 'failed'; this.broadcast({ ...d, ts: Date.now(), update: true }); }
      else again = true;
    }
    this.state.storage.put('donations', this.donations).catch(() => {});
    if (again) await this.state.storage.setAlarm(Date.now() + 20000);
  }

  // Hibernation-API event handlers. The runtime invokes these on the DO
  // instance (which may have been freshly created after eviction) when a
  // websocket frame arrives from the client. We're broadcast-only, so we
  // ignore inbound messages — they exist purely as a client-side keepalive
  // mechanism if the page chooses to send any.
  async webSocketMessage(_ws: WebSocket, _msg: string | ArrayBuffer): Promise<void> {
    // intentionally empty
  }

  async webSocketClose(ws: WebSocket, code: number, _reason: string, _wasClean: boolean): Promise<void> {
    try { ws.close(code, 'goodbye'); } catch {}
    this.presence();
  }

  async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    try { ws.close(1011, 'error'); } catch {}
  }
}
