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

const COMMENT_MAX = 500;
const NAME_MAX = 32;
const PER_IP_LIMIT = 3;          // comments per IP per window
const GLOBAL_LIMIT = 40;         // comments per window across everyone
const WINDOW_MS = 10 * 60 * 1000;
const PENDING_MAX = 200;
const VISIBLE_KINDS = new Set(['assistant', 'cycle', 'viewer', 'reply', 'artifact']);
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
  }
  if (p.term) a.term = { passed: int(p.term.passed) ?? 0, failed: int(p.term.failed) ?? 0 };
  const until = int(p.until, 9e15); if (until) a.until = until;
  return a;
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
          <div class="pane-bar"><div class="dots"><i></i><i></i><i></i></div><div class="urlbar" id="urlbar"><i class="ph-fill ph-lock-simple"></i><span id="urltext">secret-mars workspace</span></div></div>
          <div class="pane-body">
            <div class="view v-browser" id="v-browser"><div class="ogcard" id="ogcard" style="display:none"></div><div class="gh-empty" id="gh-empty"><i class="ph-fill ph-github-logo"></i>Browsing GitHub</div></div>
            <div class="view v-editor" id="v-editor"><div class="tabs"><div class="tab"><i class="ph-fill ph-file-code"></i><span id="ed-file">file</span></div></div><div class="crumbs" id="ed-path"></div><div class="code" id="ed-code"></div><div class="diffstat" id="ed-diff"></div></div>
            <div class="view v-terminal on" id="v-terminal"><div id="term"></div><div class="tline"><span class="pr">mars@stream:~$</span><span class="caret"></span></div></div>
            <div class="view v-chain" id="v-chain"><div class="blocks"><div class="blk"><i class="ph-fill ph-currency-btc"></i></div><div class="chainlink"></div><div class="blk stx live"><i class="ph-fill ph-cube"></i></div></div><div class="chain-label" id="chain-label">On-chain</div></div>
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
        <div class="chips"><a class="chip" href="https://github.com/secret-mars/drx4" target="_blank" rel="noopener"><i class="ph-fill ph-github-logo"></i>GitHub</a><a class="chip" href="https://aibtc.com" target="_blank" rel="noopener"><i class="ph-fill ph-currency-btc"></i>AIBTC</a></div>
      </div>
      <div class="credit" id="credit">Music: <a href="https://opengameart.org/content/chill-beats-collection" target="_blank" rel="noopener">HoliznaCC0, Chill Beats</a> (CC0)</div>
      <div class="stats">
        <div class="stat"><i class="ph-fill ph-chats-circle"></i><div><b id="s-review">0</b><span>reviews today</span></div></div>
        <div class="stat"><i class="ph-fill ph-bug"></i><div><b id="s-issue">0</b><span>bugs filed</span></div></div>
        <div class="stat"><i class="ph-fill ph-git-pull-request"></i><div><b id="s-pr">0</b><span>pull requests</span></div></div>
        <div class="stat"><i class="ph-fill ph-chat-circle-dots"></i><div><b id="s-chat">0</b><span>chat replies</span></div></div>
        <div class="stat"><i class="ph-fill ph-lightning"></i><div><b id="s-steps">0</b><span>steps watched</span></div></div>
      </div>
      <div class="box"><h3><i class="ph-fill ph-star"></i>Highlights</h3><div class="hl" id="hl"><div class="hl-empty" id="hl-empty">Reviews, bug reports and pull requests show up here as they are published.</div></div></div>
      <div class="box"><h3><i class="ph-fill ph-clock-counter-clockwise"></i>Earlier on stream</h3><div id="log"></div><div class="more" id="loader">Show more</div></div>
    </div>
  </div>
  <aside class="chat">
    <div class="chat-head">Live chat <span class="n"><i class="ph-fill ph-users"></i><span id="chat-n">0</span></span></div>
    <div id="chat"><div class="chat-empty" id="chat-empty">Say hi to Secret Mars</div></div>
    <form id="ask" autocomplete="off">
      <div class="who"><span class="av" id="me-av">?</span><input id="ask-name" maxlength="32" placeholder="Your name"></div>
      <div class="row"><textarea id="ask-text" maxlength="500" rows="1" placeholder="Chat..."></textarea><button id="ask-send" type="submit" aria-label="Send"><i class="ph-fill ph-paper-plane-right"></i></button></div>
      ${sitekey ? '<div class="ts-box cf-turnstile" data-sitekey="' + sitekey + '" data-theme="dark" data-size="flexible" data-appearance="interaction-only"></div>' : ''}
      <div id="ask-err"></div>
    </form>
  </aside>
</div>
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
  ['browser','editor','terminal','chain'].forEach(function(v){ $('v-' + v).classList.toggle('on', v === name); });
}
function setPrivate(on, label){ $('private').classList.toggle('on', !!on); if (label) $('private-label').textContent = label; }
function setUrl(text, lock){ $('urltext').textContent = text; $('urlbar').firstChild.className = 'ph-fill ' + (lock ? 'ph-lock-simple' : 'ph-globe'); }
function drawCode(add, del){
  var code = $('ed-code'); code.innerHTML = '';
  var n = 14, start = 3 + Math.floor(Math.random() * 4);
  for (var i = 0; i < n; i++) {
    var ln = document.createElement('div'); ln.className = 'ln';
    ln.style.width = (25 + Math.floor(Math.random() * 65)) + '%';
    ln.style.marginLeft = (Math.floor(Math.random() * 4) * 3) + '%';
    if (i >= start && i < start + Math.min(del, 3)) ln.className += ' del';
    else if (i >= start + Math.min(del, 3) && i < start + Math.min(del, 3) + Math.min(add, 5)) ln.className += ' add';
    code.appendChild(ln);
  }
  var last = code.querySelectorAll('.ln.add'); if (last.length) last[last.length - 1].classList.add('cursor');
}
function termLine(html, cls){
  var t = $('term'); var d = document.createElement('div'); d.className = 'tline' + (cls ? ' ' + cls : ''); d.innerHTML = html; t.appendChild(d);
  while (t.children.length > 9) t.removeChild(t.firstChild);
  var kids = t.children; for (var i = 0; i < kids.length; i++) kids[i].classList.toggle('old', i < kids.length - 3);
}
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
    } else { setUrl('github.com', true); $('ogcard').style.display = 'none'; $('gh-empty').style.display = 'flex'; if (/research/.test(ev.label)) { setUrl('searching the web', false); } }
  } else if (ev.pane === 'editor') {
    showView('editor');
    if (ev.file) {
      var parts = ev.file.path.split('/');
      $('ed-file').textContent = parts[parts.length - 1] || ev.file.repo;
      $('ed-path').textContent = ev.file.repo + ' / ' + ev.file.path;
      setUrl('github.com/' + ev.file.repo, true);
      drawCode(ev.file.add || 0, ev.file.del || 0);
      $('ed-diff').innerHTML = (ev.file.add || ev.file.del) ? '<span class="a">+' + (ev.file.add || 0) + '</span> <span class="d">-' + (ev.file.del || 0) + '</span>' : '';
    } else { setPrivate(true, ev.label.replace(/^\\w/, function(c){ return c.toUpperCase(); })); setUrl('private workspace', true); }
  } else if (ev.pane === 'chain') {
    showView('chain'); $('chain-label').textContent = ev.label.replace(/^\\w/, function(c){ return c.toUpperCase(); }); setUrl('stacks mainnet', true);
  } else {
    showView('terminal'); setUrl(ev.repo ? 'github.com/' + ev.repo : 'secret-mars workspace', true);
  }
  var cls = ev.term ? (ev.term.failed ? 'bad' : 'ok') : '';
  termLine('<span class="pr">' + icon(ev.icon) + '</span><span>' + esc(ev.label) + '</span>', cls);
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
  if (kind !== 'viewer' && typeof ev.ts === 'number' && ev.ts > state.lastSeen) state.lastSeen = ev.ts;
  if (kind === 'activity') { if (ev.label) applyActivity(ev, live); tick(); return; }
  if (kind === 'artifact') { var k = 'a:' + ev.url; if (seen[k]) return; seen[k] = 1; addHighlight(ev); if (live) alertCard(ev); return; }
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
connect(); setInterval(tick, 1000); tick();
</script>
</body>
</html>`;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname.startsWith('/music/')) return serveMusic(req, env);
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
      this.ring = this.ring.filter(e => e.kind !== 'activity');
    });
  }

  broadcast(ev: any) {
    this.ring.push(ev);
    while (this.ring.length > this.ringMax) this.ring.shift();
    this.state.storage.put('ring', this.ring).catch(() => {});
    const msg = JSON.stringify(ev);
    // Fan-out: WebSocket Hibernation API tracks accepted sockets even when
    // the DO has hibernated. ws.send is synchronous; bad sockets just throw.
    // Sockets tagged 'comments' (the agent's push feed) get viewer comments only.
    for (const ws of this.state.getWebSockets()) {
      if (this.isFeed(ws) && ev.kind !== 'viewer') continue;
      try { ws.send(msg); } catch {}
    }
  }

  isFeed(ws: WebSocket): boolean {
    try { return this.state.getTags(ws).includes('comments'); } catch { return false; }
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
      if (payload.kind === 'activity') {
        const act: any = cleanActivity(payload);
        if (!act) return new Response('ok');
        this.activity.push(act);
        while (this.activity.length > 40) this.activity.shift();
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
        const orig = this.pending.find(c => c.id === rid) || this.ring.find(e => e.kind === 'viewer' && e.id === rid);
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
      for (const act of this.activity.slice(-12)) {
        try { server.send(JSON.stringify(act)); } catch {}
      }
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
