import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const siteDir = path.join(docsDir, 'api-docs')
const openrpc = JSON.parse(fs.readFileSync(path.join(siteDir, 'openrpc.json')))
const asyncapi = JSON.parse(fs.readFileSync(path.join(siteDir, 'asyncapi.json')))

const rpc = openrpc.methods.map(method => ({
  kind: 'rpc', name: method.name, summary: method.summary,
  href: `#rpc-${method.name.replaceAll('.', '-')}`,
  example: JSON.stringify({ jsonrpc: '2.0', method: method.name, params: Object.fromEntries(method.params.filter(p => p.required).map(p => [p.name, `<${p.name}>`])), id: 1 }, null, 2)
}))
const events = Object.values(asyncapi.components.messages).map(message => ({
  kind: 'event', name: message.name, summary: message.summary,
  href: `#event-${message.name.replaceAll('.', '-')}`,
  example: JSON.stringify(message.examples[0].payload, null, 2)
}))
const model = {
  profile: openrpc['x-nano-profile'],
  replay: asyncapi.channels.confirmations.description,
  reset: asyncapi.info.description,
  entries: [...rpc, ...events]
}
fs.writeFileSync(path.join(siteDir, 'reference.json'), `${JSON.stringify(model, null, 2)}\n`)

function cards(entries) {
  return entries.map(entry => `<article id="${entry.href.slice(1)}" data-entry="${entry.name} ${entry.summary}"><div class="eyebrow">${entry.kind === 'rpc' ? 'JSON-RPC method' : 'SSE notification'}</div><h2>${entry.name}</h2><p>${entry.summary}</p><div class="example"><button class="copy" type="button">Copy</button><pre><code>${escapeHtml(entry.example)}</code></pre></div></article>`).join('\n')
}
const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="Nano Gateway RPC and event contracts"><title>Nano Gateway Reference</title><link rel="stylesheet" href="assets/reference.css"></head>
<body><header><a class="brand" href="#top"><span class="mark">N</span><span>Nano Gateway<small>${model.profile}</small></span></a><nav><a href="openrpc.json">OpenRPC JSON</a><a href="asyncapi.json">AsyncAPI JSON</a></nav></header>
<main id="top"><section class="hero"><p class="kicker">Unified API reference</p><h1>Call methods. Receive events.</h1><p>OpenRPC defines callable Nano RPC operations. AsyncAPI defines the receive-only confirmation stream. Two contracts, one searchable reference.</p><label class="search"><span>Search</span><input id="search" type="search" placeholder="Try account_info or stream reset" autocomplete="off"></label></section>
<div class="tabs" role="tablist"><button class="tab active" data-tab="rpc">RPC <span>${rpc.length}</span></button><button class="tab" data-tab="event">Events <span>${events.length}</span></button></div>
<section id="rpc" class="panel active"><div class="intro"><h2>RPC methods</h2><p>Callable JSON-RPC 2.0 request and response methods from the Clean V28.2 profile.</p></div>${cards(rpc)}</section>
<section id="event" class="panel"><div class="intro"><h2>Confirmation events</h2><p>${model.replay}</p><p>${model.reset}</p><code>GET /events/confirmations</code> <code>Last-Event-ID</code> <code>text/event-stream</code></div>${cards(events)}</section>
<p id="empty" hidden>No contract entries match that search.</p></main><script src="assets/reference.js"></script></body></html>`
function escapeHtml(value) { return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;') }

const css = `:root{color-scheme:dark;--ink:#eaf4ff;--muted:#91a5bb;--line:#24394e;--accent:#63e6be;--blue:#74c0fc;--panel:#101d2a}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(circle at 80% 0,#173a4d 0,transparent 32rem),#08121c;color:var(--ink);font:16px/1.55 Inter,ui-sans-serif,system-ui,sans-serif}header{height:72px;padding:0 max(24px,calc((100vw - 1120px)/2));display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line);position:sticky;top:0;background:#08121ce8;backdrop-filter:blur(14px);z-index:2}.brand{display:flex;gap:12px;align-items:center;color:inherit;text-decoration:none;font-weight:700}.brand small{display:block;color:var(--muted);font-weight:500}.mark{display:grid;place-items:center;width:36px;height:36px;border-radius:10px;background:var(--accent);color:#062018}nav{display:flex;gap:20px}nav a{color:var(--muted)}main{max-width:1120px;margin:auto;padding:80px 24px}.hero{max-width:800px}.kicker,.eyebrow{color:var(--accent);text-transform:uppercase;letter-spacing:.12em;font-size:.74rem;font-weight:800}.hero h1{font-size:clamp(2.6rem,7vw,5.4rem);line-height:.95;letter-spacing:-.055em;margin:.2em 0}.hero>p:not(.kicker){color:var(--muted);font-size:1.18rem;max-width:680px}.search{display:block;margin:38px 0}.search span{display:block;color:var(--muted);font-size:.8rem;margin-bottom:8px}.search input{width:100%;padding:16px 18px;border:1px solid var(--line);border-radius:12px;background:#0c1925;color:var(--ink);font:inherit;outline:none}.search input:focus{border-color:var(--blue);box-shadow:0 0 0 3px #74c0fc22}.tabs{display:flex;gap:8px;border-bottom:1px solid var(--line);margin-top:30px}.tab{padding:14px 18px;border:0;border-bottom:2px solid transparent;background:none;color:var(--muted);font:inherit;font-weight:700;cursor:pointer}.tab.active{color:var(--ink);border-color:var(--accent)}.tab span{padding:2px 7px;margin-left:5px;border-radius:99px;background:var(--line);font-size:.75rem}.panel{display:none}.panel.active{display:block}.intro{padding:44px 0 24px;max-width:760px}.intro p{color:var(--muted)}.intro code{display:inline-block;margin:4px 5px 0 0;padding:5px 9px;background:#162638;border-radius:6px;color:var(--blue)}article{display:grid;grid-template-columns:minmax(220px,1fr) minmax(320px,1.4fr);gap:8px 36px;padding:40px 0;border-top:1px solid var(--line);scroll-margin-top:90px}article h2{font:700 1.55rem ui-monospace,monospace;margin:.2rem 0}article>p{color:var(--muted);grid-column:1}.example{grid-column:2;grid-row:1/4;position:relative}.copy{position:absolute;right:9px;top:9px;border:1px solid var(--line);border-radius:7px;padding:6px 9px;background:#142436;color:var(--ink);cursor:pointer}pre{margin:0;padding:22px;overflow:auto;border:1px solid var(--line);border-radius:12px;background:#071019;color:#b9e7ff;font-size:.83rem}#empty{padding:44px 0;color:var(--muted)}@media(max-width:720px){header{height:auto;padding:16px 20px;align-items:flex-start}nav{flex-direction:column;gap:2px;text-align:right;font-size:.85rem}main{padding:52px 20px}.hero h1{font-size:3.2rem}article{grid-template-columns:1fr}.example{grid-column:1;grid-row:auto}}\n`
const js = `const tabs=[...document.querySelectorAll('.tab')],panels=[...document.querySelectorAll('.panel')],entries=[...document.querySelectorAll('article')],search=document.querySelector('#search'),empty=document.querySelector('#empty');function activate(kind){tabs.forEach(x=>x.classList.toggle('active',x.dataset.tab===kind));panels.forEach(x=>x.classList.toggle('active',x.id===kind))}tabs.forEach(x=>x.addEventListener('click',()=>activate(x.dataset.tab)));function filter(){const q=search.value.trim().toLowerCase();let count=0;entries.forEach(x=>{const show=!q||x.dataset.entry.toLowerCase().includes(q);x.hidden=!show;if(show)count++});empty.hidden=count>0;if(q){const kinds=new Set(entries.filter(x=>!x.hidden).map(x=>x.closest('.panel').id));if(kinds.size===1)activate([...kinds][0])}}search.addEventListener('input',filter);document.querySelectorAll('.copy').forEach(x=>x.addEventListener('click',async()=>{await navigator.clipboard.writeText(x.nextElementSibling.innerText);x.textContent='Copied';setTimeout(()=>x.textContent='Copy',1200)}));if(location.hash.startsWith('#event-'))activate('event');\n`

fs.mkdirSync(path.join(siteDir, 'assets'), { recursive: true })
fs.writeFileSync(path.join(siteDir, 'index.html'), `${html}\n`)
fs.writeFileSync(path.join(siteDir, 'assets/reference.css'), css)
fs.writeFileSync(path.join(siteDir, 'assets/reference.js'), js)
