import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import hljs from 'highlight.js/lib/core'
import json from 'highlight.js/lib/languages/json'

hljs.registerLanguage('json', json)

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const siteDir = path.join(docsDir, 'api-docs')
const fontSources = [
  ['montserrat-latin-wght-normal.woff2', path.join(docsDir, 'node_modules/@fontsource-variable/montserrat/files/montserrat-latin-wght-normal.woff2')],
  ['open-sans-latin-wght-normal.woff2', path.join(docsDir, 'node_modules/@fontsource-variable/open-sans/files/open-sans-latin-wght-normal.woff2')],
  ['LICENSE-montserrat.txt', path.join(docsDir, 'node_modules/@fontsource-variable/montserrat/LICENSE')],
  ['LICENSE-open-sans.txt', path.join(docsDir, 'node_modules/@fontsource-variable/open-sans/LICENSE')]
]
const openrpc = JSON.parse(fs.readFileSync(path.join(siteDir, 'openrpc.json')))
const asyncapi = JSON.parse(fs.readFileSync(path.join(siteDir, 'asyncapi.json')))
const schemas = openrpc.components.schemas

function exampleValue(parameter) {
  if (parameter.schema?.type === 'array') {
    const item = parameter.name === 'accounts' ? 'account'
      : parameter.name === 'hashes' ? 'hash'
        : parameter.name
    return [`<${item}>`]
  }
  return `<${parameter.name}>`
}

function resolveSchema(schema) {
  if (!schema?.$ref) return schema
  const name = schema.$ref.replace('#/components/schemas/', '')
  return schemas[name] ?? schema
}

function typeShape(schema, seen = new Set()) {
  const resolved = resolveSchema(schema)
  if (!resolved || seen.has(resolved)) return '<value>'
  seen.add(resolved)
  if (resolved.oneOf) {
    return `<${resolved.oneOf.map(item => resolveSchema(item)?.type ?? 'value').join(' | ')}>`
  }
  if (resolved.type === 'object') {
    const properties = Object.entries(resolved.properties ?? {})
    if (properties.length) {
      return Object.fromEntries(properties.map(([name, value]) => [name, typeShape(value, new Set(seen))]))
    }
    if (resolved.additionalProperties) {
      return { '<key>': typeShape(resolved.additionalProperties, new Set(seen)) }
    }
    return {}
  }
  if (resolved.type === 'array') return [typeShape(resolved.items, new Set(seen))]
  if (resolved.type === 'boolean') return '<boolean>'
  if (resolved.type === 'integer' || resolved.type === 'number') return `<${resolved.type}>`
  if (resolved.type === 'null') return null
  return `<${resolved.type ?? 'value'}>`
}

function requestExample(method) {
  return JSON.stringify({
    jsonrpc: '2.0',
    method: method.name,
    params: Object.fromEntries(method.params.filter(parameter => parameter.required).map(parameter => [parameter.name, exampleValue(parameter)])),
    id: 1
  }, null, 2)
}

function resultExample(method) {
  return JSON.stringify({ result: typeShape(method.result.schema) }, null, 2)
}

const rpc = openrpc.methods.map(method => ({
  kind: 'rpc',
  name: method.name,
  summary: method.summary,
  href: `#rpc-${method.name.replaceAll('.', '-')}`,
  request: requestExample(method),
  result: resultExample(method),
  search: `${method.name} ${method.summary} ${requestExample(method)} ${resultExample(method)}`
}))
const events = Object.values(asyncapi.components.messages).map(message => ({
  kind: 'event',
  name: message.name,
  summary: message.summary,
  href: `#event-${message.name.replaceAll('.', '-')}`,
  event: JSON.stringify(message.examples[0].payload, null, 2),
  search: `${message.name} ${message.summary} ${JSON.stringify(message.examples[0].payload)}`
}))
const model = {
  profile: openrpc['x-nano-profile'],
  accountTracking: asyncapi['x-nano-account-tracking'],
  confirmationTracking: asyncapi['x-nano-confirmation-tracking'],
  entries: [...rpc, ...events]
}
fs.writeFileSync(path.join(siteDir, 'reference.json'), `${JSON.stringify(model, null, 2)}\n`)

function escapeHtml(value) {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll('"', '&quot;')
}

function methodLink(name) {
  return `<a href="#rpc-${name.replaceAll('.', '-')}"><code>${name}</code></a>`
}

function codeBlock(label, value, language = 'json') {
  const highlighted = language
    ? hljs.highlight(value, { language }).value
    : escapeHtml(value)
  const className = language ? ` class="hljs language-${language}"` : ''
  return `<section class="example"><div class="example-head"><h3>${label}</h3><button class="copy" type="button">Copy</button></div><pre><code${className}>${highlighted}</code></pre></section>`
}

function cards(entries) {
  return entries.map(entry => {
    const examples = entry.kind === 'rpc'
      ? `${codeBlock('Request', entry.request)}${codeBlock('Result shape', entry.result)}`
      : codeBlock('Notification', entry.event)
    return `<article id="${entry.href.slice(1)}" data-entry="${escapeAttribute(entry.search)}"><div class="entry-info"><div class="eyebrow">${entry.kind === 'rpc' ? 'JSON-RPC method' : 'SSE notification'}</div><h2>${entry.name}</h2><p>${entry.summary}</p></div><div class="examples">${examples}</div></article>`
  }).join('\n')
}

function accountGuide() {
  const stateRefresh = model.accountTracking.state_refresh
  const frontier = model.accountTracking.frontier_confirmation
  const fallback = model.confirmationTracking.fallback
  return `<section class="task-guide" id="event-watch-account" data-entry="watch account transfer send receive confirmation balance receivable frontier reset replay process block_info accounts_balances account_info">
<div class="eyebrow">Account tracking</div>
<h2>Watch accounts and update local state</h2>
<p>Use this sequence to track a send or receive from submission to cementing. The stream reports cemented blocks only. It does not report votes or election progress.</p>
<div class="guide-grid">
<section><h3>Notification types</h3><ul><li><code>nano.confirmation</code> identifies a matching cemented block.</li><li><code>nano.stream_reset</code> means that the event cursor is no longer continuous. Reconcile before processing later events.</li><li>Use <code>params.hash</code> as the idempotency key. Store it and ignore duplicates.</li><li><code>params.subtype</code> identifies a state send or receive when the node provides it.</li></ul></section>
<section><h3>Stream limits</h3><ul><li>The stream does not emit aggregate balances, derived balance events, votes, or telemetry.</li><li>A client must handle a confirmation that was not delivered.</li><li>Balance responses use <code>receivable</code>, not a <code>pending</code> field.</li></ul></section>
</div>
<h3>Track a recipient before sending</h3>
<ol><li>Open <code>GET /events/confirmations?accounts=&lt;account&gt;[,&lt;account&gt;]</code>. Keep the connection open.</li><li>After the stream opens, call ${methodLink(stateRefresh.method)} once with the watched accounts and <code>include_only_confirmed: true</code>. Store each <code>result.balances[account]</code> value.</li><li>Submit the block with ${methodLink('process')}. Persist <code>result.hash</code> as the submitted block hash.</li><li>When <code>nano.confirmation.params.hash</code> equals that hash, mark the submission cemented. A recipient filter matches an incoming state send through <code>params.destination</code>.</li><li>After each matching confirmation, call ${methodLink(stateRefresh.method)} once for the complete watched set. Publish each returned <code>balance</code> and <code>receivable</code> value. This is event-triggered batch refresh, not polling.</li><li>Save the SSE <code>id:</code> value. Send it as <code>Last-Event-ID</code> when reconnecting.</li></ol>
${codeBlock('SSE request', 'GET https://<gateway>/events/confirmations?accounts=nano_...%2Cnano_...\nAccept: text/event-stream', null)}
${codeBlock('Batch state request', JSON.stringify({ jsonrpc: '2.0', method: stateRefresh.method, params: { accounts: ['nano_...', 'nano_...'], include_only_confirmed: true }, id: 1 }, null, 2))}
<h3>Read the batch balance result</h3>
<table><thead><tr><th>Path</th><th>Type</th><th>Meaning</th></tr></thead><tbody><tr><td><code>result.balances</code></td><td>object</td><td>One entry for each requested account. It is not a total.</td></tr><tr><td><code>result.balances[account].balance</code></td><td>string</td><td>Cemented account balance in RAW.</td></tr><tr><td><code>result.balances[account].receivable</code></td><td>string</td><td>Cemented incoming sends not yet received, in RAW.</td></tr><tr><td><code>result.errors[account]</code></td><td>string, optional</td><td>Native per-account validation error.</td></tr></tbody></table>
<p><code>params.block.balance</code> is the balance of <code>params.account</code> after that block. For a send addressed to a watched recipient, it is the sender balance. Use ${methodLink(stateRefresh.method)} for the full balance and receivable state of the recipient.</p>
<h3>Handle a missing notification or reset</h3>
<ol><li>When a submitted hash has no confirmation after about ${fallback.suggested_delay_seconds} seconds, call ${methodLink(fallback.method)} with that hash.</li><li>Read <code>${fallback.confirmed_field}</code>. A value of <code>true</code> means the block is cemented.</li><li>When <code>nano.stream_reset</code> arrives, refresh all watched accounts with ${methodLink(stateRefresh.method)} before applying new confirmations.</li><li>Use ${methodLink(fallback.method)} for each submitted hash whose terminal state remains unknown.</li></ol>
<h3>Check the current account frontier</h3>
<p>Use this sequence when you have an account but no submitted block hash. It checks whether the current frontier for that account is cemented.</p>
<ol><li>Call ${methodLink(frontier.method)} for the account.</li><li>If <code>result.opened</code> is <code>false</code>, the account has no frontier.</li><li>Read <code>${frontier.current_frontier_field}</code>. This is the current frontier and can be ahead of cemented state.</li><li>Call ${methodLink(frontier.status_method)} with that hash. Read <code>${frontier.status_field}</code>.</li><li>Use <code>${frontier.cemented_frontier_field}</code> and <code>result.confirmed_balance</code> when you need the last known cemented account state.</li></ol>
<p class="source-note">The fallback delay and frontier sequence follow <a href="https://docs.nano.org/integration-guides/block-confirmation-tracking/#block-confirmation-tracking">the Nano confirmation-tracking guidance</a>. The gateway exposes the methods linked above.</p>
</section>`
}

const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="Nano Gateway RPC and event contracts"><title>Nano Gateway Reference</title><link rel="stylesheet" href="assets/reference.css"></head>
<body><header><a class="brand" href="#top"><span class="mark">N</span><span>Nano Gateway<small>${model.profile}</small></span></a><nav><a href="openrpc.json">OpenRPC JSON</a><a href="asyncapi.json">AsyncAPI JSON</a></nav></header>
<main id="top"><section class="hero"><p class="kicker">Nano Gateway API</p><h1>Query accounts and track confirmations</h1><p>Use Nano RPC to read ledger state or submit a block. Use the SSE stream to watch account activity and refresh balances when a block is cemented.</p><label class="search"><span>Search methods, events, and fields</span><input id="search" type="search" placeholder="Try accounts_balances or confirmed_frontier" autocomplete="off"></label></section>
<div class="tabs" role="tablist" aria-label="Reference section"><button class="tab active" data-tab="rpc" role="tab" aria-controls="rpc" aria-selected="true">RPC <span>${rpc.length}</span></button><button class="tab" data-tab="events" role="tab" aria-controls="events" aria-selected="false">Events <span>${events.length}</span></button></div>
<section id="rpc" class="panel active" role="tabpanel"><div class="intro"><h2>Nano RPC methods</h2><p>Send JSON-RPC requests to the gateway. Each entry shows an example request and the JSON result shape. Values in the result shape are type placeholders.</p></div>${cards(rpc)}</section>
<section id="events" class="panel" role="tabpanel"><div class="intro"><h2>Account confirmation stream</h2><p>Open <code>GET /events/confirmations</code> and filter by accounts or hashes. Each SSE record contains a JSON-RPC notification. Send <code>Last-Event-ID</code> when reconnecting.</p></div>${accountGuide()}${cards(events)}</section>
<p id="empty" hidden>No contract entries match that search.</p></main><script src="assets/reference.js"></script></body></html>`

const css = `:root{color-scheme:dark;--ink:#eaf4ff;--muted:#91a5bb;--line:#24394e;--accent:#63e6be;--blue:#74c0fc;--panel:#101d2a}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(circle at 80% 0,#173a4d 0,transparent 32rem),#08121c;color:var(--ink);font:16px/1.55 Inter,ui-sans-serif,system-ui,sans-serif}header{height:72px;padding:0 max(24px,calc((100vw - 1120px)/2));display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line);position:sticky;top:0;background:#08121ce8;backdrop-filter:blur(14px);z-index:2}.brand{display:flex;gap:12px;align-items:center;color:inherit;text-decoration:none;font-weight:700}.brand small{display:block;color:var(--muted);font-weight:500}.mark{display:grid;place-items:center;width:36px;height:36px;border-radius:10px;background:var(--accent);color:#062018}nav{display:flex;gap:20px}a{color:var(--blue)}nav a{color:var(--muted)}main{max-width:1120px;margin:auto;padding:80px 24px}.hero{max-width:800px}.kicker,.eyebrow{color:var(--accent);text-transform:uppercase;letter-spacing:.12em;font-size:.74rem;font-weight:800}.hero h1{font-size:clamp(2.6rem,7vw,5.4rem);line-height:.95;letter-spacing:-.055em;margin:.2em 0}.hero>p:not(.kicker){color:var(--muted);font-size:1.18rem;max-width:680px}.search{display:block;margin:38px 0}.search span{display:block;color:var(--muted);font-size:.8rem;margin-bottom:8px}.search input{width:100%;padding:16px 18px;border:1px solid var(--line);border-radius:12px;background:#0c1925;color:var(--ink);font:inherit;outline:none}.search input:focus{border-color:var(--blue);box-shadow:0 0 0 3px #74c0fc22}.tabs{display:flex;gap:8px;border-bottom:1px solid var(--line);margin-top:30px}.tab{padding:14px 18px;border:0;border-bottom:2px solid transparent;background:none;color:var(--muted);font:inherit;font-weight:700;cursor:pointer}.tab.active{color:var(--ink);border-color:var(--accent)}.tab span{padding:2px 7px;margin-left:5px;border-radius:99px;background:var(--line);font-size:.75rem}.panel{display:none}.panel.active{display:block}.intro{padding:44px 0 24px;max-width:780px}.intro p{color:var(--muted)}code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.92em}.intro code{display:inline-block;margin:4px 5px 0 0;padding:5px 9px;background:#162638;border-radius:6px;color:var(--blue)}article,.task-guide{scroll-margin-top:90px}.task-guide{padding:34px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(135deg,#112536,#0c1824);margin:12px 0 40px}.task-guide h2{margin:.2rem 0;font-size:clamp(1.75rem,4vw,2.4rem);letter-spacing:-.035em}.task-guide>p{max-width:760px;color:var(--muted)}.task-guide h3{margin-top:32px}.guide-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.guide-grid section{padding:18px;border:1px solid var(--line);border-radius:12px;background:#091520}.guide-grid h3{margin:0 0 8px;font-size:1rem}.guide-grid ul{padding-left:20px;margin:0}.task-guide ol{padding-left:24px;max-width:860px}.task-guide li{margin:10px 0}.task-guide table{width:100%;border-collapse:collapse;margin:16px 0;background:#091520}.task-guide th,.task-guide td{padding:11px 13px;border:1px solid var(--line);text-align:left;vertical-align:top}.task-guide th{color:var(--accent);font-size:.78rem;text-transform:uppercase;letter-spacing:.07em}.source-note{font-size:.92rem}article{display:grid;grid-template-columns:minmax(220px,1fr) minmax(320px,1.4fr);gap:8px 36px;padding:40px 0;border-top:1px solid var(--line)}article h2{font:700 1.55rem ui-monospace,monospace;margin:.2rem 0}article>p{color:var(--muted);grid-column:1}.examples{grid-column:2;grid-row:1/4;display:grid;gap:12px}.example{position:relative}.example-head{display:flex;justify-content:space-between;align-items:center;margin:0 0 6px}.example h3{margin:0;color:var(--muted);font-size:.78rem;text-transform:uppercase;letter-spacing:.08em}.copy{border:1px solid var(--line);border-radius:7px;padding:6px 9px;background:#142436;color:var(--ink);cursor:pointer}pre{margin:0;padding:22px;overflow:auto;border:1px solid var(--line);border-radius:12px;background:#071019;color:#b9e7ff;font-size:.83rem}#empty{padding:44px 0;color:var(--muted)}@media(max-width:720px){header{height:auto;padding:16px 20px;align-items:flex-start}nav{flex-direction:column;gap:2px;text-align:right;font-size:.85rem}main{padding:52px 20px}.hero h1{font-size:3.2rem}.task-guide{padding:22px}.guide-grid{grid-template-columns:1fr}.task-guide table{display:block;overflow:auto}.task-guide th,.task-guide td{min-width:145px}article{grid-template-columns:1fr}.examples{grid-column:1;grid-row:auto}}\n`

const js = `const tabs=[...document.querySelectorAll('.tab')],panels=[...document.querySelectorAll('.panel')],entries=[...document.querySelectorAll('[data-entry]')],search=document.querySelector('#search'),empty=document.querySelector('#empty');function activate(kind){tabs.forEach(tab=>{const active=tab.dataset.tab===kind;tab.classList.toggle('active',active);tab.setAttribute('aria-selected',String(active))});panels.forEach(panel=>panel.classList.toggle('active',panel.id===kind))}function categoryHash(kind){return kind==='events'?'#events':'#rpc'}function activateFromHash(){const eventTarget=location.hash==='#events'||location.hash.startsWith('#event-');activate(eventTarget?'events':'rpc')}tabs.forEach(tab=>tab.addEventListener('click',()=>{const hash=categoryHash(tab.dataset.tab);activate(tab.dataset.tab);if(location.hash!==hash)location.hash=hash}));window.addEventListener('hashchange',activateFromHash);activateFromHash();setTimeout(activateFromHash,0);function filter(){const query=search.value.trim().toLowerCase();let count=0;entries.forEach(entry=>{const show=!query||entry.dataset.entry.toLowerCase().includes(query);entry.hidden=!show;if(show)count++});empty.hidden=count>0;if(query){const kinds=new Set(entries.filter(entry=>!entry.hidden).map(entry=>entry.closest('.panel').id));if(kinds.size===1)activate([...kinds][0])}}search.addEventListener('input',filter);document.querySelectorAll('.copy').forEach(button=>button.addEventListener('click',async()=>{const code=button.closest('.example').querySelector('code');await navigator.clipboard.writeText(code.innerText);button.textContent='Copied';setTimeout(()=>button.textContent='Copy',1200)}));\n`

fs.mkdirSync(path.join(siteDir, 'assets'), { recursive: true })
const fontDir = path.join(siteDir, 'assets', 'fonts')
fs.mkdirSync(fontDir, { recursive: true })
for (const [name, source] of fontSources) {
  fs.copyFileSync(source, path.join(fontDir, name))
}
fs.writeFileSync(path.join(siteDir, 'index.html'), `${html}\n`)
const paletteCss = `
:root {
  color-scheme: light;
  --ink: #20214f;
  --muted: #4a5866;
  --line: #d3dae3;
  --accent: #299be3;
  --blue: #238ed4;
  --panel: #fbfcfe;
  --surface: #dce3ea;
  --soft: #edf1f5;
  --mint: #38d6d5;
  --pink: #ed5478;
  --yellow: #ffd426;
  --dark: #1d2730;
}
@font-face { font-family: 'Montserrat'; src: url('fonts/montserrat-latin-wght-normal.woff2') format('woff2'); font-style: normal; font-weight: 100 900; font-display: swap; }
@font-face { font-family: 'Open Sans'; src: url('fonts/open-sans-latin-wght-normal.woff2') format('woff2'); font-style: normal; font-weight: 300 800; font-display: swap; }
body { background: #cfd8e2; color: var(--ink); }
body, .copy { font-family: 'Open Sans', ui-sans-serif, system-ui, sans-serif; }
h1, h2, h3, .brand, .tab, .kicker, .eyebrow, .search span { font-family: 'Montserrat', ui-sans-serif, system-ui, sans-serif; }
header { background: #20214fee; color: #ffffff; border-color: #40457a; }
.brand small, nav a { color: #d6e0eb; }
.mark { background: var(--accent); color: #ffffff; }
a { color: var(--blue); }
.kicker, .eyebrow { color: var(--pink); }
.hero { max-width: none; padding: 40px; border-radius: 16px; background: linear-gradient(135deg, #20214f, #303b78); box-shadow: 0 8px 22px #20214f2b; }
.hero h1 { color: #ffffff; max-width: 760px; }
.hero > p:not(.kicker) { color: #d6e0eb; }
.search { margin-bottom: 0; }
.search span { color: #d6e0eb; }
.search input { background: var(--panel); color: var(--ink); box-shadow: 0 2px 5px #20214f2b; }
.search input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px #299be333; }
.tabs { margin-top: 20px; padding: 0 16px; border: 1px solid #bcc8d4; border-bottom: 0; border-radius: 14px 14px 0 0; background: var(--panel); }
.tab span { background: var(--soft); }
.tab.active { background: #eef5fb; }
.panel { padding: 0 20px 24px; border: 1px solid #bcc8d4; border-top: 0; border-radius: 0 0 14px 14px; background: #eef2f6; }
.intro { padding: 30px 8px 22px; border-bottom: 1px solid #cbd5df; }
.intro code { background: #dceffb; color: var(--ink); }
.task-guide { border-color: #9cd9d8; background: #ffffff; box-shadow: 0 3px 10px #20214f16; }
.guide-grid section { background: #f1f4f7; }
.task-guide table { background: var(--panel); }
.task-guide th { color: var(--ink); background: #fff3bf; }
.copy { background: var(--panel); color: var(--ink); box-shadow: 0 1px 3px #20214f12; }
.copy:hover { border-color: var(--accent); color: var(--blue); }
pre { border-color: #303b78; background: var(--dark); color: var(--surface); box-shadow: 0 3px 10px #20214f18; }
.hljs-attr, .hljs-property { color: #74c0fc; }
.hljs-string { color: #63e6be; }
.hljs-number, .hljs-literal { color: #ffd426; }
.hljs-punctuation { color: #d6e0eb; }
article, .examples, .example, pre { min-width: 0; }
article { grid-template-columns: minmax(210px, .75fr) minmax(0, 1.5fr); align-items: start; gap: 20px 30px; margin: 16px 0; padding: 28px; border: 1px solid #c2ccd7; border-radius: 14px; background: var(--panel); box-shadow: 0 2px 7px #20214f12; }
article h2 { margin: .3rem 0 1rem; font-family: 'Montserrat', ui-sans-serif, system-ui, sans-serif; }
.entry-info p { margin: 0; color: var(--muted); }
.examples { grid-column: auto; grid-row: auto; }
@media(max-width:720px) { main { padding: 32px 16px 48px; border: 0; } .hero { padding: 28px 20px; } .tabs { padding: 0 4px; } .panel { padding: 0 12px 16px; } article { grid-template-columns: 1fr; padding: 20px; gap: 18px; } }
`
fs.writeFileSync(path.join(siteDir, 'assets/reference.css'), css + paletteCss)
fs.writeFileSync(path.join(siteDir, 'assets/reference.js'), js)
