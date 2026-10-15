import fs from 'node:fs'
import path from 'node:path'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const repoDir = path.resolve(docsDir, '..')
// Both the served reference and the published artifacts under generated/ are
// reproduced by `npm run generate`, so a stale copy in either is a failure.
const generatedRoots = [path.join(docsDir, 'api-docs'), path.join(repoDir, 'generated')]
const before = snapshot(generatedRoots)
execFileSync('npm', ['run', 'generate'], { cwd: docsDir, stdio: 'inherit' })
const after = snapshot(generatedRoots)
if (before !== after) {
  console.error('Generated documentation or client is stale; run make docs-generate')
  process.exit(1)
}

const page = fs.readFileSync(path.join(docsDir, 'api-docs', 'index.html'), 'utf8')
const stylesheet = fs.readFileSync(path.join(docsDir, 'api-docs', 'assets', 'reference.css'), 'utf8')
for (const required of [
  'Watch accounts and update local state',
  'Check the current account frontier',
  'href="#rpc-accounts_balances"',
  'href="#rpc-account_info"',
  'href="#rpc-block_info"',
  'href="#rpc-accounts_frontiers"',
  'href="#rpc-accounts_receivable"',
  'href="#rpc-block_confirm"',
  'POST /events/confirmations',
  'There is no replay and no cursor',
  'Result shape',
  'It does not report votes or election progress.',
  'OpenRPC Playground',
  'Gateway JSON-RPC 2.0 API and SSE',
  '<title>Nano Gateway JSON-RPC 2.0 API Reference</title>',
  '<h1>JSON-RPC 2.0 for Nano ledger queries</h1>',
  'Gateway JSON-RPC 2.0 methods',
  "Nano node's custom JSON RPC interface",
  'JSON-RPC 2.0 requests to its API',
  'AsyncAPI Studio',
  'href="/docs/openrpc/"',
  'href="/docs/asyncapi/"',
  'href="/openrpc.json"',
  'href="/asyncapi.json"'
]) {
  if (!page.includes(required)) {
    console.error(`Generated documentation is missing required account-tracking guidance: ${required}`)
    process.exit(1)
  }
}
for (const stale of ['Nano RPC methods', 'Use Nano RPC', 'RPC tool']) {
  if (page.includes(stale)) {
    console.error(`Generated documentation uses ambiguous gateway terminology: ${stale}`)
    process.exit(1)
  }
}

for (const font of ['montserrat-latin-wght-normal.woff2', 'open-sans-latin-wght-normal.woff2']) {
  if (!fs.existsSync(path.join(docsDir, 'api-docs', 'assets', 'fonts', font))) {
    console.error(`Generated documentation is missing self-hosted font: ${font}`)
    process.exit(1)
  }
}
if (!stylesheet.includes("font-family: 'Montserrat'") || !stylesheet.includes("font-family: 'Open Sans'")) {
  console.error('Generated documentation is missing the Nano typography families')
  process.exit(1)
}

const openrpc = JSON.parse(fs.readFileSync(path.join(docsDir, 'api-docs', 'openrpc.json')))
const asyncapi = JSON.parse(fs.readFileSync(path.join(docsDir, 'api-docs', 'asyncapi.json')))
const ids = [...page.matchAll(/\sid="([^"]+)"/g)].map(match => match[1])
if (new Set(ids).size !== ids.length) {
  console.error('Generated documentation contains duplicate deep-link targets')
  process.exit(1)
}

const anchors = [
  'rpc',
  'events',
  'event-watch-account',
  ...openrpc.methods.map(method => `rpc-${method.name.replaceAll('.', '-')}`),
  ...Object.values(asyncapi.components.messages).map(message => `event-${message.name.replaceAll('.', '-')}`)
]
for (const anchor of anchors) {
  if (!ids.includes(anchor)) {
    console.error(`Generated documentation is missing deep-link target: #${anchor}`)
    process.exit(1)
  }
}

function snapshot(roots) {
  return roots
    .flatMap(root =>
      fs.readdirSync(root, { recursive: true })
        .filter(name => fs.statSync(path.join(root, name)).isFile())
        .sort()
        .map(name => `${path.basename(root)}/${name}\0${fs.readFileSync(path.join(root, name), 'base64')}`)
    )
    .join('\n')
}
