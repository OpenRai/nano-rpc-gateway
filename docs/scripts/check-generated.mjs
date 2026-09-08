import fs from 'node:fs'
import path from 'node:path'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const before = snapshot(path.join(docsDir, 'api-docs'))
execFileSync('npm', ['run', 'generate'], { cwd: docsDir, stdio: 'inherit' })
const after = snapshot(path.join(docsDir, 'api-docs'))
if (before !== after) {
  console.error('Generated documentation is stale; run make docs-generate')
  process.exit(1)
}

const page = fs.readFileSync(path.join(docsDir, 'api-docs', 'index.html'), 'utf8')
for (const required of [
  'Watch accounts and update local state',
  'Check the current account frontier',
  'href="#rpc-accounts_balances"',
  'href="#rpc-account_info"',
  'href="#rpc-block_info"',
  'Result shape',
  'It does not report votes or election progress.'
]) {
  if (!page.includes(required)) {
    console.error(`Generated documentation is missing required account-tracking guidance: ${required}`)
    process.exit(1)
  }
}

function snapshot(root) {
  return fs.readdirSync(root, { recursive: true })
    .filter(name => fs.statSync(path.join(root, name)).isFile())
    .sort()
    .map(name => `${name}\0${fs.readFileSync(path.join(root, name), 'base64')}`)
    .join('\n')
}
