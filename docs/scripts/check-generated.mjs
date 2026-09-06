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

function snapshot(root) {
  return fs.readdirSync(root, { recursive: true })
    .filter(name => fs.statSync(path.join(root, name)).isFile())
    .sort()
    .map(name => `${name}\0${fs.readFileSync(path.join(root, name), 'base64')}`)
    .join('\n')
}
