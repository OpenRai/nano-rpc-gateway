import fs from 'node:fs'
import { Parser } from '@asyncapi/parser'

const source = fs.readFileSync(new URL('../api-docs/asyncapi.json', import.meta.url), 'utf8')
const parser = new Parser()
const diagnostics = await parser.validate(source)
const errors = diagnostics.filter(diagnostic => {
  const severity = String(diagnostic.severity ?? diagnostic.type ?? '').toLowerCase()
  return severity === 'error' || severity === 'fatal'
})
if (errors.length > 0) {
  console.error(JSON.stringify(errors, null, 2))
  process.exit(1)
}
