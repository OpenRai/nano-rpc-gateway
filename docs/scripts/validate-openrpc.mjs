import fs from 'node:fs'
import Ajv from 'ajv'
import metaSchema from '@open-rpc/meta-schema'

const document = JSON.parse(fs.readFileSync(new URL('../api-docs/openrpc.json', import.meta.url)))
const ajv = new Ajv({ strict: false })
if (!ajv.validate(metaSchema, document)) {
  console.error(ajv.errorsText(ajv.errors, { separator: '\n' }))
  process.exit(1)
}
