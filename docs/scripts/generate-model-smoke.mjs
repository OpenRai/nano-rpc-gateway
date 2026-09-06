import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { TypeScriptGenerator } from '@asyncapi/modelina'

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const outputDir = path.join(docsDir, '.model-smoke')
const document = JSON.parse(fs.readFileSync(path.join(docsDir, 'api-docs/asyncapi.json'), 'utf8'))
const generator = new TypeScriptGenerator({ modelType: 'interface' })
const models = await generator.generate(document)
if (models.length === 0) {
  throw new Error('AsyncAPI model generation produced no models')
}
fs.mkdirSync(outputDir, { recursive: true })
for (const model of models) {
  fs.writeFileSync(path.join(outputDir, `${model.modelName}.ts`), `${model.result}\n`)
}
