// Parity gate.
//
// The gateway may not add, omit or reshape anything the node does, except the six
// sanctioned translation-layer items and anything upstream itself deprecated. This
// check makes that testable rather than aspirational: it walks the generated
// OpenRPC and AsyncAPI documents and fails when anything they contain has no row in
// the parity matrix, when a row has no status from the allowed set, or when a row
// and the schema disagree about whether something is exposed.
//
// The check runs in both directions. A schema element with no matrix row is an
// undeclared addition. A matrix row marked present that the schema does not contain
// is a stale or broken declaration. Both are failures, because either one means the
// published contract and the reviewed matrix have drifted apart.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const docsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const repoDir = path.resolve(docsDir, '..')
const openrpc = JSON.parse(fs.readFileSync(path.join(docsDir, 'api-docs', 'openrpc.json')))
const asyncapi = JSON.parse(fs.readFileSync(path.join(docsDir, 'api-docs', 'asyncapi.json')))
const matrix = JSON.parse(fs.readFileSync(path.join(repoDir, 'schema', 'parity-matrix.json')))

const problems = []
const ALLOWED = new Set(['present', 'omitted-deprecated', 'translation-layer', 'not-implemented'])
const items = new Set(Object.keys(matrix._meta.translation_layer_items))

function status(where, entry) {
  const value = entry?.status
  if (!ALLOWED.has(value)) {
    problems.push(`${where}: status "${value}" is not one of ${[...ALLOWED].join(', ')}`)
    return null
  }
  if (value === 'translation-layer' && !items.has(String(entry.item))) {
    problems.push(`${where}: translation-layer entry does not name one of the six sanctioned items`)
    return null
  }
  if ((value === 'not-implemented' || value === 'omitted-deprecated') && !entry.note) {
    problems.push(`${where}: a ${value} row must carry a note explaining itself`)
    return null
  }
  if (value !== 'present' && !entry.anchor) {
    problems.push(`${where}: a ${value} row must cite an anchor in the upstream documentation`)
    return null
  }
  return value
}

// ---------------------------------------------------------------- methods

const declaredMethods = new Set()
for (const method of openrpc.methods) {
  const row = matrix.methods[method.name]
  if (!row) {
    problems.push(`method "${method.name}" is exposed but has no parity-matrix row`)
    continue
  }
  declaredMethods.add(method.name)
  status(`method "${method.name}"`, row)

  const declaredParams = new Set(method.params.map(param => param.name))
  for (const [name, entry] of Object.entries(row.params ?? {})) {
    const paramStatus = status(`method "${method.name}" param "${name}"`, entry)
    if (paramStatus !== 'present') {
      problems.push(`method "${method.name}" param "${name}" is marked ${paramStatus} but is still exposed`)
    }
    if (!declaredParams.has(name)) {
      problems.push(`method "${method.name}" param "${name}" is marked present but the schema does not declare it`)
    }
  }
  for (const name of declaredParams) {
    if (!row.params?.[name]) {
      problems.push(`method "${method.name}" param "${name}" is exposed but has no parity-matrix row`)
    }
    if (row.params?.[name] && row.params[name].required !== undefined) {
      const param = method.params.find(candidate => candidate.name === name)
      if (param && param.required !== row.params[name].required) {
        problems.push(`method "${method.name}" param "${name}": matrix says required=${row.params[name].required}, schema says required=${param.required}`)
      }
    }
  }
}

for (const [name, entry] of Object.entries(matrix.methods)) {
  const value = status(`matrix method "${name}"`, entry)
  if (value === 'present' && !declaredMethods.has(name)) {
    problems.push(`matrix marks method "${name}" present, but the schema does not expose it`)
  }
}

// -------------------------------------------------------- response schemas

// A parameter schema is reached through the method inventory, because a method's
// params are inlined by name and structure rather than referenced. Result schemas
// and any nested schema are reached by $ref.
const inventory = JSON.stringify(openrpc.methods)
const schemaGraph = JSON.stringify(openrpc.components.schemas)
for (const name of Object.keys(openrpc.components.schemas)) {
  const ref = `#/components/schemas/${name}`
  const used = inventory.includes(`"#/components/schemas/${name}"`) ||
    openrpc.methods.some(method => method.result?.schema?.$ref === ref) ||
    schemaGraph.includes(`"#/components/schemas/${name}"`)
  if (!used) {
    problems.push(`component schema "${name}" is defined but nothing references it`)
  }
}

// ----------------------------------------------------------- stream events

const declaredEvents = new Set()
for (const message of Object.values(asyncapi.components.messages)) {
  const row = matrix.stream_events[message.name]
  if (!row) {
    problems.push(`event "${message.name}" is exposed but has no parity-matrix row`)
    continue
  }
  declaredEvents.add(message.name)
  status(`event "${message.name}"`, row)
}
for (const [name, entry] of Object.entries(matrix.stream_events)) {
  const value = status(`matrix event "${name}"`, entry)
  if (value === 'present' && !declaredEvents.has(name)) {
    problems.push(`matrix marks event "${name}" present, but the schema does not expose it`)
  }
}

// ------------------------------------------------------------ stream topics

const declaredTopics = new Set(['confirmation'])
for (const [name, entry] of Object.entries(matrix.stream_topics)) {
  const value = status(`topic "${name}"`, entry)
  if (value === 'present') declaredTopics.add(name)
  if (value === 'not-implemented') {
    if (declaredTopics.has(name)) {
      problems.push(`topic "${name}" is marked not-implemented but an event claims it`)
    }
    const messages = Object.values(asyncapi.components.messages).map(message => message.name)
    if (messages.some(message => message.includes(name))) {
      problems.push(`topic "${name}" is marked not-implemented but the AsyncAPI document mentions it`)
    }
  }
}
for (const name of declaredTopics) {
  if (!matrix.stream_topics[name]) {
    problems.push(`topic "${name}" is exposed but has no parity-matrix row`)
  }
}

// --------------------------------------------------------- omission records

for (const [name, entry] of Object.entries(matrix.omitted_actions ?? {})) {
  status(`omitted action "${name}"`, entry)
  if (declaredMethods.has(name)) {
    problems.push(`action "${name}" is recorded as omitted but the schema exposes it`)
  }
}
for (const [name, entry] of Object.entries(matrix.omitted_topics ?? {})) {
  status(`omitted topic "${name}"`, entry)
}

if (problems.length) {
  console.error('Schema parity check failed:')
  for (const problem of problems) console.error(`  - ${problem}`)
  process.exit(1)
}

const counts = {}
for (const entry of Object.values(matrix.stream_topics)) {
  counts[entry.status] = (counts[entry.status] ?? 0) + 1
}
console.log(
  `Schema parity check passed: ${Object.keys(matrix.methods).length} methods, ` +
  `${Object.keys(matrix.stream_events).length} events, ${Object.keys(matrix.stream_topics).length} topics ` +
  `(${counts['not-implemented'] ?? 0} topics recorded as not implemented).`
)
