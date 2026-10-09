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
//
// It covers request parameters and response fields. The result half resolves each
// method's result schema through a $ref, unions the branches of a oneOf, and reads
// an inline object otherwise, so a gateway-added or removed response field fails the
// same way a parameter would.

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

// Resolve a schema reference against the OpenRPC component schemas.
function resolveSchema(schema) {
  if (!schema) return undefined
  if (schema.$ref) {
    const name = schema.$ref.replace('#/components/schemas/', '')
    return openrpc.components.schemas[name]
  }
  return schema
}

// The component schema name a method's result points at, or null when the result is
// an inline object rather than a $ref. A matrix row names the same schema in its
// `result` field, so the two can be compared.
function resultSchemaName(method) {
  const ref = method.result?.schema?.$ref
  if (!ref) return null
  return ref.replace('#/components/schemas/', '')
}

// Top-level property names a method's result declares, or null when it declares no
// fixed shape (an open object such as rpc.discover's). Follows a $ref, unions the
// branches of a oneOf, and reads an inline object otherwise.
function resultProperties(method) {
  const names = new Set()
  const root = resolveSchema(method.result?.schema)
  const branches = root?.oneOf ? root.oneOf : [root]
  for (const branch of branches) {
    const resolved = resolveSchema(branch)
    if (resolved?.properties) {
      for (const name of Object.keys(resolved.properties)) names.add(name)
    }
  }
  return names.size ? names : null
}

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

  // A row that names a result schema must name the one the document actually points
  // at. Otherwise a stale `result` entry reads as reviewed while the response
  // contract it claims to describe is a different schema.
  const resultName = resultSchemaName(method)
  if (row.result !== undefined && resultName !== null && row.result !== resultName) {
    problems.push(
      `method "${method.name}": matrix names result schema "${row.result}", but the document references "${resultName}"`
    )
  }

  const declaredFields = resultProperties(method)
  // Supported methods need an inspectable response contract. Open-ended results
  // remain allowed only when the matrix explicitly assigns a non-present status.
  if (!declaredFields && row.status === 'present') {
    problems.push(`method "${method.name}" is present but its result schema has no checkable properties`)
  }
  if (declaredFields) {
    for (const [name, entry] of Object.entries(row.result_fields ?? {})) {
      const fieldStatus = status(`method "${method.name}" result field "${name}"`, entry)
      if (fieldStatus !== 'present') {
        problems.push(`method "${method.name}" result field "${name}" is marked ${fieldStatus} but is still exposed`)
      }
      if (!declaredFields.has(name)) {
        problems.push(`method "${method.name}" result field "${name}" is marked present but the schema does not declare it`)
      }
    }
    for (const name of declaredFields) {
      if (!row.result_fields?.[name]) {
        problems.push(`method "${method.name}" result field "${name}" is exposed but has no parity-matrix row`)
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

// ------------------------------------------------------------ error refs

// The meta-schema validates an error definition by shape and a `$ref` as a string,
// so neither asks whether the two names agree. `components.errors` is a
// patternProperties map with no `required` list, which means a misspelled key
// holding a well-formed error validates, and `{"$ref": ...}` validates as long as
// it is a string. A dangling pointer therefore passes schema validation while
// documenting an error a reader cannot reach, so resolve every ref here instead.
//
// This check is load-bearing rather than defensive: it is what fails the shape the
// gateway previously published, where the definitions sat in a top-level `errors`
// key that the document schema does not allow and every method ref pointed at the
// empty `components.errors` namespace instead.
const errorComponents = openrpc.components.errors
if (!errorComponents || Object.keys(errorComponents).length === 0) {
  problems.push('the document declares no components.errors, so no method error ref can resolve')
}

const PREAMBLE = '#/components/errors/'
for (const method of openrpc.methods) {
  for (const error of method.errors ?? []) {
    const ref = error?.$ref
    if (!ref) continue
    if (!ref.startsWith(PREAMBLE)) {
      problems.push(`method "${method.name}": error ref "${ref}" is outside components/errors`)
      continue
    }
    const name = ref.slice(PREAMBLE.length)
    if (!errorComponents?.[name]) {
      const defined = Object.keys(errorComponents ?? {}).sort().join(', ')
      problems.push(
        `method "${method.name}": error ref "${ref}" is dangling; ` +
          `components.errors defines ${defined || '(nothing)'}`
      )
    }
  }
}

// A definition no method can reach is dead weight in a published contract, the
// error-definition counterpart of the unused-schema check above.
for (const name of Object.keys(errorComponents ?? {})) {
  const used = openrpc.methods.some(method =>
    (method.errors ?? []).some(error => error?.$ref === `${PREAMBLE}${name}`)
  )
  if (!used) {
    problems.push(`error "${name}" is defined but no method references it`)
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
