import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'api-docs')
const port = Number(process.env.PORT ?? 8080)

const server = http.createServer(function serve(request, response) {
  const requestPath = decodeURIComponent((request.url ?? '/').split('?')[0])
  const relativePath = requestPath === '/' ? 'index.html' : requestPath.replace(/^\/+/, '')
  const filePath = path.resolve(root, relativePath)
  if (filePath !== root && !filePath.startsWith(`${root}${path.sep}`)) {
    response.writeHead(403).end('Forbidden')
    return
  }
  fs.stat(filePath, function handleFile(error, stats) {
    if (error || !stats.isFile()) {
      response.writeHead(404).end('Not found')
      return
    }
    response.writeHead(200, { 'cache-control': 'no-store' })
    fs.createReadStream(filePath).pipe(response)
  })
})

server.listen(port, '127.0.0.1', function announce() {
  console.log(`Docs preview: http://127.0.0.1:${port}/`)
})
