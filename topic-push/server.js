"use strict"

const http = require("node:http")

const PORT = Number(process.env.PORT || 8090)
const MAX_EVENTS_PER_SESSION = Number(process.env.MAX_EVENTS_PER_SESSION || 500)
const MAX_SESSIONS = Number(process.env.MAX_SESSIONS || 50)
const MAX_CONTENT = 2000

/** sessionId -> { session_id, first_seen, last_seen, events: Map(key -> event) } */
const sessions = new Map()
/** active SSE responses */
const clients = new Set()

function log(message, extra) {
  if (extra === undefined) console.log(`[topic-push] ${message}`)
  else console.log(`[topic-push] ${message}`, extra)
}

function isoOrNow(value) {
  if (value === null || value === undefined || value === "") return new Date().toISOString()
  if (typeof value === "number") {
    const ms = value > 1e11 ? value : value * 1000
    const date = new Date(ms)
    return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString()
  }
  const date = new Date(String(value))
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString()
}

function pick(row, ...names) {
  for (const name of names) {
    if (row[name] !== undefined && row[name] !== null) return row[name]
  }
  return undefined
}

/** Accepts a single row, an array of rows, a Debezium-ish envelope or NDJSON. */
function extractRows(payload) {
  const rows = []
  const push = (value) => {
    if (value && typeof value === "object" && !Array.isArray(value)) rows.push(value)
  }

  if (Array.isArray(payload)) {
    payload.forEach(push)
    return rows
  }
  if (!payload || typeof payload !== "object") return rows

  const after = pick(payload, "after")
  if (after && typeof after === "object") {
    push(after)
    return rows
  }
  const data = pick(payload, "data", "payload", "record")
  if (data && typeof data === "object") {
    push(data)
    return rows
  }
  push(payload)
  return rows
}

function parseBody(raw) {
  const text = raw.toString("utf8").trim()
  if (!text) return []
  try {
    return extractRows(JSON.parse(text))
  } catch {
    const rows = []
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        rows.push(...extractRows(JSON.parse(trimmed)))
      } catch {
        /* ignore malformed line */
      }
    }
    return rows
  }
}

function pruneSession(session) {
  if (session.events.size <= MAX_EVENTS_PER_SESSION) return
  const keys = Array.from(session.events.keys()).sort(
    (a, b) => session.events.get(a).message_index - session.events.get(b).message_index,
  )
  while (session.events.size > MAX_EVENTS_PER_SESSION) {
    session.events.delete(keys.shift())
  }
}

function pruneSessions() {
  if (sessions.size <= MAX_SESSIONS) return
  const ordered = Array.from(sessions.values()).sort((a, b) =>
    a.last_seen < b.last_seen ? 1 : -1,
  )
  for (const session of ordered.slice(MAX_SESSIONS)) {
    sessions.delete(session.session_id)
  }
}

function ingest(row) {
  const sessionId = String(pick(row, "session_id", "sessionId") || "unknown")
  const topic = String(pick(row, "topic", "sujet") || "").trim()
  if (!topic) return null

  const rawIndex = Number(pick(row, "message_index", "messageIndex"))
  const messageIndex = Number.isFinite(rawIndex) ? rawIndex : 0
  const rawNode = Number(pick(row, "decision_node_id", "decisionNodeId"))

  const event = {
    session_id: sessionId,
    timestamp: isoOrNow(pick(row, "timestamp", "time")),
    topic,
    message_index: messageIndex,
    decision_node_id: Number.isFinite(rawNode) ? rawNode : 0,
    content: String(pick(row, "content", "text") || "").slice(0, MAX_CONTENT),
    received_at: new Date().toISOString(),
  }

  let session = sessions.get(sessionId)
  if (!session) {
    session = {
      session_id: sessionId,
      first_seen: event.timestamp,
      last_seen: event.timestamp,
      events: new Map(),
    }
    sessions.set(sessionId, session)
  }

  const key = `${sessionId}:${messageIndex}`
  const existing = session.events.get(key)
  if (existing) {
    if (existing.topic !== event.topic) {
      existing.topic = event.topic
      existing.content = event.content
      existing.timestamp = event.timestamp
    }
  } else {
    session.events.set(key, event)
  }

  if (event.timestamp > session.last_seen) session.last_seen = event.timestamp
  if (event.timestamp < session.first_seen) session.first_seen = event.timestamp

  pruneSession(session)
  pruneSessions()
  return event
}

function snapshot() {
  return Array.from(sessions.values())
    .map((session) => ({
      session_id: session.session_id,
      first_seen: session.first_seen,
      last_seen: session.last_seen,
      events: Array.from(session.events.values()).sort(
        (a, b) => a.message_index - b.message_index,
      ),
    }))
    .sort((a, b) => (a.last_seen < b.last_seen ? 1 : -1))
}

function broadcast(payload) {
  const frame = `data: ${JSON.stringify(payload)}\n\n`
  for (const client of clients) {
    try {
      client.write(frame)
    } catch {
      clients.delete(client)
    }
  }
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body)
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(payload),
    "Cache-Control": "no-store",
  })
  res.end(payload)
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`)
  const path = url.pathname

  if (req.method === "GET" && path === "/healthz") {
    return sendJson(res, 200, {
      ok: true,
      sessions: sessions.size,
      clients: clients.size,
    })
  }

  if (req.method === "GET" && path === "/sessions") {
    return sendJson(res, 200, { sessions: snapshot() })
  }

  if (req.method === "GET" && path === "/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    })
    res.write("retry: 2000\n\n")
    res.write(`data: ${JSON.stringify({ type: "snapshot", sessions: snapshot() })}\n\n`)
    clients.add(res)
    const ping = setInterval(() => {
      try {
        res.write(": ping\n\n")
      } catch {
        /* ignore */
      }
    }, 15000)
    req.on("close", () => {
      clearInterval(ping)
      clients.delete(res)
    })
    log(`sse client connected (${clients.size} total)`)
    return
  }

  if (req.method === "POST" && path === "/ingest") {
    const chunks = []
    req.on("data", (chunk) => chunks.push(chunk))
    req.on("end", () => {
      const rows = parseBody(Buffer.concat(chunks))
      let accepted = 0
      for (const row of rows) {
        const event = ingest(row)
        if (!event) continue
        accepted += 1
        broadcast({ type: "topic", event })
      }
      sendJson(res, 202, { accepted, received: rows.length })
    })
    return
  }

  sendJson(res, 404, { error: "not found" })
})

server.keepAliveTimeout = 65000
server.headersTimeout = 66000

server.listen(PORT, "0.0.0.0", () => {
  log(`listening on :${PORT}`)
})
