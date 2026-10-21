import parquet from "parquetjs-lite"
import { mkdirSync, unlinkSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { randomUUID } from "node:crypto"

export const BUCKET = "sessions"

export const SCHEMA = new parquet.ParquetSchema({
  session_id: { type: "UTF8" },
  timestamp: { type: "TIMESTAMP_MILLIS" },
  role: { type: "UTF8" },
  content: { type: "UTF8" },
  repository: { type: "UTF8" },
  model: { type: "UTF8" },
  message_index: { type: "INT32" },
  decision_node_id: { type: "INT32", optional: true },
  decision_parent_id: { type: "INT32", optional: true },
  decision_node_type: { type: "UTF8", optional: true },
  decision_status: { type: "UTF8", optional: true },
  tool_name: { type: "UTF8", optional: true },
  tool_input: { type: "UTF8", optional: true },
  tokens_in: { type: "INT32", optional: true },
  tokens_out: { type: "INT32", optional: true },
  harness: { type: "UTF8", optional: true },
})

export const NODE_MESSAGE = "message"
export const NODE_STEP = "step"
export const NODE_REASONING = "reasoning"
export const NODE_TOOL = "tool"

function pad(n) {
  return String(n).padStart(2, "0")
}

export function generateSessionId(date = new Date()) {
  const d = date instanceof Date ? date : new Date(date)
  const stamp = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`
  const uuid = randomUUID().replace(/-/g, "").slice(0, 12)
  return `${stamp}-${uuid}`
}

export function objectKeyFor(sessionId, date = new Date()) {
  const d = date instanceof Date ? date : new Date(date)
  return `${d.getFullYear()}/${pad(d.getMonth() + 1)}/${pad(d.getDate())}/${sessionId}.parquet`
}

function summarize(text, max = 200) {
  const flat = String(text ?? "").replace(/\s+/g, " ").trim()
  return flat.length > max ? `${flat.slice(0, max)}…` : flat
}

function node({
  sessionId,
  timestamp,
  role,
  content,
  repository,
  model,
  messageIndex,
  nodeId,
  parentId,
  nodeType,
  status = "",
  tool = "",
  input = "",
  tokensIn = null,
  tokensOut = null,
  harness = "",
}) {
  return {
    session_id: sessionId,
    timestamp,
    role,
    content,
    repository,
    model,
    message_index: messageIndex,
    decision_node_id: nodeId,
    decision_parent_id: parentId,
    decision_node_type: nodeType,
    decision_status: status,
    tool_name: tool,
    tool_input: input,
    tokens_in: tokensIn,
    tokens_out: tokensOut,
    harness: harness,
  }
}

export function buildRows(rawMessages, { sessionId, repository = "", harness = "pi" } = {}) {
  const rows = []
  let messageIndex = 0
  let nextNodeId = 1
  let lastUserNodeId = 0

  for (const entry of rawMessages || []) {
    const info = entry?.info
    const parts = Array.isArray(entry?.parts) ? entry.parts : []
    if (!info) continue

    const role = info.role
    const created = info.time?.created ?? Date.now() / 1000
    const timestamp = new Date(created * 1000)
    const model = role === "assistant"
      ? `${info.providerID || ""}/${info.modelID || ""}`
      : ""

    const usage = info.usage || entry?.usage || {}
    const tokensIn = usage.prompt_tokens ?? usage.input_tokens ?? null
    const tokensOut = usage.completion_tokens ?? usage.output_tokens ?? null

    const textParts = parts
      .filter((p) => p.type === "text" && p.text)
      .map((p) => p.text)
    const content = textParts.join("\n")

    if (!content.trim() && parts.length === 0) continue

    const messageNodeId = nextNodeId++
    const parentId = role === "user" ? 0 : (lastUserNodeId || 0)
    rows.push(node({
      sessionId,
      timestamp,
      role,
      content: content.trim() || "",
      repository,
      model,
      messageIndex,
      nodeId: messageNodeId,
      parentId,
      nodeType: NODE_MESSAGE,
      tokensIn,
      tokensOut,
      harness,
    }))

    if (role === "user") lastUserNodeId = messageNodeId

    let stepNodeId = null
    for (const part of parts) {
      const type = part?.type
      if (type === "step-start") {
        stepNodeId = nextNodeId++
        rows.push(node({
          sessionId,
          timestamp,
          role,
          content: "",
          repository,
          model,
          messageIndex,
          nodeId: stepNodeId,
          parentId: messageNodeId,
          nodeType: NODE_STEP,
          status: "started",
          harness,
        }))
      } else if (type === "reasoning") {
        if (stepNodeId === null) {
          stepNodeId = nextNodeId++
          rows.push(node({
            sessionId,
            timestamp,
            role,
            content: "",
            repository,
            model,
            messageIndex,
            nodeId: stepNodeId,
            parentId: messageNodeId,
            nodeType: NODE_STEP,
            status: "considering",
            harness,
          }))
        }
        rows.push(node({
          sessionId,
          timestamp,
          role,
          content: summarize(part?.text, 500),
          repository,
          model,
          messageIndex,
          nodeId: nextNodeId++,
          parentId: stepNodeId,
          nodeType: NODE_REASONING,
          harness,
        }))
      } else if (type === "tool") {
        if (stepNodeId === null) {
          stepNodeId = nextNodeId++
          rows.push(node({
            sessionId,
            timestamp,
            role,
            content: "",
            repository,
            model,
            messageIndex,
            nodeId: stepNodeId,
            parentId: messageNodeId,
            nodeType: NODE_STEP,
            status: "acting",
            harness,
          }))
        }
        const state = part?.state || {}
        const toolInput = summarize(
          JSON.stringify(state.input ?? part?.input ?? ""),
          200,
        )
        rows.push(node({
          sessionId,
          timestamp,
          role,
          content: summarize(
            part?.tool || toolInput || "(outil)",
            120,
          ),
          repository,
          model,
          messageIndex,
          nodeId: nextNodeId++,
          parentId: stepNodeId,
          nodeType: NODE_TOOL,
          status: String(state.status || "running"),
          tool: String(part?.tool || ""),
          input: toolInput,
          harness,
        }))
      } else if (type === "step-finish") {
        if (stepNodeId !== null) {
          const step = rows.find((r) => r.decision_node_id === stepNodeId)
          if (step) step.decision_status = String(part?.reason || "finish")
        }
        stepNodeId = null
      }
    }

    messageIndex += 1
  }
  return rows
}

export async function writeParquet(rows, localPath) {
  const dir = join(localPath, "..")
  mkdirSync(dir, { recursive: true })
  const writer = await parquet.ParquetWriter.openFile(SCHEMA, localPath)
  for (const row of rows) {
    await writer.appendRow(row)
  }
  await writer.close()
  return localPath
}

export function tmpParquetPath(sessionId) {
  return join(tmpdir(), `${sessionId}.parquet`)
}

export function cleanupFile(filePath) {
  try {
    unlinkSync(filePath)
  } catch {}
}

export const REDPANDA_TOPIC = "sessions"

export function redpandaBrokers() {
  const value = process.env.REDPANDA_BROKERS || "localhost:9092"
  return value.split(",").map((s) => s.trim()).filter(Boolean)
}

export function hashRow(row) {
  return JSON.stringify(row)
}

let topicEnsured = null
let producerHolder = null

export function resetProducer() {
  const holder = producerHolder
  producerHolder = null
  if (holder) holder.producer.disconnect().catch(() => {})
}

async function ensureTopic(topic, brokers) {
  const key = `${brokers.join(",")}|${topic}`
  if (topicEnsured === key) return
  const { Kafka, TopicAlreadyExistsError } = await import("kafkajs")
  const client = new Kafka({ clientId: "session-archive", brokers })
  const admin = client.admin()
  await admin.connect()
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1 }],
      waitForLeaders: true,
    })
  } catch (err) {
    if (!(err instanceof TopicAlreadyExistsError)) {
      throw new Error(`Failed to create topic ${topic}: ${err?.message || err}`)
    }
  } finally {
    await admin.disconnect()
  }
  topicEnsured = key
}

async function getProducer(brokers) {
  const key = brokers.join(",")
  if (producerHolder && producerHolder.key === key) return producerHolder.producer
  const { Kafka } = await import("kafkajs")
  const client = new Kafka({ clientId: "session-archive", brokers })
  const producer = client.producer({ allowAutoTopicCreation: false })
  await producer.connect()
  producerHolder = { key, producer }
  return producer
}

export async function publishToRedpanda(rows, { brokers = redpandaBrokers(), topic = REDPANDA_TOPIC } = {}) {
  if (rows.length === 0) return { topic, count: 0, status: "published" }
  await ensureTopic(topic, brokers)
  const producer = await getProducer(brokers)
  const messages = rows.map((row) => ({ key: row.session_id, value: JSON.stringify(row) }))
  try {
    await producer.send({ topic, messages })
  } catch (err) {
    resetProducer()
    throw err
  }
  return { topic, count: rows.length, status: "published" }
}