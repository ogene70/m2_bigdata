import parquet from "parquetjs-lite"
import { mkdirSync, unlinkSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { randomUUID, createHash } from "node:crypto"
import { lookup } from "node:dns/promises"

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
  topic: { type: "UTF8", optional: true },
})

export const NODE_MESSAGE = "message"
export const NODE_STEP = "step"
export const NODE_REASONING = "reasoning"
export const NODE_TOOL = "tool"

// Roles derives du type de message OpenCode V2 (API /api/session/{id}/context).
const ROLE_BY_TYPE = {
  user: "user",
  assistant: "assistant",
  synthetic: "system",
  system: "system",
  skill: "system",
  shell: "system",
  compaction: "system",
  "agent.selected": "system",
  "model.selected": "system",
  "location.switched": "system",
}

export function hashText(text) {
  return createHash("sha1").update(String(text ?? "")).digest("hex").slice(0, 16)
}

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
  topic = "",
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
    topic: topic,
  }
}

function messageContent(msg) {
  switch (msg?.type) {
    case "user":
    case "synthetic":
    case "system":
      return String(msg.text ?? "")
    case "skill":
      return `${msg.name ?? msg.skill ?? "skill"}: ${msg.text ?? ""}`
    case "shell":
      return String(msg.command ?? "")
    case "agent.selected":
      return `agent: ${msg.agent ?? ""}`
    case "model.selected":
      return `model: ${msg.model?.providerID ?? ""}/${msg.model?.id ?? ""}`
    case "location.switched":
      return `location: ${msg.location ?? ""}`
    case "assistant": {
      const items = Array.isArray(msg.content) ? msg.content : []
      return items
        .filter((item) => item?.type === "text" && item.text)
        .map((item) => item.text)
        .join("\n")
    }
    default:
      return ""
  }
}

function lookupTopic(topics, ordinal, text) {
  if (!topics || typeof topics.get !== "function") return ""
  return topics.get(`ord:${ordinal}`) || topics.get(`txt:${hashText(text)}`) || ""
}

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim()
}

/**
 * La compaction retire les messages utilisateur du transcript : le contexte ne
 * contient plus que le resume et les messages assistant. Les prompts observes
 * en direct (`session.inbox.enqueued`) sont donc re-injectes pour conserver la
 * racine de l'arbre et le sujet. La correspondance se fait sur le texte pour
 * ne jamais dupliquer un prompt deja present.
 */
function missingPrompts(messages, prompts) {
  if (!Array.isArray(prompts) || prompts.length === 0) return []
  const available = []
  for (const msg of messages ?? []) {
    if (String(msg?.type ?? "") !== "user") continue
    available.push(normalizeText(msg?.text))
  }
  const extra = []
  for (const prompt of prompts) {
    const text = normalizeText(prompt?.text)
    if (!text) continue
    const index = available.indexOf(text)
    if (index >= 0) {
      available.splice(index, 1)
      continue
    }
    extra.push({
      type: "user",
      text: String(prompt?.text ?? ""),
      time: { created: Number.isFinite(prompt?.created) ? prompt.created : Date.now() },
    })
  }
  extra.sort((a, b) => (a.time.created ?? 0) - (b.time.created ?? 0))
  return extra
}

/**
 * Construit les lignes d'archive a partir des messages OpenCode V2
 * (`ctx.session.context({ sessionID })`).
 *
 * Arbre de decision :
 *   message (user)            parent = 0
 *     message (assistant)     parent = dernier message utilisateur
 *       step                  parent = message assistant
 *         reasoning | tool    parent = step
 */
export function buildRows(
  messages,
  { sessionId, repository = "", harness = "opencode", topics = null, prompts = null } = {},
) {
  const transcript = Array.isArray(messages) ? messages : []
  const injected = missingPrompts(transcript, prompts)
  const effective = injected.length > 0 ? [...injected, ...transcript] : transcript
  const rows = []
  let messageIndex = 0
  let nextNodeId = 1
  let lastUserNodeId = 0
  let lastMessageNodeId = 0
  let userOrdinal = 0

  for (const msg of effective) {
    const type = String(msg?.type ?? "")
    if (!type) {
      messageIndex += 1
      continue
    }

    const created = msg?.time?.created
    const timestamp = new Date(typeof created === "number" ? created : Date.now())

    if (type === "idle") {
      rows.push(node({
        sessionId,
        timestamp,
        role: "system",
        content: "",
        repository,
        model: "",
        messageIndex,
        nodeId: nextNodeId++,
        parentId: lastMessageNodeId,
        nodeType: NODE_STEP,
        status: String(msg.outcome ?? "idle"),
        harness,
      }))
      messageIndex += 1
      continue
    }

    const role = ROLE_BY_TYPE[type] ?? "system"
    const content = messageContent(msg)
    const model = type === "assistant"
      ? `${msg.model?.providerID ?? ""}/${msg.model?.id ?? ""}`
      : ""

    const tokens = msg?.tokens ?? {}
    const tokensIn = Number.isFinite(tokens.input) ? tokens.input : null
    const tokensOut = Number.isFinite(tokens.output) ? tokens.output : null

    const messageNodeId = nextNodeId++
    const parentId = role === "user" ? 0 : (lastUserNodeId || 0)
    const topic = role === "user" ? lookupTopic(topics, userOrdinal, content) : ""
    if (role === "user") userOrdinal += 1

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
      topic,
    }))

    if (role === "user") lastUserNodeId = messageNodeId
    lastMessageNodeId = messageNodeId

    if (type === "assistant") {
      const items = Array.isArray(msg.content) ? msg.content : []
      const stepNodeId = nextNodeId++
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
        status: String(msg.finish ?? "started"),
        harness,
      }))

      for (const item of items) {
        if (item?.type === "reasoning") {
          rows.push(node({
            sessionId,
            timestamp,
            role,
            content: summarize(item.text, 500),
            repository,
            model,
            messageIndex,
            nodeId: nextNodeId++,
            parentId: stepNodeId,
            nodeType: NODE_REASONING,
            harness,
          }))
        } else if (item?.type === "tool") {
          const state = item.state ?? {}
          const toolInput = summarize(
            JSON.stringify(state.input ?? ""),
            200,
          )
          rows.push(node({
            sessionId,
            timestamp,
            role,
            content: summarize(item.name || toolInput || "(outil)", 120),
            repository,
            model,
            messageIndex,
            nodeId: nextNodeId++,
            parentId: stepNodeId,
            nodeType: NODE_TOOL,
            status: String(state.status ?? "running"),
            tool: String(item.name ?? ""),
            input: toolInput,
            harness,
          }))
        }
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

// `localhost:9093` est le listener hote (advertise EXTERNAL://localhost:9093) ;
// `localhost:9092` annonce `redpanda:9092`, nom interne Docker intraduisible
// depuis la machine hote ou tourne le plugin.
export const HOST_BROKERS = ["localhost:9093", "localhost:9092"]

export function redpandaBrokers() {
  const value = process.env.REDPANDA_BROKERS || ""
  return value.split(",").map((s) => s.trim()).filter(Boolean)
}

/**
 * Le plugin tourne sur la machine hote, pas dans le reseau Docker : un nom
 * interne comme `redpanda:9092` n'est pas resolvable. On ne garde que les
 * brokers resolvables, le listener hote en tete.
 */
export async function brokerCandidates() {
  const list = []
  for (const broker of [...HOST_BROKERS, ...redpandaBrokers()]) {
    if (!list.includes(broker)) list.push(broker)
  }
  const usable = []
  for (const broker of list) {
    const host = broker.split(":")[0]
    try {
      await lookup(host)
      usable.push(broker)
    } catch {
      // nom interne Docker : ignore depuis l'hote
    }
  }
  return usable.length > 0 ? usable : list
}

function kafkaClientOptions(brokers) {
  return {
    clientId: "session-archive",
    brokers,
    connectionTimeout: 3000,
    requestTimeout: 15000,
    retry: { retries: 2 },
  }
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
  const { Kafka } = await import("kafkajs")
  const client = new Kafka(kafkaClientOptions(brokers))
  const admin = client.admin()
  await admin.connect()
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1 }],
      waitForLeaders: true,
    })
  } catch (err) {
    // kafkajs 2.x n'expose pas ses classes d'erreur : on teste le type.
    const type = err?.type ?? ""
    const name = err?.name ?? ""
    if (type !== "TOPIC_ALREADY_EXISTS" && name !== "TopicAlreadyExistsError") {
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
  const client = new Kafka(kafkaClientOptions(brokers))
  const producer = client.producer({ allowAutoTopicCreation: false })
  await producer.connect()
  producerHolder = { key, producer }
  return producer
}

export async function publishToRedpanda(rows, { brokers, topic = REDPANDA_TOPIC } = {}) {
  if (rows.length === 0) return { topic, count: 0, status: "published" }
  const candidates = brokers && brokers.length > 0 ? brokers : await brokerCandidates()
  const messages = rows.map((row) => ({ key: row.session_id, value: JSON.stringify(row) }))
  let lastError = null
  for (const candidate of candidates) {
    try {
      await ensureTopic(topic, [candidate])
      const producer = await getProducer([candidate])
      await producer.send({ topic, messages })
      return { topic, count: rows.length, status: "published", broker: candidate }
    } catch (err) {
      lastError = err
      resetProducer()
    }
  }
  throw lastError ?? new Error("aucun broker Redpanda joignable")
}