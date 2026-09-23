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
  }
}

export function buildRows(rawMessages, { sessionId, repository = "" } = {}) {
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

export async function uploadToMinio({ parquetPath, bucket = BUCKET, objectKey, workspaceDir }) {
  const postSh = join(workspaceDir, "post.sh")
  const args = [parquetPath, bucket, objectKey]
  const result = await runScript(postSh, args, workspaceDir)
  if (result.exitCode !== 0) {
    const err = new Error(result.stderr || `post.sh exited with ${result.exitCode}`)
    err.exitCode = result.exitCode
    err.stdout = result.stdout
    err.stderr = result.stderr
    throw err
  }
  return { bucket, objectKey, status: "uploaded", stdout: result.stdout }
}

async function runScript(scriptPath, args, cwd) {
  const { spawn } = await import("node:child_process")
  const command = process.platform === "win32" ? "bash.exe" : "bash"
  return new Promise((resolve, reject) => {
    const child = spawn(command, [scriptPath, ...args], { cwd, stdio: ["ignore", "pipe", "pipe"] })
    let stdout = ""
    let stderr = ""
    let errored = null
    child.stdout.on("data", (chunk) => (stdout += chunk.toString()))
    child.stderr.on("data", (chunk) => (stderr += chunk.toString()))
    child.on("error", (err) => {
      errored = err
      reject(new Error(`Failed to run ${command}: ${err.message}`))
    })
    child.on("close", (exitCode) => {
      if (!errored) resolve({ exitCode, stdout, stderr })
    })
  })
}