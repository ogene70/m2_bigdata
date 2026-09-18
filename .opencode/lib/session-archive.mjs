import parquet from "parquetjs-lite"
import { mkdirSync, unlinkSync } from "node:fs"
import { join, tmpdir } from "node:path"
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
})

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

export function buildRows(rawMessages, { sessionId, repository = "" } = {}) {
  const rows = []
  let index = 0
  for (const entry of rawMessages || []) {
    const info = entry?.info
    const parts = Array.isArray(entry?.parts) ? entry.parts : []
    if (!info) continue

    const textParts = parts
      .filter((p) => p.type === "text" && p.text)
      .map((p) => p.text)
    const content = textParts.join("\n")

    if (!content.trim()) continue

    const created = info.time?.created ?? Date.now() / 1000
    rows.push({
      session_id: sessionId,
      timestamp: new Date(created * 1000),
      role: info.role,
      content,
      repository,
      model: info.role === "assistant"
        ? `${info.providerID || ""}/${info.modelID || ""}`
        : "",
      message_index: index++,
    })
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