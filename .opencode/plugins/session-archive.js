import { join } from "node:path"
import { mkdirSync, copyFileSync } from "node:fs"
import {
  buildRows,
  objectKeyFor,
  writeParquet,
  publishToRedpanda,
  hashRow,
  redpandaBrokers,
  REDPANDA_TOPIC,
  tmpParquetPath,
  cleanupFile,
} from "../lib/session-archive.mjs"

const DEBOUNCE_MS = 100

export const SessionArchivePlugin = async ({ client, directory }) => {
  const log = async (message, level = "info") => {
    await client.app.log({
      body: {
        service: "session-archive",
        level,
        message,
      },
    }).catch(() => {})
  }

  const preserveLocally = async (sessionID, tmpPath) => {
    const archiveDir = join(directory, "archives")
    mkdirSync(archiveDir, { recursive: true })
    const localPath = join(archiveDir, `${sessionID}.parquet`)
    copyFileSync(tmpPath, localPath)
    return localPath
  }

  const published = new Map() // sessionID -> Map(nodeKey -> rowHash)
  const timers = new Map() // sessionID -> timeout
  const inFlight = new Set()

  const fetchRows = async (sessionID) => {
    const msgsRes = await client.session.messages({
      path: { id: sessionID },
    })
    const rawMessages = msgsRes.data || []
    return buildRows(rawMessages, {
      sessionId: sessionID,
      repository: directory || "",
      harness: "opencode",
    })
  }

  const diffRows = (sessionID, rows) => {
    let seen = published.get(sessionID)
    if (!seen) {
      seen = new Map()
      published.set(sessionID, seen)
    }
    const changed = []
    const pending = new Map()
    for (const row of rows) {
      const key = `${row.decision_node_id}`
      const hash = hashRow(row)
      pending.set(key, hash)
      if (seen.get(key) !== hash) changed.push(row)
    }
    return { changed, pending }
  }

  const archive = async (sessionID) => {
    if (inFlight.has(sessionID)) {
      schedule(sessionID)
      return
    }
    inFlight.add(sessionID)
    try {
      const rows = await fetchRows(sessionID)
      if (rows.length === 0) return
      const { changed, pending } = diffRows(sessionID, rows)
      if (changed.length === 0) return
      const res = await publishToRedpanda(changed)
      published.set(sessionID, pending)
      await log(
        `archived ${res.count}/${rows.length} rows -> ${res.topic} (session ${sessionID})`,
      )
    } catch (err) {
      await log(`Archive failed for session ${sessionID}: ${err?.message || err}`, "error")

      try {
        const rows = await fetchRows(sessionID)
        if (rows.length > 0) {
          const tmpPath = tmpParquetPath(sessionID)
          await writeParquet(rows, tmpPath)
          const objectKey = objectKeyFor(sessionID)
          const localPath = await preserveLocally(sessionID, tmpPath)
          cleanupFile(tmpPath)
          await log(`Session archive retained locally for manual upload.`, "error")
          await log(`Intended object key: ${objectKey}`, "error")
          await log(`Local file: ${localPath}`, "error")
        }
      } catch (fallbackErr) {
        await log(`Fallback Parquet preservation failed: ${fallbackErr?.message || fallbackErr}`, "error")
      }
    } finally {
      inFlight.delete(sessionID)
    }
  }

  const schedule = (sessionID) => {
    const existing = timers.get(sessionID)
    if (existing) clearTimeout(existing)
    timers.set(
      sessionID,
      setTimeout(() => {
        timers.delete(sessionID)
        void archive(sessionID)
      }, DEBOUNCE_MS),
    )
  }

  return {
    "session.idle": async (input) => {
      const sessionID = input?.properties?.sessionID
      if (!sessionID) return
      const t = timers.get(sessionID)
      if (t) {
        clearTimeout(t)
        timers.delete(sessionID)
      }
      await log(`Session idle, flushing archive: ${sessionID}`)
      await archive(sessionID)
    },
    "session.deleted": async (input) => {
      const sessionID = input?.properties?.info?.id
      if (!sessionID) return
      const t = timers.get(sessionID)
      if (t) {
        clearTimeout(t)
        timers.delete(sessionID)
      }
      published.delete(sessionID)
      await log(`Cleared archive state for deleted session ${sessionID}`)
    },
    event: async ({ event }) => {
      if (event?.type !== "message.part.updated") return
      const sessionID = event.properties?.part?.sessionID
      if (!sessionID) return
      schedule(sessionID)
    },
  }
}

export default SessionArchivePlugin
