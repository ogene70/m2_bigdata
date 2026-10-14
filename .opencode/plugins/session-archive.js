import { join } from "node:path"
import { mkdirSync, copyFileSync } from "node:fs"
import {
  buildRows,
  objectKeyFor,
  writeParquet,
  publishToRedpanda,
  redpandaBrokers,
  REDPANDA_TOPIC,
  tmpParquetPath,
  cleanupFile,
} from "../lib/session-archive.mjs"

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

  return {
    "session.idle": async (input) => {
      const sessionID = input?.properties?.sessionID
      if (!sessionID) return
      await log(`Session idle, archiving: ${sessionID}`)

      try {
        const msgsRes = await client.session.messages({
          path: { id: sessionID },
        })
        const rawMessages = msgsRes.data || []

        const rows = buildRows(rawMessages, {
          sessionId: sessionID,
          repository: directory || "",
        })

        if (rows.length === 0) {
          await log(`No messages with text content for session ${sessionID}, skipping archive`)
          return
        }

        await log(`rows: ${rows.length}`)
        await log(`redpanda topic: ${REDPANDA_TOPIC}`)
        await log(`brokers: ${redpandaBrokers().join(", ")}`)

        const published = await publishToRedpanda(rows)

        await log(`topic: ${published.topic}`)
        await log(`count: ${published.count}`)
        await log(`status: ${published.status}`)
      } catch (err) {
        await log(`Archive failed for session ${sessionID}: ${err?.message || err}`, "error")

        try {
          const msgsRes = await client.session.messages({
            path: { id: sessionID },
          })
          const rawMessages = msgsRes.data || []
          const rows = buildRows(rawMessages, {
            sessionId: sessionID,
            repository: directory || "",
          })
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
      }
    },
  }
}

export default SessionArchivePlugin