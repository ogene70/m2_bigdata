import {
  buildRows,
  objectKeyFor,
  writeParquet,
  uploadToMinio,
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

        const localPath = tmpParquetPath(sessionID)
        await writeParquet(rows, localPath)

        const objectKey = objectKeyFor(sessionID)
        const result = await uploadToMinio({
          parquetPath: localPath,
          objectKey,
          workspaceDir: directory,
        })

        cleanupFile(localPath)

        await log(`bucket: ${result.bucket}`)
        await log(`object-key: ${result.objectKey}`)
        await log(`status: ${result.status}`)
      } catch (err) {
        await log(`Archive failed for session ${sessionID}: ${err?.message || err}`, "error")
      }
    },
  }
}

export default SessionArchivePlugin