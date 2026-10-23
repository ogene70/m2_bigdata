import { join } from "node:path"
import { mkdirSync, copyFileSync, appendFileSync } from "node:fs"
import {
  buildRows,
  objectKeyFor,
  writeParquet,
  publishToRedpanda,
  hashRow,
  generateSessionId,
  tmpParquetPath,
  cleanupFile,
} from "../lib/session-archive.mjs"
import { createTopicExtractor } from "../lib/topic-extractor.mjs"

const DEBOUNCE_MS = 250
const TOPIC_DELAY_MS = 400

// Evenements qui declenchent une vidange incremental de l'archive.
const FLUSH_EVENTS = new Set([
  "session.text.ended",
  "session.reasoning.ended",
  "session.tool.success",
  "session.tool.error",
  "session.step.ended",
])

export default {
  id: "session-archive",
  async setup(ctx) {
    const directory = ctx.location?.directory ?? ""

    const logFile = join(directory, ".opencode", "logs", "session-archive.log")
    const log = async (message, level = "info") => {
      const line = `${new Date().toISOString()} [${level}] ${message}\n`
      try {
        mkdirSync(join(directory, ".opencode", "logs"), { recursive: true })
        appendFileSync(logFile, line)
      } catch {
        // le journal est best-effort
      }
    }

    const topicExtractor = createTopicExtractor({ ctx, log })

    const published = new Map() // sessionID -> Map(nodeKey -> rowHash)
    const timers = new Map() // sessionID -> timeout
    const inFlight = new Set()
    const sessionDirs = new Map() // sessionID -> directory
    const userCounters = new Map() // sessionID -> nombre de prompts utilisateurs
    const userPrompts = new Map() // sessionID -> [{ text, created }] observes en direct
    const archiveIds = new Map() // sessionID -> identifiant d'archive

    const preserveLocally = async (sessionID, tmpPath) => {
      const archiveDir = join(directory, "archives")
      mkdirSync(archiveDir, { recursive: true })
      const localPath = join(archiveDir, `${sessionID}.parquet`)
      copyFileSync(tmpPath, localPath)
      return localPath
    }

    const resolveArchiveSessionId = async (sessionID) => {
      if (archiveIds.has(sessionID)) return archiveIds.get(sessionID)
      const key = `session-id:${sessionID}`
      let value = null
      try {
        value = await ctx.storage.get(key)
      } catch {
        value = null
      }
      if (typeof value !== "string" || !value) {
        value = generateSessionId(new Date())
        try {
          await ctx.storage.set(key, value)
        } catch {
          // le stockage est un cache : l'identifiant reste valable pour la session
        }
      }
      archiveIds.set(sessionID, value)
      return value
    }

    const nextUserOrdinal = (sessionID) => {
      const current = userCounters.get(sessionID) ?? 0
      userCounters.set(sessionID, current + 1)
      return current
    }

    const belongsToProject = async (sessionID) => {
      const known = sessionDirs.get(sessionID)
      if (known) return known === directory
      try {
        const info = await ctx.session.get({ sessionID })
        const dir = info?.location?.directory ?? info?.directory ?? ""
        sessionDirs.set(sessionID, dir)
        return dir === "" || dir === directory
      } catch {
        return true
      }
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

    const schedule = (sessionID, delay = DEBOUNCE_MS) => {
      const existing = timers.get(sessionID)
      if (existing) clearTimeout(existing)
      timers.set(
        sessionID,
        setTimeout(() => {
          timers.delete(sessionID)
          void archive(sessionID)
        }, delay),
      )
    }

    const archive = async (sessionID) => {
      if (inFlight.has(sessionID)) {
        schedule(sessionID)
        return
      }
      inFlight.add(sessionID)
      try {
        const messages = await ctx.session.context({ sessionID })
        if (!Array.isArray(messages) || messages.length === 0) return

        const archiveSessionId = await resolveArchiveSessionId(sessionID)
        const rows = buildRows(messages, {
          sessionId: archiveSessionId,
          repository: directory,
          harness: "opencode",
          topics: topicExtractor.topicsFor(sessionID),
          prompts: userPrompts.get(sessionID) ?? [],
        })
        if (rows.length === 0) return

        const { changed, pending } = diffRows(sessionID, rows)
        if (changed.length === 0) return

        const res = await publishToRedpanda(changed)
        published.set(sessionID, pending)
        await log(
          `archived ${res.count}/${rows.length} rows -> ${res.topic} (session ${sessionID} / ${archiveSessionId})`,
        )
      } catch (err) {
        await log(`Archive failed for session ${sessionID}: ${err?.message || err}`, "error")
        try {
          const messages = await ctx.session.context({ sessionID })
          if (Array.isArray(messages) && messages.length > 0) {
            const archiveSessionId = await resolveArchiveSessionId(sessionID)
            const rows = buildRows(messages, {
              sessionId: archiveSessionId,
              repository: directory,
              harness: "opencode",
              topics: topicExtractor.topicsFor(sessionID),
              prompts: userPrompts.get(sessionID) ?? [],
            })
            if (rows.length > 0) {
              const tmpPath = tmpParquetPath(archiveSessionId)
              await writeParquet(rows, tmpPath)
              const objectKey = objectKeyFor(archiveSessionId)
              const localPath = await preserveLocally(archiveSessionId, tmpPath)
              cleanupFile(tmpPath)
              await log(`Session archive retained locally for manual upload.`, "error")
              await log(`Intended object key: ${objectKey}`, "error")
              await log(`Local file: ${localPath}`, "error")
            }
          }
        } catch (fallbackErr) {
          await log(`Fallback Parquet preservation failed: ${fallbackErr?.message || fallbackErr}`, "error")
        }
      } finally {
        inFlight.delete(sessionID)
      }
    }

    const extractTopic = (sessionID, ordinal, text) => {
      if (!text) return
      void topicExtractor
        .extract(sessionID, `ord:${ordinal}`, text)
        .then(() => schedule(sessionID, TOPIC_DELAY_MS))
        .catch((err) => {
          void log(`extraction de sujet echouee: ${err?.message || err}`, "warn")
        })
    }

    // Le texte du prompt peut se trouver a plusieurs endroits selon la version.
    const inboxText = (data) =>
      data?.item?.payload?.text ??
      data?.payload?.text ??
      data?.item?.text ??
      data?.text ??
      ""

    const controller = new AbortController()
    void (async () => {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        const type = event?.type
        const data = event?.data ?? {}
        const sessionID = data.sessionID
        if (!sessionID) continue

        const eventDir = event.location?.directory ?? data.location?.directory
        if (eventDir) sessionDirs.set(sessionID, eventDir)
        if (!(await belongsToProject(sessionID))) continue

        if (type === "session.inbox.enqueued") {
          const ordinal = nextUserOrdinal(sessionID)
          const text = inboxText(data)
          if (!text) {
            void log(
              `inbox.enqueued sans texte: data=${JSON.stringify(Object.keys(data))} item=${JSON.stringify(Object.keys(data?.item ?? {}))} payload=${JSON.stringify(Object.keys(data?.item?.payload ?? data?.payload ?? {}))}`,
              "warn",
            )
          } else {
            // Conserve le prompt : la compaction le retire du transcript, et
            // c'est notre seule source fiable pour la racine de l'arbre.
            const list = userPrompts.get(sessionID) ?? []
            list.push({ text, created: Date.now() })
            if (list.length > 200) list.splice(0, list.length - 200)
            userPrompts.set(sessionID, list)
          }
          extractTopic(sessionID, ordinal, text)
          schedule(sessionID)
          continue
        }

        if (FLUSH_EVENTS.has(type)) {
          schedule(sessionID)
          continue
        }

        if (type && type.startsWith("session.execution.") && type !== "session.execution.started") {
          const timer = timers.get(sessionID)
          if (timer) {
            clearTimeout(timer)
            timers.delete(sessionID)
          }
          await archive(sessionID)
        }
      }
    })().catch((err) => {
      void log(`Event stream stopped: ${err?.message || err}`, "error")
    })

    await log(`plugin loaded for ${directory} (opencode ${ctx.app?.version ?? "?"})`)

    return () => {
      controller.abort()
      for (const timer of timers.values()) clearTimeout(timer)
      timers.clear()
    }
  },
}
