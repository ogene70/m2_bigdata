import { createHash } from "node:crypto"

const MAX_TOPIC_CHARS = 72
const MAX_INPUT_CHARS = 4000
const PROMPT_TIMEOUT_MS = 30000

const SYSTEM_PROMPT = [
  "Tu analyses la demande d'un utilisateur dans une session de travail logiciel.",
  "Reponds UNIQUEMENT par un sujet de 3 a 6 mots, en francais.",
  "Pas de guillemets, pas de ponctuation finale, pas de preambule ni d'explication.",
  "Le sujet doit nommer l'intention precise.",
  "Exemples de sujets valides : 'refonte du dashboard HTML', 'correction du job Spark ingest', 'ajout du webhook Redpanda'.",
].join(" ")

const STOPWORDS = new Set([
  "le", "la", "les", "un", "une", "des", "de", "du", "et", "ou", "mais", "donc", "ni", "car",
  "je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "on", "me", "te", "se", "y", "en",
  "que", "qui", "quoi", "dont", "ou", "ce", "cet", "cette", "ces", "au", "aux", "a",
  "pour", "par", "avec", "sans", "dans", "sur", "sous", "vers", "chez", "entre", "depuis",
  "est", "sont", "etait", "etaient", "ete", "etre", "avoir", "ai", "as", "ont", "eu",
  "plus", "moins", "tres", "bien", "tout", "tous", "toute", "toutes", "aussi", "alors", "ainsi",
  "mon", "ma", "mes", "ton", "ta", "tes", "son", "sa", "ses", "notre", "nos", "votre", "vos",
  "leur", "leurs", "si", "ne", "pas", "non", "oui",
  "peux", "peut", "pourrais", "pourrait", "voudrais", "voudrait", "fais", "faire", "faites",
  "comment", "pourquoi", "quel", "quelle", "quels", "quelles", "quand",
  "the", "and", "you", "this", "that", "with", "from", "have", "not", "but", "for", "are", "was",
])

function withTimeout(promise, ms) {
  let timer
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`delai depasse (${ms}ms)`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

export function cleanTopic(raw) {
  let text = String(raw ?? "")
    .replace(/```[\s\S]*?```/g, " ")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)[0]
  if (!text) return ""
  text = text
    .replace(/^\s*(sujet|topic)\s*[:\-—]\s*/i, "")
    .replace(/^["'`«“]+/, "")
    .replace(/["'`»”]+$/, "")
    .replace(/[.:;!?]+$/, "")
    .replace(/\s+/g, " ")
    .trim()
  if (!text) return ""
  if (text.length > MAX_TOPIC_CHARS) {
    text = text.slice(0, MAX_TOPIC_CHARS)
    const cut = text.lastIndexOf(" ")
    if (cut > MAX_TOPIC_CHARS * 0.6) text = text.slice(0, cut)
    text = text.trim()
  }
  return text
}

export function heuristicTopic(raw) {
  const text = String(raw ?? "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
  if (!text) return "conversation"
  const words = text
    .split(" ")
    .map((w) => w.replace(/^[^\p{L}\p{N}_]+|[^\p{L}\p{N}_]+$/gu, ""))
    .filter((w) => w.length > 2 && !STOPWORDS.has(w.toLowerCase()))
  if (words.length === 0) return "conversation"
  return cleanTopic(words.slice(0, 5).join(" ")) || "conversation"
}

export function hashKey(text) {
  return createHash("sha1").update(String(text ?? "")).digest("hex").slice(0, 16)
}

async function resolveModelRef(ctx, model) {
  if (model?.providerID && (model.id || model.modelID)) {
    return { providerID: model.providerID, id: model.id ?? model.modelID }
  }
  const def = await ctx.model.default()
  const data = def?.data ?? def
  if (!data?.providerID || !data?.id) throw new Error("modele par defaut introuvable")
  return { providerID: data.providerID, id: data.id }
}

// Modele qui a deja produit un sujet : evite de retenter un modele inutilisable
// (le palier gratuit du fournisseur `opencode` refuse `ctx.generate.text`).
let preferredRef = null

export function resetPreferredModel() {
  preferredRef = null
}

function modelScore(model) {
  let score = 0
  if (model.providerID !== "opencode") score += 10
  if (/:free$/.test(model.id)) score += 3
  if (/flash|mini|haiku|nano/i.test(model.id)) score += 2
  return score
}

/**
 * Le modele de la session d'abord (fidelite a la configuration), puis les
 * modeles capables de repondre a un appel transitoire, du moins cher au plus
 * cher. Limite a trois essais pour borner la latence.
 */
async function modelCandidates(ctx, model) {
  const out = []
  const push = (ref) => {
    if (!ref?.providerID || !ref?.id) return
    if (out.some((c) => c.providerID === ref.providerID && c.id === ref.id)) return
    out.push({ providerID: ref.providerID, id: ref.id })
  }
  if (preferredRef) push(preferredRef)
  try {
    push(await resolveModelRef(ctx, model))
  } catch {
    // pas de modele par defaut : on s'appuie sur la liste
  }
  try {
    const listed = await ctx.model.list()
    const models = (listed?.data ?? listed ?? [])
      .map((entry) => ({ providerID: entry?.providerID, id: entry?.id ?? entry?.modelID }))
      .filter((entry) => entry.providerID && entry.id)
      .sort((a, b) => modelScore(b) - modelScore(a))
    for (const entry of models) {
      if (out.length >= 3) break
      push(entry)
    }
  } catch {
    // la liste est optionnelle
  }
  return out.slice(0, 3)
}

/**
 * Extraction des sujets de conversation (OpenCode V2).
 *
 * L'appel se fait via `ctx.generate.text` : aucune session interne n'est creee,
 * donc rien n'est archive pour l'extraction et aucune recursion n'est possible.
 */
export function createTopicExtractor({ ctx, log, enabled = true } = {}) {
  const topicsBySession = new Map()
  const inFlight = new Map()

  function sessionTopics(sessionId) {
    let map = topicsBySession.get(sessionId)
    if (!map) {
      map = new Map()
      topicsBySession.set(sessionId, map)
    }
    return map
  }

  async function callModel(text, model) {
    const prompt = `${SYSTEM_PROMPT}\n\n${String(text).slice(0, MAX_INPUT_CHARS)}`
    const candidates = await modelCandidates(ctx, model)
    if (candidates.length === 0) throw new Error("aucun modele disponible")
    let lastError = null
    for (const ref of candidates) {
      try {
        const res = await withTimeout(
          ctx.generate.text({ model: ref, prompt }),
          PROMPT_TIMEOUT_MS,
        )
        const topic = cleanTopic(res?.text ?? "")
        if (!topic) {
          lastError = new Error(`reponse vide (${ref.providerID}/${ref.id})`)
          continue
        }
        const changed =
          !preferredRef ||
          preferredRef.providerID !== ref.providerID ||
          preferredRef.id !== ref.id
        preferredRef = ref
        if (changed) {
          await log?.(`sujet extrait avec ${ref.providerID}/${ref.id}`, "info")
        }
        return topic
      } catch (err) {
        lastError = err
        await log?.(
          `modele ${ref.providerID}/${ref.id} indisponible: ${err?.message || err}`,
          "warn",
        )
      }
    }
    throw lastError ?? new Error("aucun modele n'a produit de sujet")
  }

  function has(sessionId, key) {
    return sessionTopics(sessionId).has(key)
  }

  function topicsFor(sessionId) {
    return sessionTopics(sessionId)
  }

  /**
   * `key` designe le message utilisateur : `ord:<n>` (rang du message
   * utilisateur dans la session) et `txt:<hash>` (empreinte du texte) sont
   * tous deux enregistres pour rester robuste aux decalages d'index.
   */
  async function extract(sessionId, key, text, model) {
    const map = sessionTopics(sessionId)
    const pendingKey = `${sessionId}::${key}`
    const existing = inFlight.get(pendingKey)
    if (existing) return existing

    const fallback = heuristicTopic(text)
    if (!enabled) {
      store(map, key, text, fallback)
      return fallback
    }

    const promise = (async () => {
      try {
        const topic = (await callModel(text, model)) || fallback
        store(map, key, text, topic)
        return topic
      } catch (err) {
        store(map, key, text, fallback)
        await log?.(
          `extraction de sujet impossible (${err?.message || err}), repli heuristique`,
          "warn",
        )
        return fallback
      } finally {
        inFlight.delete(pendingKey)
      }
    })()

    inFlight.set(pendingKey, promise)
    return promise
  }

  function store(map, key, text, topic) {
    map.set(key, topic)
    if (key.startsWith("ord:")) map.set(`txt:${hashKey(text)}`, topic)
  }

  return {
    extract,
    has,
    topicsFor,
    hashKey,
  }
}
