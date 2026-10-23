import { strict as assert } from "node:assert"
import {
  cleanTopic,
  heuristicTopic,
  createTopicExtractor,
  hashKey,
  resetPreferredModel,
} from "./topic-extractor.mjs"

// 1. cleanTopic
assert.equal(cleanTopic('  "Sujet de test"  '), "Sujet de test")
assert.equal(cleanTopic("Sujet : extraction des sujets"), "extraction des sujets")
assert.equal(cleanTopic("```\ncode block\n```\nSujet reel"), "Sujet reel")
assert.equal(cleanTopic(""), "")
assert.ok(cleanTopic("x".repeat(200)).length <= 72)

// 2. heuristicTopic
assert.equal(heuristicTopic(""), "conversation")
assert.ok(heuristicTopic("Ajoute le service topic push au projet").length > 0)
assert.ok(!heuristicTopic("je veux que tu").includes("je"))

// 3. extract via ctx.generate.text (API V2, aucune session interne)
const calls = []
const ctx = {
  generate: {
    text: async ({ model, prompt }) => {
      calls.push([model, prompt])
      return { text: "Extraction des sujets LLM" }
    },
  },
  model: {
    default: async () => ({
      location: { directory: "/tmp" },
      data: { providerID: "openrouter", id: "minimax/minimax-m3.1-flash:free" },
    }),
  },
}

const extractor = createTopicExtractor({ ctx, log: async () => {} })
const topic = await extractor.extract(
  "session-1",
  "ord:0",
  "Construis le dashboard de sujets",
  { providerID: "openrouter", id: "minimax/minimax-m3.1-flash:free" },
)
assert.equal(topic, "Extraction des sujets LLM")
assert.ok(extractor.has("session-1", "ord:0"))
assert.equal(extractor.topicsFor("session-1").get("ord:0"), "Extraction des sujets LLM")
// le sujet est aussi indexe par l'empreinte du texte
assert.equal(
  extractor.topicsFor("session-1").get(`txt:${hashKey("Construis le dashboard de sujets")}`),
  "Extraction des sujets LLM",
)
assert.equal(calls.length, 1)
assert.equal(calls[0][0].providerID, "openrouter")
assert.equal(calls[0][0].id, "minimax/minimax-m3.1-flash:free")
assert.ok(calls[0][1].includes("Construis le dashboard de sujets"))

// 4. repli quand le modele echoue
const failing = createTopicExtractor({
  ctx: {
    generate: {
      text: async () => {
        throw new Error("boom")
      },
    },
    model: { default: async () => ({ data: { providerID: "openrouter", id: "x" } }) },
  },
  log: async () => {},
})
const fallback = await failing.extract(
  "session-2",
  "ord:0",
  "Corrige le job Spark d'ingestion Iceberg",
)
assert.equal(fallback, heuristicTopic("Corrige le job Spark d'ingestion Iceberg"))

// 5. mode desactive -> heuristique seule
const disabled = createTopicExtractor({ ctx, log: async () => {}, enabled: false })
const heur = await disabled.extract("session-3", "ord:0", "Ajoute un pipeline Arroyo")
assert.equal(heur, heuristicTopic("Ajoute un pipeline Arroyo"))
assert.equal(calls.length, 1, "aucun appel modele en mode desactive")

// 6. chaine de repli : le modele par defaut refuse, un autre modele repond
resetPreferredModel()
const attempts = []
const fallbackCtx = {
  generate: {
    text: async ({ model }) => {
      attempts.push(model)
      if (model.providerID === "opencode") {
        throw new Error("OpenCode's free tier can only be used from within OpenCode")
      }
      return { text: "redemarrage du service opencode" }
    },
  },
  model: {
    default: async () => ({ data: { providerID: "opencode", id: "deepseek-v4.1-flash-free" } }),
    list: async () => ({
      data: [
        { providerID: "opencode", id: "deepseek-v4.1-flash-free" },
        { providerID: "openrouter", id: "minimax/minimax-m3.1-flash:free" },
        { providerID: "openrouter", id: "anthropic/claude-sonnet-5.5" },
      ],
    }),
  },
}
const chained = createTopicExtractor({ ctx: fallbackCtx, log: async () => {} })
const chainedTopic = await chained.extract("session-4", "ord:0", "J'ai redemarre le service")
assert.equal(chainedTopic, "redemarrage du service opencode")
assert.equal(attempts[0].providerID, "opencode", "le modele de la session est tente en premier")
assert.ok(attempts.length >= 2, "un second modele est tente apres l'echec")
assert.equal(attempts[1].providerID, "openrouter")

// le modele qui fonctionne est memorise : un seul essai pour le prompt suivant
const before = attempts.length
const secondTopic = await chained.extract("session-4", "ord:1", "Continuer la verification")
assert.equal(secondTopic, "redemarrage du service opencode")
assert.equal(attempts.length, before + 1, "le modele memorise est reutilise directement")

console.log("topic-extractor: OK")
