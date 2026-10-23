import { strict as assert } from "node:assert"
import { buildRows, hashText, SCHEMA } from "./session-archive.mjs"

const now = 1792586796831

const messages = [
  {
    id: "msg_1",
    type: "user",
    time: { created: now },
    text: "Construis le dashboard de sujets",
  },
  {
    id: "msg_2",
    type: "assistant",
    time: { created: now + 10, completed: now + 900 },
    agent: "build",
    model: { providerID: "openrouter", id: "minimax/minimax-m3.1-flash:free" },
    content: [
      { type: "reasoning", text: "Je commence par lire la config" },
      { type: "text", text: "Je lis la config du projet" },
      {
        type: "tool",
        id: "call_1",
        name: "read",
        state: {
          status: "completed",
          input: { filePath: "AGENTS.md" },
          content: [{ type: "text", text: "..." }],
        },
      },
    ],
    finish: "tool-calls",
    tokens: { input: 120, output: 40 },
  },
  {
    id: "msg_3",
    type: "idle",
    time: { created: now + 1000 },
    outcome: "succeeded",
  },
]

const topics = new Map([
  ["ord:0", "dashboard de sujets"],
  [`txt:${hashText("Construis le dashboard de sujets")}`, "dashboard de sujets"],
])

const rows = buildRows(messages, {
  sessionId: "20261021-120000-abcdef123456",
  repository: "/home/ogene/m1Eadl/M2/m2_bigdata",
  harness: "opencode",
  topics,
})

// 4 noeuds : message user, message assistant, step, reasoning, tool, idle
assert.equal(rows.length, 6)

// 1. message utilisateur : racine de l'arbre, sujet renseigne
const user = rows[0]
assert.equal(user.role, "user")
assert.equal(user.decision_node_type, "message")
assert.equal(user.decision_parent_id, 0)
assert.equal(user.message_index, 0)
assert.equal(user.topic, "dashboard de sujets")
assert.equal(user.content, "Construis le dashboard de sujets")
assert.equal(user.session_id, "20261021-120000-abcdef123456")
assert.equal(user.harness, "opencode")
assert.equal(user.decision_node_id, 1)

// 2. message assistant : enfant du message utilisateur
const assistant = rows[1]
assert.equal(assistant.role, "assistant")
assert.equal(assistant.decision_node_type, "message")
assert.equal(assistant.decision_parent_id, user.decision_node_id)
assert.equal(assistant.message_index, 1)
assert.equal(assistant.model, "openrouter/minimax/minimax-m3.1-flash:free")
assert.equal(assistant.tokens_in, 120)
assert.equal(assistant.tokens_out, 40)
assert.equal(assistant.content, "Je lis la config du projet")

// 3. step : enfant du message assistant
const step = rows[2]
assert.equal(step.decision_node_type, "step")
assert.equal(step.decision_parent_id, assistant.decision_node_id)
assert.equal(step.decision_status, "tool-calls")

// 4. reasoning : enfant du step
const reasoning = rows[3]
assert.equal(reasoning.decision_node_type, "reasoning")
assert.equal(reasoning.decision_parent_id, step.decision_node_id)
assert.equal(reasoning.content, "Je commence par lire la config")

// 5. tool : enfant du step, avec nom et entree
const tool = rows[4]
assert.equal(tool.decision_node_type, "tool")
assert.equal(tool.decision_parent_id, step.decision_node_id)
assert.equal(tool.tool_name, "read")
assert.equal(tool.decision_status, "completed")
assert.ok(tool.tool_input.includes("AGENTS.md"))

// 6. idle : noeud terminal avec le statut de la session
const idle = rows[5]
assert.equal(idle.decision_node_type, "step")
assert.equal(idle.decision_status, "succeeded")
assert.equal(idle.decision_parent_id, assistant.decision_node_id)
assert.equal(idle.message_index, 2)

// le schema Parquet expose bien la colonne topic
const fields = Object.keys(SCHEMA.fields)
assert.ok(fields.includes("topic"))
assert.ok(fields.includes("decision_node_id"))

// sans sujet, la colonne reste vide
const withoutTopic = buildRows(messages, { sessionId: "s" })
assert.equal(withoutTopic[0].topic, "")

// compaction : le transcript ne contient plus de message utilisateur
const compacted = [
  {
    id: "msg_c",
    type: "compaction",
    time: { created: now + 2000 },
    text: "Resume de la conversation precedente",
  },
  {
    id: "msg_a",
    type: "assistant",
    time: { created: now + 3000 },
    model: { providerID: "opencode", id: "deepseek-v4.1-flash-free" },
    content: [{ type: "text", text: "Je reprends le travail" }],
    finish: "stop",
  },
]

const prompts = [{ text: "Construis le dashboard de sujets", created: now }]
const injected = buildRows(compacted, {
  sessionId: "20261021-153509-000000000001",
  topics,
  prompts,
})
assert.equal(injected[0].role, "user")
assert.equal(injected[0].content, "Construis le dashboard de sujets")
assert.equal(injected[0].decision_parent_id, 0)
assert.equal(injected[0].topic, "dashboard de sujets")
const injectedAssistant = injected.find((row) => row.role === "assistant" && row.decision_node_type === "message")
assert.equal(injectedAssistant.decision_parent_id, injected[0].decision_node_id)

// le prompt deja present dans le transcript n'est pas duplique
const notDuplicated = buildRows(messages, {
  sessionId: "20261021-120000-abcdef123456",
  topics,
  prompts,
})
assert.equal(notDuplicated.filter((row) => row.role === "user").length, 1)

console.log("session-archive: OK")
