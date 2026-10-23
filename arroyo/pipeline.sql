-- Stream d'extraction des sujets de conversation.
--
-- Source : topic Redpanda `sessions` (publie par le plugin opencode).
-- Filtre : lignes `role = 'user'` portant un sujet extrait par le LLM.
-- Sink   : push HTTP (une requete POST par sujet) vers topic-push:8090.

CREATE TABLE session_rows (
  session_id TEXT,
  "timestamp" TIMESTAMP,
  role TEXT,
  content TEXT,
  message_index INT,
  decision_node_id INT,
  topic TEXT
) WITH (
  connector = 'kafka',
  type = 'source',
  format = 'json',
  bootstrap_servers = 'redpanda:9092',
  topic = 'sessions',
  'source.offset' = 'earliest',
  bad_data = 'drop'
);

CREATE TABLE topic_events (
  session_id TEXT,
  "timestamp" TIMESTAMP,
  topic TEXT,
  message_index INT,
  decision_node_id INT,
  content TEXT
) WITH (
  connector = 'webhook',
  endpoint = 'http://topic-push:8090/ingest',
  format = 'json'
);

INSERT INTO topic_events
SELECT
  session_id,
  "timestamp",
  topic,
  message_index,
  decision_node_id,
  content
FROM session_rows
WHERE role = 'user'
  AND topic IS NOT NULL
  AND topic <> '';
