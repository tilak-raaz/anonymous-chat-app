const os = require("os");
const { createClient } = require("redis");
const { Pool } = require("pg");
const config = require("./config");

// PostgreSQL Connection Pool
const pgPool = new Pool(config.DB);
pgPool.on("error", (err) => console.error("Postgres pool error:", err.message));

// Redis Connection
const redis = createClient({ url: config.REDIS_URL });
redis.on("error", (err) => console.error("Redis Client Error:", err));

const STREAM_KEY = "chat_history_log";
const DEAD_LETTER_KEY = "chat_history_dead";
const GROUP_NAME = "db_writers";
// Unique per container (Docker sets the hostname to the container id), so you
// can run several workers without them stealing each other's pending entries.
const CONSUMER_NAME = process.env.WORKER_NAME || `worker_${os.hostname()}`;

const BATCH_SIZE = 50;
const BLOCK_MS = 5000;
const RETRY_DELAY_MS = 1000;
// Entries a crashed consumer left un-acked for this long get taken over.
const CLAIM_IDLE_MS = config.WORKER_CLAIM_IDLE_MS;
const CLAIM_INTERVAL_MS = config.WORKER_CLAIM_INTERVAL_MS;

let running = true;

async function ensureSchema() {
  // Idempotent: safe on a fresh database and on one where the table was
  // created by hand earlier. stream_id makes inserts idempotent, because the
  // stream gives at-least-once delivery and a retry may replay a batch.
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS chat_history (
      id         BIGSERIAL PRIMARY KEY,
      room_id    TEXT NOT NULL,
      user_id    TEXT NOT NULL,
      message    TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS stream_id TEXT;
    CREATE UNIQUE INDEX IF NOT EXISTS chat_history_stream_id_key
      ON chat_history (stream_id);
    CREATE INDEX IF NOT EXISTS chat_history_room_id_idx
      ON chat_history (room_id);
  `);
  console.log("chat_history schema ready");
}

async function setupRedisGroup() {
  try {
    await redis.xGroupCreate(STREAM_KEY, GROUP_NAME, "0", { MKSTREAM: true });
    console.log(`consumer group '${GROUP_NAME}' created`);
  } catch (err) {
    if (err.message.includes("BUSYGROUP")) {
      console.log(`consumer group '${GROUP_NAME}' already exists`);
    } else {
      throw err;
    }
  }
}

// Postgres error classes 22 (bad data) and 23 (constraint violation) will fail
// forever no matter how often we retry. Anything else (connection refused,
// timeout, ...) is temporary.
function isPermanentDbError(err) {
  return typeof err.code === "string" && /^2[23]/.test(err.code);
}

async function insertBatch(messages) {
  const values = [];
  const rows = messages.map((msg, i) => {
    const { roomId, userId, text } = msg.message;
    values.push(msg.id, roomId, userId, text);
    const o = i * 4;
    return `($${o + 1}, $${o + 2}, $${o + 3}, $${o + 4})`;
  });

  await pgPool.query(
    `INSERT INTO chat_history (stream_id, room_id, user_id, message)
     VALUES ${rows.join(", ")}
     ON CONFLICT (stream_id) DO NOTHING`,
    values,
  );
}

async function deadLetter(msg, err) {
  console.error(`dead-lettering ${msg.id}:`, err.message);
  await redis.xAdd(DEAD_LETTER_KEY, "*", {
    originalId: msg.id,
    error: err.message,
    payload: JSON.stringify(msg.message),
  });
}

/**
 * Persist and acknowledge a batch. Returns false if the database is
 * unavailable (nothing acked, entries stay pending and will be retried).
 */
async function processMessages(messages) {
  // XAUTOCLAIM can return null for entries that were trimmed meanwhile
  const valid = messages.filter(Boolean);
  if (valid.length === 0) return true;

  try {
    await insertBatch(valid);
  } catch (err) {
    if (!isPermanentDbError(err)) {
      console.error("worker Error (will retry):", err.message);
      return false;
    }
    // One bad row must not block the whole stream: retry row by row and
    // move only the poison entries aside.
    for (const msg of valid) {
      try {
        await insertBatch([msg]);
      } catch (rowErr) {
        if (!isPermanentDbError(rowErr)) return false;
        await deadLetter(msg, rowErr);
      }
    }
  }

  // Ack only after the rows are committed: a crash before this line means
  // the entries are redelivered, and ON CONFLICT drops the duplicates.
  await redis.xAck(STREAM_KEY, GROUP_NAME, valid.map((m) => m.id));
  return true;
}

async function readBatch(startId) {
  const response = await redis.xReadGroup(
    GROUP_NAME,
    CONSUMER_NAME,
    [{ key: STREAM_KEY, id: startId }],
    // Only block when waiting for new entries; our own backlog is instant.
    startId === ">" ? { COUNT: BATCH_SIZE, BLOCK: BLOCK_MS } : { COUNT: BATCH_SIZE },
  );
  return response ? response[0].messages : [];
}

/** Take over entries left pending by a consumer that crashed. */
async function claimAbandoned() {
  let cursor = "0-0";
  do {
    const { nextId, messages } = await redis.xAutoClaim(
      STREAM_KEY,
      GROUP_NAME,
      CONSUMER_NAME,
      CLAIM_IDLE_MS,
      cursor,
      { COUNT: BATCH_SIZE },
    );
    if (messages.length > 0) {
      console.log(`claimed ${messages.length} abandoned entries`);
      if (!(await processMessages(messages))) return;
    }
    cursor = nextId;
  } while (running && cursor !== "0-0");
}

async function startWorker() {
  await redis.connect();
  console.log("Redis connected.");
  await setupRedisGroup();
  await ensureSchema();

  console.log(` ${CONSUMER_NAME} is listening for messages`);

  // "0" = our own pending entries (delivered before but never acked, e.g. we
  // crashed or the DB was down). ">" = brand new entries. ">" alone would
  // never look at the pending ones again, so they would be stuck forever.
  let startId = "0";
  let lastClaim = 0;

  while (running) {
    try {
      if (Date.now() - lastClaim > CLAIM_INTERVAL_MS) {
        lastClaim = Date.now();
        await claimAbandoned();
      }

      const messages = await readBatch(startId);

      if (startId === "0" && messages.length === 0) {
        startId = ">"; // backlog drained, switch to new entries
        continue;
      }

      if (!(await processMessages(messages))) {
        startId = "0"; // go back and retry what is still pending
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      }
    } catch (error) {
      if (String(error.message).includes("NOGROUP")) {
        // Redis lost the stream/group (restart without persistence, FLUSHALL).
        // Recreate it instead of failing forever.
        console.warn("consumer group missing, recreating it");
        await setupRedisGroup().catch((err) => console.error(err.message));
      } else {
        console.error("worker Error:", error);
      }
      startId = "0";
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}

async function shutdown() {
  console.log("worker shutting down");
  running = false;
  setTimeout(() => process.exit(0), BLOCK_MS + 1000).unref();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

startWorker().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
