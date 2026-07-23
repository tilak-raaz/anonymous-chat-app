const { createClient } = require("redis");
const { Pool } = require("pg");

// 1. PostgreSQL Connection Pool
const pgPool = new Pool({
  host: process.env.DB_HOST || "postgres",
  port: 5432,
  user: process.env.DB_USER || "postgres",
  password: process.env.DB_PASSWORD || "postgres",
  database: process.env.DB_NAME || "anonymous_db",
});

// 2. Redis Connection
const redis = createClient({ url: "redis://redis:6379" });

const STREAM_KEY = "chat_history_log";
const GROUP_NAME = "db_writers";
const CONSUMER_NAME = "worker_1";

async function setupRedisGroup() {
  try {
    await redis.xGroupCreate(STREAM_KEY, GROUP_NAME, "0", { MKSTREAM: true });
    console.log(`📦 Consumer group '${GROUP_NAME}' created.`);
  } catch (err) {
    if (err.message.includes("BUSYGROUP")) {
      console.log(`✅ Consumer group '${GROUP_NAME}' already exists.`);
    } else {
      throw err;
    }
  }
}

async function startWorker() {
  await redis.connect();
  console.log("🟢 Redis connected.");
  await setupRedisGroup();

  console.log(`🚀 ${CONSUMER_NAME} is listening for messages...`);

  while (true) {
    try {
      const response = await redis.xReadGroup(
        GROUP_NAME,
        CONSUMER_NAME,
        [{ key: STREAM_KEY, id: ">" }],
        { COUNT: 10, BLOCK: 5000 },
      );

      if (response) {
        const messages = response[0].messages;

        for (const msg of messages) {
          const redisMessageId = msg.id;
          const { roomId, userId, text } = msg.message;

          // 4. Write to PostgreSQL using pgPool
          await pgPool.query(
            "INSERT INTO chat_history (room_id, user_id, message) VALUES ($1, $2, $3)",
            [roomId, userId, text],
          );

          // 5. Acknowledge (XACK)
          await redis.xAck(STREAM_KEY, GROUP_NAME, redisMessageId);

          console.log(`💾 Saved & Ack'd msg from ${userId} in ${roomId}`);
        }
      }
    } catch (error) {
      console.error("❌ Worker Error:", error);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

startWorker();
