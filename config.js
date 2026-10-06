// Central place for every setting that differs between local, Docker and EC2.
// Defaults match the docker-compose service names, so nothing changes there.
const int = (name, fallback) => {
  const value = Number.parseInt(process.env[name], 10);
  return Number.isFinite(value) ? value : fallback;
};

module.exports = {
  REDIS_URL: process.env.REDIS_URL || "redis://redis:6379",
  MATCHMAKER_URL: process.env.MATCHMAKER_URL || "http://matchmaker:3001",
  MATCHMAKER_PORT: int("MATCHMAKER_PORT", 3001),
  MATCHMAKER_TIMEOUT_MS: int("MATCHMAKER_TIMEOUT_MS", 3000),

  // Abuse limits for a single WebSocket connection
  MAX_PAYLOAD_BYTES: int("MAX_PAYLOAD_BYTES", 16 * 1024), // whole frame
  MAX_MESSAGE_LENGTH: int("MAX_MESSAGE_LENGTH", 1000), // characters of chat text
  RATE_LIMIT_MAX: int("RATE_LIMIT_MAX", 20), // packets...
  RATE_LIMIT_WINDOW_MS: int("RATE_LIMIT_WINDOW_MS", 10_000), // ...per window

  // Liveness. Sockets that miss a pong are dropped; route keys of users on a
  // node that crashed expire on their own because nobody refreshes them.
  HEARTBEAT_INTERVAL_MS: int("HEARTBEAT_INTERVAL_MS", 30_000),
  ROUTE_TTL_SECONDS: int("ROUTE_TTL_SECONDS", 90),

  // Upper bound on chat_history_log entries kept in Redis memory
  STREAM_MAX_LENGTH: int("STREAM_MAX_LENGTH", 100_000),

  // Worker: take over entries a crashed worker left un-acked
  WORKER_CLAIM_IDLE_MS: int("WORKER_CLAIM_IDLE_MS", 60_000),
  WORKER_CLAIM_INTERVAL_MS: int("WORKER_CLAIM_INTERVAL_MS", 30_000),

  DB: {
    host: process.env.DB_HOST || "postgres",
    port: int("DB_PORT", 5432),
    user: process.env.DB_USER || "postgres",
    password: process.env.DB_PASSWORD || "postgres",
    database: process.env.DB_NAME || "anonymous_db",
  },
};
