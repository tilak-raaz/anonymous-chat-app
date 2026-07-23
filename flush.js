const { createClient } = require("redis");

async function nukeRedis() {
  const client = createClient({ url: "redis://localhost:6379" });

  await client.connect();
  console.log("🟢 Connected to Redis...");

  await client.flushAll();
  console.log("💥 BOOM! Redis database completely wiped and reset.");

  await client.quit();
  process.exit(0);
}

nukeRedis();
