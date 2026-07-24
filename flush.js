const { createClient } = require("redis");

async function nukeRedis() {
  const client = createClient({ url: "redis://localhost:6379" });

  await client.connect();

  await client.flushAll();

  await client.quit();
  process.exit(0);
}

nukeRedis();
