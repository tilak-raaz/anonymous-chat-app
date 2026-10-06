const { randomUUID } = require("crypto");
const express = require("express");
const { createClient } = require("redis");
const config = require("./config");

const app = express();
const PORT = config.MATCHMAKER_PORT;
const WAITING_POOL = "waiting_pool";
const ROUTE_PREFIX = "route:";

app.use(express.json());

const redis = createClient({ url: config.REDIS_URL });

redis.on("error", (err) => console.error("Redis Client Error:", err));

/**
 * Enqueue + pair in ONE atomic step.
 *
 * Doing ZADD, ZPOPMIN and "put the single user back" as separate commands
 * leaves a gap: with two matchmaker replicas, A can pop itself, B can pop
 * itself, both get pushed back and wait forever even though two people are
 * in the queue. Redis runs a Lua script without interleaving any other
 * command, so that gap disappears.
 *
 * It also skips "ghosts": users whose route key is gone (they disconnected,
 * or their chat node died) are dropped instead of being matched.
 *
 * Note: route keys are built inside the script instead of being passed in
 * KEYS. That is fine on a single Redis instance (what we run) but would need
 * hash tags to work on Redis Cluster.
 *
 * KEYS[1] = waiting pool, ARGV[1] = userId, ARGV[2] = score, ARGV[3] = route prefix
 * Returns {} (keep waiting) or {userA, userB}.
 */
const MATCH_SCRIPT = `
redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[1])
while true do
  local popped = redis.call('ZPOPMIN', KEYS[1], 2)
  if #popped < 4 then
    if #popped == 2 then
      redis.call('ZADD', KEYS[1], popped[2], popped[1])
    end
    return {}
  end
  local a, b = popped[1], popped[3]
  local aAlive = redis.call('EXISTS', ARGV[3] .. a) == 1
  local bAlive = redis.call('EXISTS', ARGV[3] .. b) == 1
  if aAlive and bAlive then
    return {a, b}
  end
  if aAlive then redis.call('ZADD', KEYS[1], popped[2], a) end
  if bAlive then redis.call('ZADD', KEYS[1], popped[4], b) end
end
`;

async function notifyMatch(userId, partnerId, roomId) {
  const serverId = await redis.get(`${ROUTE_PREFIX}${userId}`);
  if (serverId) {
    await redis.publish(
      serverId,
      JSON.stringify({ type: "matched", userId, partnerId, roomId }),
    );
  }
}

async function startMatchmaker() {
  await redis.connect();

  app.post("/find-match", async (req, res) => {
    const { userId, serverId } = req.body || {};
    if (typeof userId !== "string" || typeof serverId !== "string") {
      return res.status(400).json({ error: "userId and serverId are required" });
    }

    try {
      // The chat node refreshes this TTL on every heartbeat. If the node dies,
      // the key expires and the Lua script treats the user as a ghost.
      await redis.set(`${ROUTE_PREFIX}${userId}`, serverId, {
        EX: config.ROUTE_TTL_SECONDS,
      });

      const pair = await redis.eval(MATCH_SCRIPT, {
        keys: [WAITING_POOL],
        arguments: [userId, String(Date.now()), ROUTE_PREFIX],
      });

      if (!pair || pair.length !== 2) {
        return res.status(200).json({ status: "waiting" });
      }

      const [userA, userB] = pair;
      // Math.random() * 100000 collides often (~70% chance among 500 live
      // rooms), which puts two pairs of strangers in the same room.
      const roomId = `room_${randomUUID()}`;

      await Promise.all([
        notifyMatch(userA, userB, roomId),
        notifyMatch(userB, userA, roomId),
      ]);

      console.log(`successfully Matched ${userA} & ${userB} -> ${roomId}`);
      res.status(200).json({ status: "matched" });
    } catch (err) {
      console.error("❌ Matchmaker Error:", err);
      res.status(500).json({ error: "matchmaking failed" });
    }
  });

  app.get("/health", (req, res) => res.json({ ok: redis.isReady }));

  app.listen(PORT, () => {
    console.log(`🚀 Matchmaker API is running on http://localhost:${PORT}`);
  });
}

startMatchmaker().catch((err) => {
  console.error("Failed to start matchmaker:", err);
  process.exit(1);
});
