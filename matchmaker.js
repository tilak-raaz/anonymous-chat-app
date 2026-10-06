const express = require("express");
const { createClient } = require("redis");
const config = require("./config");

const app = express();
const PORT = config.MATCHMAKER_PORT;

app.use(express.json());

const redis = createClient({ url: config.REDIS_URL });

redis.on("error", (err) => console.error("Redis Client Error:", err));

async function startMatchmaker() {
  await redis.connect();

  app.post("/find-match", async (req, res) => {
    const { userId, serverId } = req.body || {};
    if (typeof userId !== "string" || typeof serverId !== "string") {
      return res.status(400).json({ error: "userId and serverId are required" });
    }
    res.status(200).json({ status: "processing" });

    try {
      await redis.set(`route:${userId}`, serverId);
      await redis.zAdd(
        "waiting_pool",
        { score: Date.now(), value: userId },
        { NX: true },
      );

      const matchedUsers = await redis.zPopMinCount("waiting_pool", 2);

      if (matchedUsers && matchedUsers.length === 2) {
        const userA = matchedUsers[0].value;
        const userB = matchedUsers[1].value;

        const roomId = "room_" + Math.floor(Math.random() * 100000);

        const serverForA = await redis.get(`route:${userA}`);
        const serverForB = await redis.get(`route:${userB}`);

        if (serverForA) {
          await redis.publish(
            serverForA,
            JSON.stringify({ userId: userA, roomId }),
          );
        }
        if (serverForB) {
          await redis.publish(
            serverForB,
            JSON.stringify({ userId: userB, roomId }),
          );
        }

        console.log(`successfully Matched ${userA} & ${userB} -> ${roomId}`);
      } else if (matchedUsers && matchedUsers.length === 1) {
        await redis.zAdd("waiting_pool", {
          score: matchedUsers[0].score,
          value: matchedUsers[0].value,
        });
      }
    } catch (err) {
      console.error("❌ Matchmaker Error:", err);
    }
  });
  app.listen(PORT, () => {
    console.log(`🚀 Matchmaker API is running on http://localhost:${PORT}`);
  });
}
startMatchmaker().catch((err) => {
  console.error("Failed to start matchmaker:", err);
  process.exit(1);
});
