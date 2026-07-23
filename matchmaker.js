const express = require("express");
const { createClient } = require("redis");

const app = express();
const PORT = 3001;

// Middleware to parse incoming JSON payloads from the Chat Servers
app.use(express.json());

// Initialize Redis Client
const redis =  createClient({ url: "redis://redis:6379" });

redis.on("error", (err) => console.error("❌ Redis Client Error:", err));

async function startMatchmaker() {
  // Wait for Redis to connect before booting the API
  await redis.connect();
  console.log("🟢 Matchmaker connected to Redis.");

  // --- THE MATCHMAKING ROUTE ---
  app.post("/find-match", async (req, res) => {
    const { userId, serverId } = req.body;

    // 1. Instantly free the chat server so it doesn't block its event loop
    res.status(200).json({ status: "processing" });

    try {
      // 2. Set the routing GPS (which server this user is connected to)
      await redis.set(`route:${userId}`, serverId);

      // 3. Add to Sorted Set
      await redis.zAdd(
        "waiting_pool",
        { score: Date.now(), value: userId },
        { NX: true },
      );

      // 🏆 THE ATOMIC FIX: Pop and remove the top 2 users in a single atomic action.
      // This completely prevents the Race Condition under heavy load!
      
      const matchedUsers = await redis.zPopMinCount("waiting_pool", 2);

      // 4. If we successfully grabbed exactly 2 users, match them!
      if (matchedUsers && matchedUsers.length === 2) {
        const userA = matchedUsers[0].value;
        const userB = matchedUsers[1].value;

        const roomId = "room_" + Math.floor(Math.random() * 100000);

        const serverForA = await redis.get(`route:${userA}`);
        const serverForB = await redis.get(`route:${userB}`);

        // Fire the events back to the specific Chat Servers those users are on
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

        console.log(`🔗 Successfully Matched ${userA} & ${userB} -> ${roomId}`);
      }
      // Edge Case: The pool had an odd number of people, and we only popped 1
      else if (matchedUsers && matchedUsers.length === 1) {
        // Put that single user safely back into the queue with their original score
        await redis.zAdd("waiting_pool", {
          score: matchedUsers[0].score,
          value: matchedUsers[0].value,
        });
      }
    } catch (err) {
      console.error("❌ Matchmaker Error:", err);
    }
  });

  // Start the Express Server
  app.listen(PORT, () => {
    console.log(`🚀 Matchmaker API is running on http://localhost:${PORT}`);
  });
}

// Boot up the microservice
startMatchmaker();
