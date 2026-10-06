const { WebSocketServer } = require("ws");
const { createClient } = require("redis");
const config = require("./config");
const { parsePacket, createRateLimiter } = require("./protocol");

const PORT = Number(process.argv[2] || 8081);
const SERVER_ID = `server_events_${PORT}`;
const MATCHMAKER_URL = `${config.MATCHMAKER_URL}/find-match`;

// A bug in one handler must never take the whole node (and every socket on it) down.
process.on("unhandledRejection", (err) => {
  console.error(` [${SERVER_ID}] Unhandled rejection:`, err);
});

function sendJson(ws, payload) {
  if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

function sendError(ws, text) {
  sendJson(ws, { system: true, action: "error", text });
}

async function startServer() {
  const publisher = createClient({ url: config.REDIS_URL });
  const subscriber = publisher.duplicate();

  // Without an 'error' listener a Redis hiccup becomes an uncaught exception.
  publisher.on("error", (err) => console.error("Redis publisher error:", err));
  subscriber.on("error", (err) => console.error("Redis subscriber error:", err));

  await publisher.connect();
  await subscriber.connect();

  const wss = new WebSocketServer({
    port: PORT,
    maxPayload: config.MAX_PAYLOAD_BYTES,
  });

  const localRooms = new Map();
  const localUsers = new Map();

  await subscriber.subscribe(SERVER_ID, (eventStr) => {
    let event;
    try {
      event = JSON.parse(eventStr);
    } catch {
      return console.error("Ignoring malformed server event:", eventStr);
    }
    const targetSocket = localUsers.get(event.userId);

    if (targetSocket && targetSocket.readyState === 1) {
      if (!localRooms.has(event.roomId)) {
        localRooms.set(event.roomId, new Set());

        subscriber.subscribe(event.roomId, (chatMsg) => {
          const clientsInRoom = localRooms.get(event.roomId);
          if (clientsInRoom) {
            clientsInRoom.forEach((client) => {
              if (client.readyState === 1) client.send(chatMsg);
            });
          }
        });
      }

      localRooms.get(event.roomId).add(targetSocket);
      targetSocket.currentRoom = event.roomId;

      targetSocket.send(
        JSON.stringify({
          system: true,
          action: "matched",
          text: `match found you are in ${event.roomId}`,
        }),
      );
    }
  });

  wss.on("connection", (ws) => {
    let myUserId = null;
    const allowPacket = createRateLimiter(
      config.RATE_LIMIT_MAX,
      config.RATE_LIMIT_WINDOW_MS,
    );
    console.log(` [${SERVER_ID}] Client connected.`);

    ws.on("error", (err) => {
      console.error(` [${SERVER_ID}] Socket error:`, err.message);
    });

    ws.on("message", async (rawData) => {
      try {
        if (!allowPacket()) {
          return sendError(ws, "Slow down! You are sending messages too fast.");
        }

        const parsed = parsePacket(rawData);
        if (!parsed.ok) return sendError(ws, parsed.error);
        const packet = parsed.packet;

        if (packet.action === "find_match") {
          if (typeof packet.userId !== "string" || !packet.userId) {
            return sendError(ws, "Missing userId.");
          }
          myUserId = packet.userId;
          localUsers.set(myUserId, ws);

          if (ws.currentRoom && localRooms.has(ws.currentRoom)) {
            const oldRoomSet = localRooms.get(ws.currentRoom);
            oldRoomSet.delete(ws);

            if (oldRoomSet.size === 0) {
              localRooms.delete(ws.currentRoom);
              await subscriber.unsubscribe(ws.currentRoom);
              console.log(` unsubscribed from abandoned room: ${ws.currentRoom}`);
            }

            ws.currentRoom = null;
          }

          sendJson(ws, {
            system: true,
            action: "searching",
            text: `Searching for a stranger`,
          });

          try {
            const response = await fetch(MATCHMAKER_URL, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ userId: myUserId, serverId: SERVER_ID }),
              signal: AbortSignal.timeout(config.MATCHMAKER_TIMEOUT_MS),
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
          } catch (err) {
            console.error("failed to contact Matchmaker API:", err.message);
            sendError(ws, "Matchmaking is unavailable right now. Try again.");
          }
        }

        if (packet.action === "send") {
          if (!ws.currentRoom) return sendError(ws, "You are not in a chat.");

          await publisher.publish(
            ws.currentRoom,
            JSON.stringify({
              from: myUserId,
              text: packet.text,
            }),
          );
          await publisher.xAdd("chat_history_log", "*", {
            roomId: ws.currentRoom,
            userId: myUserId,
            text: packet.text,
          });
        }
      } catch (err) {
        console.error(` [${SERVER_ID}] Failed to handle packet:`, err);
        sendError(ws, "Something went wrong on our side.");
      }
    });

    ws.on("close", async () => {
      console.log(` [${SERVER_ID}] ${myUserId || "unknown"} disconnected.`);

      try {
        if (myUserId) {
          localUsers.delete(myUserId);
          await publisher.del(`route:${myUserId}`);

          if (!ws.currentRoom) {
            await publisher.zRem("waiting_pool", myUserId);
          } else {
            await publisher.publish(
              ws.currentRoom,
              JSON.stringify({
                system: true,
                action: "partner_left",
                text: "Stranger has disconnected.",
              }),
            );

            if (localRooms.has(ws.currentRoom)) {
              const roomSet = localRooms.get(ws.currentRoom);
              roomSet.delete(ws);
              if (roomSet.size === 0) {
                localRooms.delete(ws.currentRoom);
                await subscriber.unsubscribe(ws.currentRoom);
                console.log(` unsubscribed from dead room: ${ws.currentRoom}`);
              }
            }
          }
        }
      } catch (err) {
        console.error(` [${SERVER_ID}] Cleanup failed:`, err);
      }
    });
  });

  console.log(`Node [${SERVER_ID}] live on ws://localhost:${PORT}`);
}

startServer().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
