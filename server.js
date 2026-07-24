const { WebSocketServer } = require("ws");
const { createClient } = require("redis");

const PORT = process.argv[2] || 8081;
const SERVER_ID = `server_events_${PORT}`;
const MATCHMAKER_URL = "http://matchmaker:3001/find-match";

async function startServer() {
  const publisher = createClient({ url: "redis://redis:6379" });
  const subscriber = publisher.duplicate();

  await publisher.connect();
  await subscriber.connect();

  const wss = new WebSocketServer({
    port: PORT,
  });

  const localRooms = new Map();
  const localUsers = new Map();

  await subscriber.subscribe(SERVER_ID, (eventStr) => {
    const event = JSON.parse(eventStr);
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
    console.log(` [${SERVER_ID}] Client connected.`);

    ws.on("message", async (rawData) => {
      const packet = JSON.parse(rawData.toString());

      if (packet.action === "find_match") {
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

        ws.send(
          JSON.stringify({
            system: true,
            action: "searching",
            text: `Searching for a stranger`,
          }),
        );

        try {
          const response = await fetch(MATCHMAKER_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ userId: myUserId, serverId: SERVER_ID }),
          });
          const data = await response.json();
        } catch (err) {
          console.error("failed to contact Matchmaker API:", err);
        }
      }

      if (packet.action === "send") {
        if (!ws.currentRoom) return ws.send("Error: Not in a room!");

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
    });

    ws.on("close", async () => {
      console.log(` [${SERVER_ID}] ${myUserId || "unknown"} disconnected.`);

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
    });
  });

  console.log(`Node [${SERVER_ID}] live on ws://localhost:${PORT}`);
}

startServer();
