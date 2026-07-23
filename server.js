const { WebSocketServer } = require("ws");
const { createClient } = require("redis");

const PORT = process.argv[2] || 8080;
const SERVER_ID = `server_events_${PORT}`;
const MATCHMAKER_URL = "http://matchmaker:3001/find-match";

async function startServer() {
  const publisher = createClient({ url: "redis://redis:6379" });
  const subscriber = publisher.duplicate();

  await publisher.connect();
  await subscriber.connect();

  const wss = new WebSocketServer({
     port: PORT });

  const localRooms = new Map();
  const localUsers = new Map();

  // Listen ONLY to this server's specific command channel
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
         action: "matched", // ADD THIS LINE
         text: `Match found! You are in ${event.roomId}`,
       }),
     );
    }
  });
 

  wss.on("connection", (ws) => {
    let myUserId = null;
    console.log(`🟢 [${SERVER_ID}] Client connected.`);

    ws.on("message", async (rawData) => {
      const packet = JSON.parse(rawData.toString());

      // --- ACTION: JOIN QUEUE (TRIGGER MATCHMAKER API) ---
      // --- ACTION: JOIN QUEUE (TRIGGER MATCHMAKER API) ---
      if (packet.action === "find_match") {
        myUserId = packet.userId;
        localUsers.set(myUserId, ws);

        // 🚨 THE FIX: State Transition Cleanup 🚨
        // If they are already in a room and hitting "find_match" again,
        // we must explicitly remove them from the old room first!
        if (ws.currentRoom && localRooms.has(ws.currentRoom)) {
          const oldRoomSet = localRooms.get(ws.currentRoom);
          oldRoomSet.delete(ws);

          // If this server has no more users in that old room, kill the Redis listener
          if (oldRoomSet.size === 0) {
            localRooms.delete(ws.currentRoom);
            await subscriber.unsubscribe(ws.currentRoom);
            console.log(
              `🗑️ Unsubscribed from abandoned room: ${ws.currentRoom}`,
            );
          }

          // Wipe the sticky note so they are completely clean for the new match
          ws.currentRoom = null;
        }

       ws.send(
         JSON.stringify({
           system: true,
           action: "searching", // ADD THIS LINE
           text: `Searching for a stranger...`,
         }),
       );

        // Fire the HTTP request to the Matchmaker Service
        try {
          console.log("making req")
          const response = await fetch(MATCHMAKER_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ userId: myUserId, serverId: SERVER_ID })
          })
          const data = await response.json();
          console.log("✅ Matchmaker API responded:", data);
        } catch (err) {
          console.error("Failed to contact Matchmaker API:", err);
        }
        console.log("api requested");
      }

      // --- ACTION: SEND MESSAGE ---
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
    })
   
    ws.on("close", async () => {
      console.log(`🔴 [${SERVER_ID}] ${myUserId || "Unknown"} disconnected.`);

      if (myUserId) {
        // 1. Remove from local memory and global routing table
        localUsers.delete(myUserId);
        await publisher.del(`route:${myUserId}`);

        // 2. Queue Drop Scenario
        if (!ws.currentRoom) {
          // If they don't have a room, they might be in the Matchmaker queue.
          // ZREM atomically removes them so the Matchmaker can't pop them.
          await publisher.zRem("waiting_pool", myUserId);
          console.log(`🧹 Removed ghost ${myUserId} from waiting queue.`);
        }

        // 3. Active Chat Room Scenario
        else {
          // Notify the room that this user has bailed out
          await publisher.publish(
            ws.currentRoom,
            JSON.stringify({
              system: true,
              action: "partner_left",
              text: "Stranger has disconnected.",
            }),
          );

          // Clean up the local room memory
          if (localRooms.has(ws.currentRoom)) {
            const roomSet = localRooms.get(ws.currentRoom);
            roomSet.delete(ws);

            // 4. Prevent the RAM Leak!
            // If this server no longer has ANY users left in this specific room,
            // we must tell Redis to stop sending us messages for it.
            if (roomSet.size === 0) {
              localRooms.delete(ws.currentRoom);
              await subscriber.unsubscribe(ws.currentRoom);
              console.log(`🗑️ Unsubscribed from dead room: ${ws.currentRoom}`);
            }
          }
        }
      }
    });
  })

    console.log(`🚀 Node [${SERVER_ID}] live on ws://localhost:${PORT}`);
}


startServer();