const { randomUUID } = require("crypto");
const { WebSocketServer } = require("ws");
const { createClient } = require("redis");
const config = require("./config");
const { parsePacket, createRateLimiter } = require("./protocol");

const PORT = Number(process.argv[2] || 8081);
const SERVER_ID = `server_events_${PORT}`;
const MATCHMAKER_URL = `${config.MATCHMAKER_URL}/find-match`;
const WAITING_POOL = "waiting_pool";
// Marks a room as finished so a late joiner doesn't sit in a dead room.
const ROOM_CLOSED_TTL_SECONDS = 600;

// A bug in one handler must never take the whole node (and every socket on it) down.
process.on("unhandledRejection", (err) => {
  console.error(` [${SERVER_ID}] Unhandled rejection:`, err);
});

function sendJson(ws, payload) {
  if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

function sendSystem(ws, action, text) {
  sendJson(ws, { system: true, action, text });
}

function sendError(ws, text) {
  sendSystem(ws, "error", text);
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

  /**
   * roomId -> { clients: Set<ws>, ready: Promise }
   * `ready` resolves once Redis has confirmed the SUBSCRIBE, so nothing that
   * is published to the room after that point can be missed.
   */
  const localRooms = new Map();
  /** sessionId -> ws, for every socket connected to this node */
  const localUsers = new Map();

  // ---------------------------------------------------------------- rooms

  /**
   * Room channels carry the sender's internal session id. It must never reach
   * a browser: whoever learns it could impersonate that user. So every event
   * is rewritten per recipient (`from: "me" | "stranger"`).
   */
  function handleRoomEvent(roomId, rawEvent) {
    const room = localRooms.get(roomId);
    if (!room) return;

    let event;
    try {
      event = JSON.parse(rawEvent);
    } catch {
      return console.error("Ignoring malformed room event:", rawEvent);
    }

    for (const client of [...room.clients]) {
      const isSender = event.from === client.sessionId;

      if (event.kind === "chat") {
        sendJson(client, {
          id: event.id,
          from: isSender ? "me" : "stranger",
          text: event.text,
          ts: event.ts,
        });
      } else if (event.kind === "typing" && !isSender) {
        sendJson(client, { action: "typing" });
      } else if (event.kind === "left" && !isSender) {
        // Our partner is gone: free this socket so it can't keep sending
        // messages into a dead room.
        client.state = "idle";
        client.currentRoom = null;
        client.partnerId = null;
        detachFromRoom(client, roomId);
        sendSystem(client, "partner_left", "Stranger has disconnected.");
      }
    }
  }

  async function attachToRoom(ws, roomId) {
    let room = localRooms.get(roomId);
    if (!room) {
      room = { clients: new Set(), ready: null };
      room.ready = subscriber.subscribe(roomId, (raw) =>
        handleRoomEvent(roomId, raw),
      );
      localRooms.set(roomId, room);
    }
    room.clients.add(ws);
    await room.ready;
  }

  function detachFromRoom(ws, roomId) {
    const room = localRooms.get(roomId);
    if (!room) return;
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      localRooms.delete(roomId);
      subscriber
        .unsubscribe(roomId)
        .then(() => console.log(` unsubscribed from finished room: ${roomId}`))
        .catch((err) => console.error("Unsubscribe failed:", err));
    }
  }

  /** Close a room on behalf of `userId` and tell the other side. */
  async function closeRoom(roomId, userId) {
    // Set the marker first: a partner whose join is still in flight will see
    // it, and a partner that already joined will get the publish below.
    await publisher.set(`room_closed:${roomId}`, "1", {
      EX: ROOM_CLOSED_TTL_SECONDS,
    });
    await publisher.publish(roomId, JSON.stringify({ kind: "left", from: userId }));
  }

  async function leaveRoom(ws) {
    const roomId = ws.currentRoom;
    if (!roomId) return;
    ws.state = "idle";
    ws.currentRoom = null;
    ws.partnerId = null;
    await closeRoom(roomId, ws.sessionId);
    detachFromRoom(ws, roomId);
  }

  // ---------------------------------------------------------- matchmaking

  async function requestMatch(ws) {
    try {
      const response = await fetch(MATCHMAKER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: ws.sessionId, serverId: SERVER_ID }),
        signal: AbortSignal.timeout(config.MATCHMAKER_TIMEOUT_MS),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
    } catch (err) {
      console.error("failed to contact Matchmaker API:", err.message);
      // We might already be queued; don't leave a half-registered user behind.
      await publisher.zRem(WAITING_POOL, ws.sessionId).catch(() => {});
      if (ws.state === "searching") ws.state = "idle";
      sendError(ws, "Matchmaking is unavailable right now. Try again.");
    }
  }

  async function handleMatched({ userId, partnerId, roomId }) {
    const ws = localUsers.get(userId);

    // The user disconnected, cancelled or skipped while the match was being
    // made. Decline it so the partner isn't left waiting in an empty room.
    if (!ws || ws.readyState !== 1 || ws.state !== "searching") {
      return closeRoom(roomId, userId);
    }

    ws.state = "chatting";
    ws.currentRoom = roomId;
    ws.partnerId = partnerId;
    await attachToRoom(ws, roomId);

    if (await publisher.exists(`room_closed:${roomId}`)) {
      // Partner was gone before we even arrived: quietly search again
      // instead of showing "matched" followed by "disconnected".
      detachFromRoom(ws, roomId);
      ws.currentRoom = null;
      ws.partnerId = null;
      ws.state = "searching";
      return requestMatch(ws);
    }

    sendSystem(ws, "matched", "You are now chatting with a stranger. Say hi!");
  }

  await subscriber.subscribe(SERVER_ID, (eventStr) => {
    let event;
    try {
      event = JSON.parse(eventStr);
    } catch {
      return console.error("Ignoring malformed server event:", eventStr);
    }
    if (event.type === "matched") {
      handleMatched(event).catch((err) =>
        console.error(` [${SERVER_ID}] Failed to handle match:`, err),
      );
    }
  });

  // ------------------------------------------------------------ heartbeat

  /**
   * Every HEARTBEAT_INTERVAL_MS:
   *  1. Ping every socket; one that didn't answer the previous ping is a dead
   *     TCP connection (closed laptop, lost network) and gets terminated,
   *     which runs the normal close cleanup.
   *  2. Refresh route keys of our users (one pipelined round trip).
   *  3. For users in a chat, check the partner's route key still exists. If
   *     it expired, the partner's node crashed and nobody will ever send us
   *     "left", so we end the chat ourselves.
   */
  async function heartbeat() {
    const chatting = [];
    const refresh = publisher.multi();
    let refreshed = 0;

    for (const ws of localUsers.values()) {
      if (ws.isAlive === false) {
        console.log(` [${SERVER_ID}] ${ws.sessionId} missed a heartbeat, dropping.`);
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
      refresh.expire(`route:${ws.sessionId}`, config.ROUTE_TTL_SECONDS);
      refreshed += 1;
      if (ws.state === "chatting" && ws.partnerId) chatting.push(ws);
    }

    if (refreshed > 0) await refresh.execAsPipeline();
    if (chatting.length === 0) return;

    const probe = publisher.multi();
    chatting.forEach((ws) => probe.exists(`route:${ws.partnerId}`));
    const alive = await probe.execAsPipeline();

    chatting.forEach((ws, i) => {
      if (alive[i] || ws.state !== "chatting") return;
      const roomId = ws.currentRoom;
      console.log(` [${SERVER_ID}] partner of ${ws.sessionId} vanished, closing ${roomId}`);
      ws.state = "idle";
      ws.currentRoom = null;
      ws.partnerId = null;
      detachFromRoom(ws, roomId);
      sendSystem(ws, "partner_left", "Stranger has disconnected.");
      closeRoom(roomId, ws.sessionId).catch(() => {});
    });
  }

  const heartbeatTimer = setInterval(() => {
    heartbeat().catch((err) => console.error(` [${SERVER_ID}] Heartbeat failed:`, err));
  }, config.HEARTBEAT_INTERVAL_MS);
  wss.on("close", () => clearInterval(heartbeatTimer));

  // -------------------------------------------------------------- sockets

  const handlers = {
    async find_match(ws) {
      if (ws.state === "searching") return; // double click: already queued
      if (ws.state === "chatting") await leaveRoom(ws); // "next stranger"

      ws.state = "searching";
      sendSystem(ws, "searching", "Searching for a stranger");
      await requestMatch(ws);
    },

    async cancel_search(ws) {
      if (ws.state !== "searching") return;
      ws.state = "idle";
      // If a match is already in flight, handleMatched() sees state "idle"
      // and declines it, so cancelling is always safe.
      await publisher.zRem(WAITING_POOL, ws.sessionId);
      sendSystem(ws, "search_cancelled", "Stopped searching.");
    },

    async leave(ws) {
      if (ws.state !== "chatting") return;
      await leaveRoom(ws);
      sendSystem(ws, "left", "You left the chat.");
    },

    async send(ws, packet) {
      if (ws.state !== "chatting") return sendError(ws, "You are not in a chat.");

      await publisher.publish(
        ws.currentRoom,
        JSON.stringify({
          kind: "chat",
          id: randomUUID(),
          from: ws.sessionId,
          text: packet.text,
          ts: Date.now(),
        }),
      );
      await publisher.xAdd(
        "chat_history_log",
        "*",
        {
          roomId: ws.currentRoom,
          userId: ws.sessionId,
          text: packet.text,
        },
        // Cap the stream's memory. "~" lets Redis trim in cheap whole-node
        // steps. Only matters if the worker falls this far behind.
        {
          TRIM: {
            strategy: "MAXLEN",
            strategyModifier: "~",
            threshold: config.STREAM_MAX_LENGTH,
          },
        },
      );
    },

    async typing(ws) {
      if (ws.state !== "chatting") return;
      // Ephemeral: relayed to the partner, never persisted.
      await publisher.publish(
        ws.currentRoom,
        JSON.stringify({ kind: "typing", from: ws.sessionId }),
      );
    },
  };

  wss.on("connection", (ws) => {
    // Identity is decided by the server, never by the client. A client-chosen
    // id could be set to someone else's id to hijack their match.
    ws.sessionId = randomUUID();
    ws.state = "idle"; // idle -> searching -> chatting -> idle
    ws.currentRoom = null;
    ws.partnerId = null;
    ws.isAlive = true;
    ws.on("pong", () => {
      ws.isAlive = true;
    });
    localUsers.set(ws.sessionId, ws);

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

        await handlers[parsed.packet.action](ws, parsed.packet);
      } catch (err) {
        console.error(` [${SERVER_ID}] Failed to handle packet:`, err);
        sendError(ws, "Something went wrong on our side.");
      }
    });

    ws.on("close", async () => {
      console.log(` [${SERVER_ID}] ${ws.sessionId} disconnected.`);
      localUsers.delete(ws.sessionId);

      try {
        const wasSearching = ws.state === "searching";
        ws.state = "closed";
        await publisher.del(`route:${ws.sessionId}`);

        if (wasSearching) await publisher.zRem(WAITING_POOL, ws.sessionId);
        if (ws.currentRoom) await leaveRoom(ws);
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
