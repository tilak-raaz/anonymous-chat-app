import ws from "k6/ws";
import { check } from "k6";
import exec from "k6/execution";

// The Attack Plan: 30s warmup, 60s ramp up to 500 users/sec, 60s sustained
export const options = {
  stages: [
    { duration: "30s", target: 500 }, // Fast warmup
    { duration: "60s", target: 1000 }, // 5x the previous maximum load
    { duration: "60s", target: 1000 }, // Sustained punishment
    { duration: "30s", target: 0 },
  ],
};

export default function () {
  const url = "ws://localhost:8080";

  // Generate a unique ID using the k6 Virtual User ID and the current timestamp
  const userId = `k6_user_${exec.vu.idInTest}_${Date.now()}`;

  const res = ws.connect(url, null, function (socket) {
    socket.on("open", () => {
      // 1. Immediately request a match upon connection
      socket.send(
        JSON.stringify({
          action: "find_match",
          userId: userId,
        }),
      );
    });

    socket.on("message", (msgStr) => {
      const msg = JSON.parse(msgStr);

      // 2. Wait for the atomic Matchmaker to pair us up
      if (msg.action === "matched") {
        // Wait 2 seconds, then send a message to the stranger
        socket.setTimeout(function () {
          socket.send(
            JSON.stringify({
              action: "send",
              text: "Hello from the k6 multi-threaded load test!",
            }),
          );
        }, 2000);

        // Keep the chat room open for 5 more seconds, then disconnect cleanly
        socket.setTimeout(function () {
          socket.close();
        }, 7000);
      }
    });

    // Failsafe: If the server freezes and we don't get matched in 15 seconds, drop out
    socket.setTimeout(function () {
      socket.close();
    }, 15000);
  });

  // Verify that the WebSocket handshake actually succeeded
  check(res, { "Connected successfully": (r) => r && r.status === 101 });
}
