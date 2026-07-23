const net = require("net");

// Fleet configuration with mutable health states
const BACKENDS = [
  { host: "node_a", port: 8081, alive: true },
  { host: "node_b", port: 8082, alive: true },
  { host: "node_c", port: 8083, alive: true },
];

let currentIndex = 0;
const HEALTH_CHECK_INTERVAL_MS = 3000;

/**
 * Active Health Check Engine
 * Actively probes the TCP ports to dynamically maintain the routing table.
 */
function checkBackendHealth(backend) {
  const socket = net.createConnection({
    host: backend.host,
    port: backend.port,
  });
  socket.setTimeout(1500); // Guard against hanging probes

  socket.on("connect", () => {
    if (!backend.alive) {
      console.log(
        `🟢 [Health Monitor] Backend ${backend.host}:${backend.port} is ONLINE.`,
      );
    }
    backend.alive = true;
    socket.destroy(); // Instantly release handle
  });

  const handleFailure = () => {
    if (backend.alive) {
      console.error(
        `🔴 [Health Monitor] Backend ${backend.host}:${backend.port} is OFFLINE.`,
      );
    }
    backend.alive = false;
    socket.destroy();
  };

  socket.on("error", handleFailure);
  socket.on("timeout", handleFailure);
}

function runClusterHealthChecks() {
  BACKENDS.forEach(checkBackendHealth);
}

// Initialize active polling
setInterval(runClusterHealthChecks, HEALTH_CHECK_INTERVAL_MS);
runClusterHealthChecks();

/**
 * Resilient Round-Robin Router
 * Skips dead nodes. Returns null if the whole cluster is down.
 */
function getNextHealthyBackend() {
  const healthyNodes = BACKENDS.filter((node) => node.alive);
  if (healthyNodes.length === 0) return null;

  const selectedNode = healthyNodes[currentIndex % healthyNodes.length];
  currentIndex = (currentIndex + 1) % healthyNodes.length;
  return selectedNode;
}

// Core Load Balancer Server
const server = net.createServer((clientSocket) => {
  const targetBackend = getNextHealthyBackend();

  // Edge Case: Total Blackout Recovery
  if (!targetBackend) {
    console.error(
      "🚨 Critical: Total cluster blackout. Dropping connection cleanly.",
    );
    clientSocket.destroy();
    return;
  }

  // Open the upstream socket to the selected healthy node
  const serverSocket = net.createConnection({
    host: targetBackend.host,
    port: targetBackend.port,
  });

  // Thread-safe flag to guarantee execution happens once
  let isClosed = false;

  const cleanupAndPurge = () => {
    if (isClosed) return;
    isClosed = true;

    // Explicitly destroy rather than gracefully ending.
    // This forces the OS kernel to instantly free file handles.
    clientSocket.destroy();
    serverSocket.destroy();
  };

  serverSocket.on("connect", () => {
    // Bi-directional data piping only AFTER connection succeeds
    clientSocket.pipe(serverSocket);
    serverSocket.pipe(clientSocket);
  });

  // Edge Case: Upstream Backend Crashes during active chat session
  serverSocket.on("error", (err) => {
    console.error(
      `❌ Upstream socket error from backend ${targetBackend.port}:`,
      err.message,
    );
    cleanupAndPurge();
  });

  // Edge Case: Client connection drops/aborts unexpectedly
  clientSocket.on("error", (err) => {
    console.error("❌ Client socket dropped out unexpectedly:", err.message);
    cleanupAndPurge();
  });

  // Ensure clean teardown when either side signals termination
  serverSocket.on("close", cleanupAndPurge);
  clientSocket.on("close", cleanupAndPurge);
});

// Server Process Level Error Handling (e.g., EADDRINUSE)
server.on("error", (err) => {
  console.error("🚨 Critical Load Balancer Process Error:", err);
});

const LB_PORT = 8080;
server.listen(LB_PORT, () => {
  console.log(`⚖️  [Layer 4 Load Balancer] Active on port ${LB_PORT}`);
  console.log(`📡 Polling monitoring initialized for backend cluster...`);
});
