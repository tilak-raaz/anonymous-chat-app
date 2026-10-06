// Parsing and validation for packets coming from the browser.
// Anything a client sends is untrusted: never let it throw inside a handler.
const { MAX_MESSAGE_LENGTH } = require("./config");

const ACTIONS = new Set(["find_match", "send"]);

/**
 * Returns { ok: true, packet } or { ok: false, error }.
 */
function parsePacket(raw) {
  let packet;
  try {
    packet = JSON.parse(raw.toString());
  } catch {
    return { ok: false, error: "Malformed packet: expected JSON." };
  }

  if (!packet || typeof packet !== "object" || !ACTIONS.has(packet.action)) {
    return { ok: false, error: "Unknown action." };
  }

  if (packet.action === "send") {
    if (typeof packet.text !== "string") {
      return { ok: false, error: "Message text must be a string." };
    }
    const text = packet.text.trim();
    if (!text) return { ok: false, error: "Message is empty." };
    if (text.length > MAX_MESSAGE_LENGTH) {
      return {
        ok: false,
        error: `Message is too long (max ${MAX_MESSAGE_LENGTH} characters).`,
      };
    }
    packet.text = text;
  }

  return { ok: true, packet };
}

/**
 * Fixed-window rate limiter, one per socket. Cheap and good enough to stop a
 * single client from flooding Redis and the database.
 */
function createRateLimiter(max, windowMs) {
  let windowStart = Date.now();
  let count = 0;
  return function allow() {
    const now = Date.now();
    if (now - windowStart >= windowMs) {
      windowStart = now;
      count = 0;
    }
    count += 1;
    return count <= max;
  };
}

module.exports = { parsePacket, createRateLimiter };
