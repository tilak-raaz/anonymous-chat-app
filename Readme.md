# Distributed Anonymous Real-Time Chat

A highly scalable, real-time anonymous chat application. Built with a Next.js edge-cached frontend and a distributed containerized backend featuring WebSockets, Nginx reverse proxy, load balancing, and Redis Pub/Sub for cross-node communication.

**[View Live Demo](https://anonymous-frontend-three.vercel.app)** | **[ View System Architecture (Eraser.io)](https://app.eraser.io/workspace/SbT5CcxGfQsZ7xDCLFg2?origin=share&elements=utvXblAsjHvhzcIphL0zAQ)**

---

# System Architecture

![System Architecture](./public/system.png)

This project is engineered for high availability and real-time performance. The architecture completely separates the client-side UI delivery from the persistent WebSocket connections:

1. **Frontend Delivery (Edge):** The Next.js frontend is deployed on Vercel, utilizing edge caching for instant load times (`304 Not Modified`).
2. **Reverse Proxy (Nginx):** All real-time traffic is securely routed through a custom cloud domain (`api.tilakraaz.cloud`). Nginx handles SSL/TLS termination (Let's Encrypt) and upgrades standard HTTPS requests into persistent WebSocket TCP streams (`101 Switching Protocols`), keeping sockets alive with custom read/write timeouts.
3. **Load Balancing & Cluster:** Traffic is piped into a Dockerized load balancer that distributes WebSocket connections across a cluster of backend nodes (`node_a`, `node_b`, `node_c`).
4. **State & Messaging (Redis & Postgres):** 
   - **Redis Pub/Sub:** Used for fast message brokering across the distributed nodes so users connected to different servers can seamlessly chat in real-time.
   - **Redis Streams:** Implemented as a highly reliable, append-only event log to manage asynchronous microservice tasks and event dispatching without message loss.
   - **Postgres:** Handles structured, persistent relational data.
5. **Microservices & Matchmaking:** Dedicated `worker` and `matchmaker` containers run in the background. The Matchmaker service queues waiting users using Redis Sorted Sets. To eliminate race conditions during high concurrent traffic, it relies on Redis's atomic operations to pull users out of the queue, guaranteeing that concurrent requests cannot pop the same users simultaneously.

---

## ✨ Key Features
- **Real-Time WebSockets:** Low-latency, bi-directional communication bypassing standard HTTP overhead.
- **High-Concurrency Matchmaking:** Utilizes Redis Sorted Sets (ZSET) for matchmaking queues, relying on strictly atomic extractions to prevent race conditions.
- **Proven Scalability (Stress Tested):** 
  - **AWS EC2 (`t3.small`):** Successfully stress-tested with **1,000 concurrent users**, achieving a **92% success rate**.
  - **Local Cluster:** Handled **2,000 concurrent users** flawlessly with a **100% success rate**.
- **Auto-Reconnection Logic:** The frontend aggressively polls and re-establishes dropped connections seamlessly.
- **Distributed Backend:** Containerized cluster architecture capable of scaling horizontally.
- **Secure Infrastructure:** Enforced HTTPS and WSS protocols with strictly managed Nginx proxy pass headers.

---

## 🛠 Technology Stack

**Frontend**
* Next.js
* React
* WebSockets (Native API)
* Vercel (Hosting & Edge Cache)

**Backend & Microservices**
* Node.js 
* WebSockets
* Docker & Docker Compose (Container Orchestration)

**Database & Caching**
* PostgreSQL (Relational Data)
* Redis (Pub/Sub & In-Memory Cache)

**Cloud & DevOps**
* AWS EC2 (Hosting Environment)
* Nginx (Reverse Proxy & Load Balancing)
* Certbot / Let's Encrypt (SSL/TLS Security)
* Custom Domain Routing

---

## 🚀 Local Development Setup

To run this heavily containerized stack locally, you need Docker installed on your machine.

**1. Clone the repository:**
```bash
git clone https://github.com/tilak-raaz/anonymous-chat-app.git
cd anonymous-chat-app
```

**2. Spin up the Backend Cluster:**
```bash
docker compose up --build
```
*(This will boot the load balancer, backend nodes, matchmaker, worker, Redis, and Postgres on your local machine).*

**3. Run the Frontend Locally:**
```bash
git clone https://github.com/tilak-raaz/anonymous-frontend
npm install
npm run dev
```

**4. Environment Variables:**
Ensure your local frontend `.env.local` points to your local Docker proxy:
```env
NEXT_PUBLIC_WS_URL=ws://localhost:8080/
```

---

## 👨‍💻 Author

**Tilak Kumar**
Specializing in Distributed Systems, Cloud Architecture, and High-Concurrency Backend Engineering.
* GitHub: [@tilak-raaz](https://github.com/tilak-raaz)