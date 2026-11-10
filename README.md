# Causal Multicast Middleware

A distributed systems middleware in Java implementing reliable **Causal Order Message Multicast** over unreliable UDP unicast channels, featuring **Matrix Clocks**, **Dynamic Peer Discovery**, and an **Autonomous Buffer Stabilization (Garbage Collection)** mechanism.

---

## Overview

In distributed systems without a centralized coordinator or synchronized physical clocks, ensuring causal consistency across nodes is essential. This middleware guarantees that messages are delivered to application layers respecting the Lamport happens-before relationship ($\rightarrow$), preventing out-of-order state mutations caused by network latency or jitter.

The implementation relies on an $N \times N$ **Matrix Clock** protocol to simultaneously enforce causal delivery and determine message stabilization for safe buffer truncation.

---

## Core Algorithms & Theoretical Foundation

### 1. Causal Ordering (Matrix Clocks)

Each participating node $P_i$ maintains an $N \times N$ matrix clock $MC_i$, where:
* $MC_i[i][*]$ represents the vector clock of node $P_i$.
* $MC_i[j][k]$ represents node $P_i$'s knowledge of node $P_j$'s knowledge regarding events from node $P_k$.

#### Emission (`mcsend`):
1. Node $P_i$ attaches its current vector clock $V_i = MC_i[i][*]$ to message $m$.
2. Transmits $m$ to all known peers in the group via UDP.
3. Increments its local event counter: $MC_i[i][i] \leftarrow MC_i[i][i] + 1$.

#### Reception & Delivery:
When node $P_i$ receives message $m$ sent by $P_j$ with vector clock $V_m$:
1. $m$ is placed into a thread-safe validation buffer.
2. The delivery condition checks:
   * $V_m[j] = MC_i[i][j] + 1$ (next immediate message from $P_j$).
   * $\forall k \neq j, V_m[k] \leq MC_i[i][k]$ (all causal dependencies have already been delivered).
3. Once satisfied:
   * $m$ is dispatched to the application layer via callback (`deliver`).
   * Node $P_i$ updates its knowledge: $MC_i[j][*] \leftarrow V_m$ and $MC_i[i][j] \leftarrow MC_i[i][j] + 1$.

### 2. Message Stabilization & Buffer Pruning

To prevent unbounded memory growth in long-running distributed processes, buffered messages must be safely purged once acknowledged by the entire membership.

A message $m$ originating from $P_{sender}$ with timestamp $V_m$ is deemed **stable** at node $P_i$ when:
$$\min_{1 \leq x \leq N} (MC_i[x][sender]) \ge V_m[sender]$$

When this condition holds, every node in the group has witnessed and acknowledged message $m$, allowing $P_i$ to discard it from memory.

---

## Architecture & Components

```
+-------------------------------------------------------------+
|                      Application Node                       |
|                   (ICausalMulticast API)                    |
+-------------------------------------------------------------+
                              |
                     mcsend() | deliver()
                              v
+-------------------------------------------------------------+
|                      CausalMulticast                        |
|   +-----------------------------------------------------+   |
|   | Matrix Clock (NxN)    | Message Buffer (Thread-safe)|   |
|   | Dynamic Group State   | Stabilization Controller    |   |
|   +-----------------------------------------------------+   |
+-------------------------------------------------------------+
         |                                           ^
         v                                           |
+------------------+                       +------------------+
|    UDPSender     |                       |   UDPReceiver    |
| (Unicast Output) |                       | (Unicast Listen) |
+------------------+                       +------------------+
         |                                           ^
         +--------------------+----------------------+
                              | UDP
                              v
+-------------------------------------------------------------+
|                      DiscoveryService                       |
|   IP Multicast Announcement & Dynamic Peer Membership       |
+-------------------------------------------------------------+
```

* **`CausalMulticast`**: Central middleware engine. Coordinates logical matrix clocks, buffering, causal delivery constraints, and buffer stabilization.
* **`DiscoveryService`**: Daemon running on an IP multicast address. Enables dynamic join/leave operations by continuously broadcasting node presence and updating peer lists without manual topology configuration.
* **`DelayQueue` / `DelayedPacket`**: Built-in network fault-injection utility allowing operators to selectively delay outgoing datagrams to simulate packet reordering and verify causal recovery in real time.
* **`UDPReceiver` / `UDPSender`**: Non-blocking UDP transport layer handling packet serialization and raw network I/O.
* **`App`**: Interactive CLI node providing real-time visibility into the current matrix clock, buffered messages, delivered message history, and transmission delay controls.

---

## Tech Stack & Prerequisites

* **Language:** Java 21+
* **Build System:** Apache Maven
* **Dependencies:** Google Gson (JSON serialization for network datagrams)
* **Transport:** UDP (Unicast for message transfer, Multicast for discovery)

---

## Getting Started

### 1. Build

Clone the repository and compile via Maven:

```bash
git clone https://github.com/lucasxpaire/causal-multicast-middleware.git
cd causal-multicast-middleware
mvn clean package
```

### 2. Running a Distributed Cluster (Local Simulation)

Open 3 separate terminals to instantiate a 3-node cluster:

**Terminal 1 (Node A):**
```bash
java -cp target/trabalho2-1.0-SNAPSHOT.jar CausalMulticast.App
# When prompted:
# IP: 127.0.0.1
# Port: 5001
```

**Terminal 2 (Node B):**
```bash
java -cp target/trabalho2-1.0-SNAPSHOT.jar CausalMulticast.App
# IP: 127.0.0.1
# Port: 5002
```

**Terminal 3 (Node C):**
```bash
java -cp target/trabalho2-1.0-SNAPSHOT.jar CausalMulticast.App
# IP: 127.0.0.1
# Port: 5003
```

Nodes will automatically discover each other via `DiscoveryService`.

### 3. Testing Causal Ordering with Artificial Latency

1. On Node A, choose to send a message but select **Delay** for Node C.
2. Send a causally related reply from Node B to all nodes.
3. Observe that Node C buffers Node B's message and **refuses to deliver it** until Node A's delayed dependency is released.
4. Release the delayed packet on Node A; Node C delivers both messages strictly in causal order.

---

## Author

* **Lucas Pairé** — [GitHub](https://github.com/lucasxpaire) • [LinkedIn](https://www.linkedin.com/in/lucaspaire/)
