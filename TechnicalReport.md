# Technical Report - P2P Distributed File Sharing System

This report outlines the implementation details, architectural choices, synchronization mechanisms, and performance strategies designed for the P2P DFS assignment.

---

## 1. Implementation Approach
The project is built entirely in C++17 using standard networking and system APIs to ensure clean compiles without high-level library restrictions.

- **State Persistence**: Trackers maintain their database state in-memory for speed and serialize it to disk (`tracker1_state.txt` and `tracker2_state.txt`) upon modification.
- **Multithreading**: Both the tracker and client applications employ a thread-per-connection concurrency model. Standard `std::thread`, `std::mutex`, and `std::condition_variable` primitives manage parallel workloads.
- **Header-Only SHA-1**: Written from scratch to prevent dependencies on OpenSSL headers on the host OS. The implementation parses blocks of 512KB to construct hashes at piece-level and file-level.

---

## 2. Tracker Synchronization Algorithm Design
A robust bidirectional synchronization protocol guarantees eventual consistency between Tracker 1 and Tracker 2:

- **Startup Sync**: When Tracker $T_i$ boots, it attempts a TCP connection to Tracker $T_{1-i}$. If successful, it triggers a `SYNC_REQUEST`. $T_{1-i}$ serializes its entire database into raw text and streams it over the socket. $T_i$ reads this payload, deserializes it, updates its memory, and truncates its local pending queue. If $T_{1-i}$ is offline, $T_i$ falls back to its local state file.
- **Incremental Sync**: When a tracker executes a state-altering command (e.g., user creation, group registration, file upload), it queues a command string (e.g., `CREATE_USER user1 pwd_hash`) in a thread-safe sync list. A background worker thread attempts to send these commands to the other tracker.
- **Offline Transaction Logs**: If the peer tracker goes offline during incremental sync, the worker logs commands to `tracker<no>_pending_sync.txt` and sleeps. Once the peer starts up, it recovers using Startup Sync, allowing the primary tracker to safely discard its pending queue.

---

## 3. Piece Selection & Parallel Download Strategy
To maximize transfer efficiency and piece distribution in the network, we implement a **Rarest-First piece selection algorithm**:

1. **Discovery**: The client retrieves the metadata (hashes, sizes) and a list of online peers from the tracker.
2. **Handshake**: The client opens TCP connections in parallel to all seeders and requests their bitfields using `GET_BITFIELD`.
3. **Frequency Mapping**: The coordinator thread computes the frequency of each piece index (i.e. how many seeders possess that chunk) and initializes a `PieceQueue` sorted in ascending order of frequency.
4. **Worker Allocation**: A pool of downloader threads is allocated (one thread per connected seeder). Since sockets are stream-oriented, dedicating one thread to a single peer connection eliminates socket read/write concurrency issues and locks.
5. **Rarest Download Loop**: Each worker thread queries the queue for the rarest piece available on its assigned peer. It downloads the piece, verifies its SHA-1 hash against the tracker metadata, writes it to disk at the exact offset using concurrent file streams, and marks the piece as complete.
6. **Error Handling**: If a chunk hash fails or a seeder connection drops, the piece is pushed back into the sorted queue for other workers to retrieve from alternate seeders.

After a verified piece is written, the downloader exposes its local bitfield and registers with the tracker. This allows other clients to discover and request that piece before the downloader finishes. The file path is persisted as a complete local share only after the whole-file SHA-1 check succeeds. Peer lists and bitfields are currently sampled at download start; live availability refresh remains future work.

---

## 4. Network Protocol Design
Communication uses standard TCP sockets with custom application-layer protocols:

### A. Client-Tracker Commands
Text-based, space-separated commands terminated by a newline (`\n`).
- Format: `COMMAND_NAME <arg1> <arg2> ...`
- Responses: `SUCCESS <payload>` or `ERROR <reason>`

### B. Client-Client (P2P) Chunk Transfer
Uses a text header to communicate file offsets and data size, followed immediately by binary payload transmission to handle file formatting natively (avoiding issues with null terminators in images/binaries):
- **Request Bitfield**: `GET_BITFIELD <group_id> <file_name>` -> Returns `BITFIELD <01_string>`
- **Request Piece**: `GET_PIECE <group_id> <file_name> <piece_index>` -> Returns `OK <bytes_len>\n<binary_payload>`

---

## 5. Challenges Encountered & Solutions

### A. Short-Lived Socket Disconnect Logouts
*Challenge*: The initial client implementation connected, sent a command, and closed the socket. The tracker's auto-logout triggered on socket closure, meaning users were logged out immediately after logging in.
*Solution*: Removed the automatic logout on socket closure from `tracker.cpp`. User login status is maintained on trackers until an explicit `logout` command is sent. Connection drops during download are handled gracefully by client-to-client worker threads.

### B. Block Buffering on Subprocess Pipes
*Challenge*: When running under automated Python tests, stdout logs from background downloader threads were block-buffered, preventing real-time verification of piece progress.
*Solution*: Replaced manual stream buffers with direct `std::endl` which forces an output stream flush, resolving logging latency.

---

## 6. References
1. **RFC 3174**: US Secure Hash Algorithm 1 (SHA1) standard.
2. **P2P Networks**: BitTorrent piece selection heuristics and bitfield representation principles.
3. **POSIX Socket Programming**: Standard Linux/macOS TCP socket options (`SO_REUSEADDR`, `SO_RCVTIMEO`, `SO_SNDTIMEO`).
