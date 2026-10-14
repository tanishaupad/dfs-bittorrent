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

Each tracker persists accepted changes in `tracker<no>_journal.txt` with a unique `<tracker>:<sequence>` identifier and a two-counter vector clock. A background worker requests the other tracker's journal using `SYNC_PULL`, applies unseen entries, and sends its own entries using `SYNC_OP`. The receiver answers `SYNC_ACK <id>` only when an entry is already known or has been applied and appended locally; failed applications receive `SYNC_NACK <id>`. This avoids treating failed replay as success and avoids overwriting local state on startup.

Trackers also save atomic state snapshots. On restart, they load their snapshot and journal, or replay the journal if the snapshot is absent. When both trackers accept independent changes during a separation, they continue serving their local view and exchange the missing operations after reconnection. Their answers may differ during the separation. For membership, request IDs bind approvals to one join request, and the vector clocks make a removal win over concurrent or earlier approvals. A later join and approval may restore access.

New usernames, group IDs, and filenames are assigned only by tracker 1, preventing both trackers from accepting competing claims during a partition. Tracker 2 may add a seeder to an existing file only when its size, full SHA-1, and piece SHA-1 list match the registered metadata; a different file must use a different name. If tracker 1 is unavailable, new identity creation waits for its return. The system has no automatic leader promotion or quorum, and previously divergent data would require manual repair.

---

## 3. Piece Selection & Parallel Download Strategy
To maximize transfer efficiency and piece distribution in the network, we implement a **Rarest-First piece selection algorithm**:

1. **Discovery**: The client retrieves the metadata (hashes, sizes) and a list of online peers from the tracker.
2. **Handshake**: The client opens TCP connections in parallel to all seeders and requests their bitfields using `GET_BITFIELD`.
3. **Frequency Mapping**: The coordinator thread computes the frequency of each piece index (i.e. how many seeders possess that chunk) and initializes a `PieceQueue` sorted in ascending order of frequency.
4. **Worker Allocation**: A pool of downloader threads is allocated (one thread per connected seeder). Since sockets are stream-oriented, dedicating one thread to a single peer connection eliminates socket read/write concurrency issues and locks.
5. **Rarest Download Loop**: Each worker thread queries the queue for the rarest piece available on its assigned peer. It downloads the piece, verifies its SHA-1 hash against the tracker metadata, writes it at the exact offset, syncs the bytes, and then updates an atomic per-destination checkpoint containing a verified-piece bitmap.
6. **Error Handling**: If a chunk hash fails, the scheduler excludes that peer for the piece and returns the piece to the queue. If a peer disconnects or the current bitfields cannot supply every missing piece, the coordinator refreshes tracker peers and bitfields every two seconds. The download remains paused until a source returns or the client exits.

After a verified piece is written, the downloader exposes its local bitfield and registers with the tracker. This allows other clients to discover and request that piece before the downloader finishes. The file path is persisted as a complete local share only after the whole-file SHA-1 check succeeds. On client restart, issuing `download file` with the same destination hashes all existing pieces against the current tracker metadata. This recovers valid bytes even if the latest checkpoint is missing, and replaces damaged pieces. A checkpoint bound to a different file identity is rejected rather than silently reused.

---

## 4. Network Protocol Design
Communication uses standard TCP sockets with custom application-layer protocols:

### A. Client-Tracker Commands
Text-based, space-separated commands terminated by a newline (`\n`).
- Registration and login are public. Login returns `SUCCESS <session_token>`.
- Other requests use `AUTH <session_token> COMMAND_NAME <arg1> <arg2> ...`. The tracker checks that the command's username matches the token owner.
- Responses: `SUCCESS <payload>` or `ERROR <reason>`

### B. Client-Client (P2P) Chunk Transfer
Uses a text header to communicate file offsets and data size, followed immediately by binary payload transmission to handle file formatting natively (avoiding issues with null terminators in images/binaries):
- **Request Bitfield**: `AUTH <user> <session_token> GET_BITFIELD <group_id> <file_name>` -> Returns `BITFIELD <01_string>` after tracker validation.
- **Request Piece**: `AUTH <user> <session_token> GET_PIECE <group_id> <file_name> <piece_index>` -> Returns `OK <bytes_len>\n<binary_payload>` after tracker validation.

Trackers authenticate journal connections with a shared secret. The same run directory uses a locally generated `tracker_sync.key`; deployments on separate machines can supply `DFS_TRACKER_SECRET`. Client and peer traffic is still plaintext TCP, so token confidentiality requires a trusted network or a future encrypted transport.

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

---

## 7. Distribution Benchmark

`benchmark_distribution.py` compared three modes using the same C++ clients, two trackers, an 8 MiB file, and three repetitions per mode on one host over loopback TCP. A process-wide 25 ms delay per 512 KiB upload models a constrained uplink. The timer starts when the first downloader is instructed to begin and stops when both clients report completion; both output SHA-1 hashes are checked. Trial order rotates among modes. The full observations are in `benchmarks/results.json`.

| Mode | Second downloader starts | Median completion | First downloader upload |
| --- | --- | ---: | ---: |
| Centralized | Immediately; only original source serves | 0.9376 s | 0 MiB |
| Cooperative | After first peer advertises a verified piece | 0.8113 s | 2.5 MiB |
| Seeded | After first peer finishes and can seed the whole file | 0.8221 s | 4 MiB |

The cooperative mode was 1.156 times as fast as the centralized mode; the seeded mode was 1.140 times as fast. The downloader now refreshes peers and bitfields after a round stalls, allowing partial sharing to contribute more upload bytes than in the earlier run. These are local measurements with an artificial delay and a small sample, so they do not predict performance on a real network.
