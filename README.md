# Peer-to-Peer Distributed File Sharing System (P2P DFS)

A robust, concurrent, peer-to-peer file sharing application written in C++17. The system allows users to form groups, register shared files, download files in parallel chunks from multiple seeders simultaneously with integrity checking, and runs with redundant and synchronized tracker servers.

---

## Technical Specifications
- **Compiler**: standard `g++` (C++17)
- **Dependencies**: POSIX Threads (`pthread`), Standard Socket library, and Standard Template Library (STL). Zero external library dependencies.
- **Piece Size**: Exactly 512KB (524288 bytes) per piece.
- **Hashing**: SHA-1 hash computed at both the piece level and complete file level.

---

## Directory Structure
```
├── client/
│   ├── client.cpp          # Client logic (CLI, downloader thread, C2C server)
│   ├── client.h            # Structures for downloading & shared states
│   └── Makefile            # Client compilation rules
├── tracker/
│   ├── tracker.cpp         # Tracker server (Client handler, T2T synchronization)
│   ├── tracker.h           # Database schema definition
│   └── Makefile            # Tracker compilation rules
├── common/
│   ├── sha1.cpp            # Self-contained SHA-1 implementation
│   ├── sha1.h              # Hashing declarations
│   └── common_utils.h      # TCP send/recv utilities and string parsers
├── tracker_info.txt        # Contains trackers list (IP Port)
└── README.md               # System Documentation
```

---

## Compilation Instructions

To compile the tracker and client applications, navigate to their respective directories and run `make`:

```bash
# Compile Tracker
cd tracker
make

# Compile Client
cd ../client
make
```

To clean up binary build artifacts and temporary files:
```bash
make clean
```

---

## Execution Instructions

### 1. Configure Trackers
Create a file named `tracker_info.txt` at the run location containing the IP and port of both trackers (one per line, separated by space or colon):
```text
127.0.0.1 8000
127.0.0.1 8001
```

### 2. Start Tracker Servers
Start both trackers using the following command format:
```bash
# To start Tracker 1
./tracker/tracker tracker_info.txt 1

# To start Tracker 2
./tracker/tracker tracker_info.txt 2
```
To shutdown a tracker server gracefully, enter `quit` in the tracker console.
Trackers started from the same run directory create a private `tracker_sync.key` file for authenticating journal exchange. For trackers on different machines, set the same `DFS_TRACKER_SECRET` environment variable on both before starting them. Keep that secret and the tracker state files private.

### 3. Start Client Applications
Start the clients using the following command format:
```bash
./client/client <CLIENT_IP>:<CLIENT_PORT> tracker_info.txt

# Example
./client/client 127.0.0.1:9000 tracker_info.txt
```

---

## Command Reference (Client Console)

Once a client starts, it presents an interactive prompt `>` accepting the following commands:

| Command | Description |
|---|---|
| `create user <user id> <password>` | Registers a new user account. |
| `login <user id> <password>` | Authenticates a user and starts a session. |
| `create group <group id>` | Creates a new group (user becomes the owner). |
| `join group <group id>` | Submits a request to join an existing group. |
| `leave group <group id>` | Leaves the specified group. |
| `list groups` | Displays all available groups. |
| `list requests <group id>` | Displays pending join requests (owner only). |
| `accept request <group id> <user id>` | Accepts a pending join request (owner only). |
| `upload file <group id> <file path>` | Shares a local file with the group. |
| `list files <group id>` | Displays all files available in a group. |
| `download file <group id> <file name> <dest path>` | Downloads a shared file in parallel from group seeders. |
| `show downloads` | Displays the progress of current/completed downloads. |
| `show stats` | Displays the client’s uploaded payload byte count. |
| `stop share <group id> <file name>` | Stops seeding a file in the group. |
| `logout` | Ends the current session and stops active seeding. |
| `quit` | Gracefully closes the client application. |

---

## Key Architectural Decisions

1. **Self-Contained Hashing**: Included a lightweight, dependency-free SHA-1 implementation in `common/` to guarantee smooth compiles on any standard system.
2. **Hybrid TCP Communication Protocol**: Used text-based headers for C2C/C2T metadata exchange and length-prefixed raw bytes for high-performance binary transfers.
3. **Rarest-First Piece Selector**: Queries online peer bitfields and schedules downloads starting with the rarest piece across available peers.
4. **Sharing During Downloads**: Once a piece passes its SHA-1 check and is written to disk, the downloader registers with the tracker. Other clients can discover it, query its current bitfield, and request verified pieces before the full file completes. The partial file is added to the persistent local sharing database only after the whole-file check succeeds.
5. **Resumable Downloads**: The receiver writes and syncs each verified piece, then atomically saves a bitmap and file identity in `<destination>.dfs.resume`. When `download file` is run again with the same destination after a client restart, it hashes existing pieces against tracker metadata and fetches only missing or damaged pieces. A missing checkpoint can be rebuilt from the destination bytes; a checkpoint for different file content is rejected. If peers disappear, the active download pauses and refreshes peer discovery until pieces become available again. The checkpoint is removed after the whole-file SHA-1 check succeeds.
6. **Tracker Operation Journals**: Each tracker retains accepted state changes with unique operation IDs and a two-counter vector clock. The trackers exchange journal entries repeatedly and apply unseen operations after a restart or temporary separation. State snapshots are replaced atomically, and a missing snapshot can be rebuilt from the journal.
7. **Single-Writer Names**: Tracker 1 alone accepts new usernames, group IDs, and group filenames. Tracker 2 can add seeders to an existing filename only when the size, full hash, and piece hashes match exactly. Different content needs a different filename. This avoids conflicting identity claims during a partition; creating new names pauses if tracker 1 is unavailable.
8. **Transparent Failover**: Client detects primary tracker disconnects, automatically redirects commands to the backup tracker, and transparently recovers the user session in the background for operations that the backup can accept.
9. **Access Checks**: Login returns a session token. Tracker commands carry the token and must name its owner. Before serving a piece or bitfield, a peer asks the tracker to validate the requester's token and group membership. Logout invalidates the token.

Peer availability is refreshed after a download round stalls or exhausts its current peers. The client waits and retries every two seconds while incomplete; exiting the client stops the attempt, and rerunning the same `download file` command resumes from verified bytes on disk.

## Local validation

After building both programs, run `python3 test_partial_discovery.py`, `python3 test_recovery.py`, `python3 test_resume.py`, `python3 test_tracker_recovery.py`, `python3 test_membership_conflict.py`, and `python3 test_namespace_conflict.py`. These tests use temporary tracker state and controlled peers. They cover piece sharing during downloads, recovery from corrupt and disconnected peers, concurrent files, client restart with and without a checkpoint, damaged-piece replacement, automatic continuation after a peer returns, tracker convergence after independent writes and restart, concurrent group approval/removal in both delivery orders, and filename/namespace conflicts.

Run `python3 test_large_file.py` for a 1 GiB end-to-end transfer over loopback TCP. The test starts two trackers and two clients, creates a binary source file in a temporary directory, transfers it in 512 KiB pieces, compares full-file SHA-1 hashes, checks that the source sent at least 1,073,741,824 payload bytes, and samples both clients' resident memory. The temporary files are removed afterward. Set `DFS_LARGE_TEST_MIB=16` for a quick harness smoke test. On the local validation run, the 1 GiB transfer completed and verified; sampled peak RSS was 8.3 MiB for each client. These measurements are from one host and do not establish performance on a real network or under interruptions during a 1 GiB transfer.

Run `python3 benchmark_distribution.py` to repeat the local distribution comparison. It starts two trackers and three clients in temporary directories, verifies both downloaded hashes, and writes measurements to `benchmarks/results.json`. The centralized mode disables sharing on the first downloader. The cooperative mode starts the second downloader after the first advertises a verified piece. The seeded mode starts it after the first finishes. All modes use the same C++ transfer code and a 25 ms process-wide delay per uploaded piece to model a limited uplink. The `show stats` client command prints uploaded payload bytes. `DFS_UPLOAD_DELAY_MS` and `DFS_DISABLE_SHARING=1` are benchmark controls; leave them unset for normal use.

Tracker convergence is eventual after a separation. Group join requests have unique IDs and approvals name the exact request. Membership events carry vector clocks; a removal wins over approvals or requests that happened before it or concurrently with it. A new request and approval that both causally follow the removal can restore access. Concurrent distinct join requests are retained and resolved consistently. New user, group, and file identities are assigned only by tracker 1, so a partition cannot create competing claims; no automatic promotion is implemented. This implementation assumes exactly two configured trackers, and revocation history is retained in snapshots and journals.
