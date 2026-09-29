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
| `stop share <group id> <file name>` | Stops seeding a file in the group. |
| `logout` | Ends the current session and stops active seeding. |
| `quit` | Gracefully closes the client application. |

---

## Key Architectural Decisions

1. **Self-Contained Hashing**: Included a lightweight, dependency-free SHA-1 implementation in `common/` to guarantee smooth compiles on any standard system.
2. **Hybrid TCP Communication Protocol**: Used text-based headers for C2C/C2T metadata exchange and length-prefixed raw bytes for high-performance binary transfers.
3. **Rarest-First Piece Selector**: Queries online peer bitfields and schedules downloads starting with the rarest piece across seeders to maximize chunk availability.
4. **Bidirectional Startup Sync & Command Replay Logs**: Replicates database state on start via `SYNC_REQUEST` and maintains a transaction replay file (`tracker_pending_sync.txt`) for recovery when a peer is offline.
5. **Transparent Failover**: Client detects primary tracker disconnects, automatically redirects commands to the backup tracker, and transparently recovers the user session in the background.
