"""Verify that a client can serve a piece before its download completes.

Build with `make -C tracker && make -C client`, then run this script.
All tracker state and downloaded data stay in a temporary directory.
"""

import hashlib
import pathlib
import socket
import subprocess
import tempfile
import threading
import time


ROOT = pathlib.Path(__file__).resolve().parent
PIECE_SIZE = 524288
PIECES = [bytes([value]) * PIECE_SIZE for value in (65, 66, 67)]
DATA = b"".join(PIECES)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def read_line(sock):
    result = bytearray()
    while not result.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("socket closed before response")
        result.extend(chunk)
    return result[:-1].decode()


def command(port, message):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall((message + "\n").encode())
        return read_line(sock)


def wait_until(predicate, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("timed out waiting for expected state")


def fake_seed(listener, release, errors):
    try:
        with listener:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(25)
                assert read_line(connection) == "GET_BITFIELD group sample.bin"
                connection.sendall(b"BITFIELD 111\n")
                for index in range(len(PIECES)):
                    assert read_line(connection) == f"GET_PIECE group sample.bin {index}"
                    if index > 0 and not release.wait(20):
                        raise AssertionError("test did not release the source")
                    piece = PIECES[index]
                    connection.sendall(f"OK {len(piece)}\n".encode() + piece)
    except Exception as exc:
        errors.append(exc)
        release.set()


def main():
    tracker_port, backup_port, seed_port, client_port = [free_port() for _ in range(4)]
    release = threading.Event()
    seed_errors = []
    processes = []
    with tempfile.TemporaryDirectory(prefix="dfs-partial-") as directory:
        work = pathlib.Path(directory)
        config = work / "tracker_info.txt"
        config.write_text(f"127.0.0.1 {tracker_port}\n127.0.0.1 {backup_port}\n")
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", seed_port))
        listener.listen(1)
        seed_thread = threading.Thread(target=fake_seed, args=(listener, release, seed_errors))
        seed_thread.start()
        try:
            tracker = subprocess.Popen(
                [str(ROOT / "tracker/tracker"), str(config), "1"], cwd=work,
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )
            processes.append(tracker)

            def tracker_ready():
                try:
                    return command(tracker_port, "LIST_GROUPS").startswith("SUCCESS")
                except OSError:
                    return False

            wait_until(tracker_ready)
            for user, port in (("seed", seed_port), ("leecher", client_port)):
                password = hashlib.sha1(b"password").hexdigest()
                assert command(tracker_port, f"CREATE_USER {user} {password}") == "SUCCESS"
                assert command(tracker_port, f"LOGIN {user} {password} 127.0.0.1 {port}") == "SUCCESS"
            assert command(tracker_port, "CREATE_GROUP group seed") == "SUCCESS"
            assert command(tracker_port, "JOIN_GROUP group leecher") == "SUCCESS"
            assert command(tracker_port, "ACCEPT_REQUEST group leecher seed") == "SUCCESS"
            assert command(tracker_port, "REGISTER_SEEDER group leecher missing.bin").startswith("ERROR")
            hashes = " ".join(hashlib.sha1(piece).hexdigest() for piece in PIECES)
            file_hash = hashlib.sha1(DATA).hexdigest()
            assert command(tracker_port, f"UPLOAD_FILE group seed sample.bin {len(DATA)} {file_hash} {hashes}") == "SUCCESS"

            client = subprocess.Popen(
                [str(ROOT / "client/client"), f"127.0.0.1:{client_port}", str(config)],
                cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(client)
            destination = work / "received.bin"
            client.stdin.write(f"login leecher password\ndownload file group sample.bin {destination}\n")
            client.stdin.flush()

            def leecher_discovered():
                reply = command(tracker_port, "DOWNLOAD_FILE group leecher sample.bin")
                return f"127.0.0.1:{client_port}" in reply

            wait_until(leecher_discovered)
            with socket.create_connection(("127.0.0.1", client_port), timeout=3) as peer:
                peer.settimeout(3)
                peer.sendall(b"GET_BITFIELD group sample.bin\n")
                assert read_line(peer) == "BITFIELD 100"
                peer.sendall(b"GET_PIECE group sample.bin 0\n")
                assert read_line(peer) == f"OK {PIECE_SIZE}"
                received = bytearray()
                while len(received) < PIECE_SIZE:
                    received.extend(peer.recv(PIECE_SIZE - len(received)))
                assert bytes(received) == PIECES[0]

            release.set()
            wait_until(lambda: destination.exists() and
                       hashlib.sha1(destination.read_bytes()).hexdigest() == file_hash)
            print("PASS: incomplete downloader was discoverable and served a verified piece")
        finally:
            release.set()
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            seed_thread.join(timeout=3)
        if seed_errors:
            raise seed_errors[0]


if __name__ == "__main__":
    main()
