"""Local integration tests for corrupt peers, disconnects, and concurrent files.

Build the binaries first. Tests use temporary tracker state and random local ports.
"""

import hashlib
import pathlib
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


ROOT = pathlib.Path(__file__).resolve().parent
PIECE = 524288


def line(sock):
    data = bytearray()
    while not data.endswith(b"\n"):
        part = sock.recv(1)
        if not part:
            raise ConnectionError("peer closed connection")
        data.extend(part)
    return data[:-1].decode()


def command(port, request):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall((request + "\n").encode())
        return line(sock)


def wait_for(check, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = check()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("timed out waiting for download")


class PeerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, files, behavior="good", delay=0):
        self.files = files
        self.behavior = behavior
        self.delay = delay
        self.requests = []
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), PeerHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()


class PeerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(8)
        try:
            while True:
                fields = line(self.request).split()
                if len(fields) == 3 and fields[0] == "GET_BITFIELD":
                    pieces = self.server.files.get(fields[2])
                    bits = "1" * len(pieces)
                    if self.server.behavior in ("corrupt", "disconnect"):
                        bits = "1" + "0" * (len(pieces) - 1)
                    self.request.sendall(("BITFIELD " + bits + "\n").encode())
                elif len(fields) == 4 and fields[0] == "GET_PIECE":
                    name, index = fields[2], int(fields[3])
                    with self.server.lock:
                        self.server.requests.append((name, index))
                    if self.server.behavior == "disconnect" and index == 0:
                        return
                    piece = self.server.files[name][index]
                    if self.server.behavior == "corrupt" and index == 0:
                        piece = bytes([piece[0] ^ 1]) + piece[1:]
                    if self.server.delay:
                        time.sleep(self.server.delay)
                    self.request.sendall(f"OK {len(piece)}\n".encode() + piece)
                else:
                    return
        except (ConnectionError, OSError):
            return


def split_file(data):
    return [data[i:i + PIECE] for i in range(0, len(data), PIECE)]


def run_case(name, files, peers, concurrent=False):
    tracker_socket = socket.socket()
    tracker_socket.bind(("127.0.0.1", 0))
    tracker_port = tracker_socket.getsockname()[1]
    tracker_socket.close()
    backup_socket = socket.socket()
    backup_socket.bind(("127.0.0.1", 0))
    backup_port = backup_socket.getsockname()[1]
    backup_socket.close()
    with tempfile.TemporaryDirectory(prefix=f"dfs-{name}-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {tracker_port}\n127.0.0.1 {backup_port}\n")
        tracker = None
        client = None
        try:
            tracker = subprocess.Popen([str(ROOT / "tracker/tracker"), str(config), "1"],
                                       cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.PIPE)

            def ready():
                try:
                    return command(tracker_port, "LIST_GROUPS").startswith("SUCCESS")
                except OSError:
                    return False

            wait_for(ready)
            pwd = hashlib.sha1(b"secret").hexdigest()
            for username, peer in peers:
                assert command(tracker_port, f"CREATE_USER {username} {pwd}") == "SUCCESS"
                assert command(tracker_port, f"LOGIN {username} {pwd} 127.0.0.1 {peer.server_address[1]}") == "SUCCESS"
            assert command(tracker_port, "CREATE_GROUP group a_good") == "SUCCESS"
            for username, _ in peers:
                if username != "a_good":
                    assert command(tracker_port, f"JOIN_GROUP group {username}") == "SUCCESS"
                    assert command(tracker_port, f"ACCEPT_REQUEST group {username} a_good") == "SUCCESS"
            for filename, data in files.items():
                pieces = split_file(data)
                hashes = " ".join(hashlib.sha1(p).hexdigest() for p in pieces)
                full = hashlib.sha1(data).hexdigest()
                for username, _ in peers:
                    assert command(tracker_port, f"UPLOAD_FILE group {username} {filename} {len(data)} {full} {hashes}") == "SUCCESS"

            client_port = socket.socket()
            client_port.bind(("127.0.0.1", 0))
            port = client_port.getsockname()[1]
            client_port.close()
            assert command(tracker_port, f"CREATE_USER leecher {pwd}") == "SUCCESS"
            client = subprocess.Popen([str(ROOT / "client/client"), f"127.0.0.1:{port}", str(config)],
                                      cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.PIPE, text=True)
            client.stdin.write("login leecher secret\njoin group group\n")
            client.stdin.flush()
            wait_for(lambda: "leecher" in command(tracker_port, "LIST_REQUESTS group a_good"))
            assert command(tracker_port, "ACCEPT_REQUEST group leecher a_good") == "SUCCESS"
            for filename in files:
                client.stdin.write(f"download file group {filename} {work / ('received-' + filename)}\n")
            client.stdin.flush()
            if concurrent:
                wait_for(lambda: all((work / ('received-' + filename)).exists() for filename in files), 3)
            for filename, data in files.items():
                target = work / ('received-' + filename)
                wait_for(lambda: target.exists() and
                         hashlib.sha1(target.read_bytes()).digest() == hashlib.sha1(data).digest(), 20)
            print(f"PASS: {name}")
        finally:
            for process in (client, tracker):
                if process is not None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def main():
    data = b"A" * PIECE + b"B" * PIECE
    pieces = {"sample.bin": split_file(data)}
    good = PeerServer(pieces, delay=0.1)
    corrupt = PeerServer(pieces, "corrupt")
    try:
        run_case("corrupt-piece-recovery", {"sample.bin": data},
                 [("a_good", good), ("b_bad", corrupt)])
        assert ("sample.bin", 0) in corrupt.requests
        assert ("sample.bin", 0) in good.requests
    finally:
        good.close()
        corrupt.close()

    good = PeerServer(pieces, delay=0.1)
    dropping = PeerServer(pieces, "disconnect")
    try:
        run_case("peer-disconnect-recovery", {"sample.bin": data},
                 [("a_good", good), ("b_drop", dropping)])
        assert ("sample.bin", 0) in dropping.requests
        assert ("sample.bin", 0) in good.requests
    finally:
        good.close()
        dropping.close()

    files = {f"file{i}.bin": bytes([65 + i]) * (PIECE * 6) for i in range(2)}
    good = PeerServer({name: split_file(data) for name, data in files.items()}, delay=0.1)
    try:
        run_case("concurrent-downloads", files, [("a_good", good)], concurrent=True)
    finally:
        good.close()


if __name__ == "__main__":
    main()
