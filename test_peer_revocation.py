"""Peer serving stops if either tracker revokes access or is unavailable."""

import hashlib
import pathlib
import socket
import subprocess
import tempfile

from test_tracker_recovery import command, free_port, start_tracker, stop, wait_for


ROOT = pathlib.Path(__file__).resolve().parent


def peer_request(port, token, request="GET_BITFIELD group sample.bin"):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.settimeout(7)
        sock.sendall(f"AUTH observer {token} {request}\n".encode())
        data = bytearray()
        while not data.endswith(b"\n"):
            part = sock.recv(1)
            if not part:
                raise ConnectionError("peer closed without a response")
            data.extend(part)
        return data[:-1].decode()


def main():
    primary, backup, peer, missing1, missing2 = [free_port() for _ in range(5)]
    password = hashlib.sha1(b"secret").hexdigest()
    with tempfile.TemporaryDirectory(prefix="dfs-peer-revoke-") as directory:
        work = pathlib.Path(directory)
        healthy, broken1, broken2 = [work / name for name in
                                     ("trackers.txt", "broken1.txt", "broken2.txt")]
        healthy.write_text(f"127.0.0.1 {primary}\n127.0.0.1 {backup}\n")
        broken1.write_text(f"127.0.0.1 {primary}\n127.0.0.1 {missing2}\n")
        broken2.write_text(f"127.0.0.1 {missing1}\n127.0.0.1 {backup}\n")
        trackers = [start_tracker(1, healthy, work), start_tracker(2, healthy, work)]
        client = None
        try:
            def ready(port):
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True

            wait_for(lambda: ready(primary) and ready(backup))
            for user in ("seed", "observer"):
                assert command(primary, f"CREATE_USER {user} {password}") == "SUCCESS"
            seed_token = command(primary, f"LOGIN seed {password} 127.0.0.1 {peer}").split()[1]
            observer_token = command(primary, f"LOGIN observer {password} 127.0.0.1 9999").split()[1]
            assert command(primary, "CREATE_GROUP group seed", seed_token) == "SUCCESS"
            assert command(primary, "JOIN_GROUP group observer", observer_token) == "SUCCESS"
            assert command(primary, "ACCEPT_REQUEST group observer seed", seed_token) == "SUCCESS"
            payload = b"approved piece"
            path = work / "sample.bin"
            path.write_bytes(payload)
            digest = hashlib.sha1(payload).hexdigest()
            assert command(primary, f"UPLOAD_FILE group seed sample.bin {len(payload)} {digest} {digest}",
                           seed_token) == "SUCCESS"
            (work / f"client_{peer}_files.txt").write_text(f"group sample.bin {path}\n")
            client = subprocess.Popen([str(ROOT / "client/client"), f"127.0.0.1:{peer}", str(healthy)],
                                      cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.PIPE, text=True)
            client.stdin.write("login seed secret\n")
            client.stdin.flush()
            wait_for(lambda: peer_request(peer, observer_token) == "BITFIELD 1")

            for process in trackers:
                stop(process)
            trackers = [start_tracker(1, broken1, work), start_tracker(2, broken2, work)]
            wait_for(lambda: ready(primary) and ready(backup))
            assert command(primary, "LEAVE_GROUP group observer", observer_token) == "SUCCESS"
            assert command(primary, "LIST_FILES group observer", observer_token).startswith("ERROR")
            assert command(backup, "LIST_FILES group observer", observer_token) == "SUCCESS sample.bin"
            assert peer_request(peer, observer_token) == "ERROR peer access denied"
            assert peer_request(peer, observer_token, "GET_PIECE group sample.bin 0") == \
                "ERROR peer access denied"

            for process in trackers:
                stop(process)
            trackers = [start_tracker(1, healthy, work), start_tracker(2, healthy, work)]
            wait_for(lambda: command(backup, "LIST_FILES group observer", observer_token).startswith("ERROR"))
            assert peer_request(peer, observer_token) == "ERROR peer access denied"

            stop(trackers.pop())
            assert peer_request(peer, observer_token) == "ERROR peer access denied"
            print("PASS: stale approval and unreachable tracker cannot authorize peer serving")
        finally:
            if client is not None:
                stop(client)
            for process in trackers:
                stop(process)


if __name__ == "__main__":
    main()
