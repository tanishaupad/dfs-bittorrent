"""Exercise checkpoint resume, lost-checkpoint recovery, and peer reappearance."""

import hashlib
import pathlib
import socket
import subprocess
import tempfile
import time

from test_recovery import PIECE, PeerServer, command, wait_for
from test_tracker_recovery import free_port, stop


ROOT = pathlib.Path(__file__).resolve().parent


def checkpoint_bits(path):
    if not path.exists():
        return ""
    lines = path.read_text().splitlines()
    return lines[2] if len(lines) >= 3 else ""


def start_client(config, work, port, number):
    log = (work / f"client-{number}.log").open("w+")
    process = subprocess.Popen([str(ROOT / "client/client"), f"127.0.0.1:{port}", str(config)],
                               cwd=work, stdin=subprocess.PIPE, stdout=log,
                               stderr=subprocess.STDOUT, text=True)

    def ready():
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            return False
    wait_for(ready)
    send(process, "login leecher secret")
    wait_for(lambda: "SUCCESS" in (work / f"client-{number}.log").read_text())
    return process, log


def send(process, message):
    process.stdin.write(message + "\n")
    process.stdin.flush()


def run_interrupted_case(config, work, port, peer, expected, case, lose_checkpoint):
    destination = work / f"{case}.bin"
    checkpoint = pathlib.Path(str(destination) + ".dfs.resume")
    client, log = start_client(config, work, port, f"{case}-first")
    try:
        send(client, f"download file group source.bin {destination}")
        bits = wait_for(lambda: (value if 2 <= value.count("1") < len(value) else None)
                        if (value := checkpoint_bits(checkpoint)) else None, timeout=15)
    finally:
        stop(client)
        log.close()
    verified = {i for i, bit in enumerate(bits) if bit == "1"}
    assert verified
    if lose_checkpoint:
        damaged = min(verified)
        with destination.open("r+b") as file:
            file.seek(damaged * PIECE)
            file.write(b"corrupted bytes")
            file.flush()
        checkpoint.unlink()
    else:
        damaged = None

    with peer.lock:
        request_count = len(peer.requests)
    client, log = start_client(config, work, port, f"{case}-second")
    try:
        send(client, f"download file group source.bin {destination}")
        wait_for(lambda: f"[C] [group] source.bin" in
                 (work / f"client-{case}-second.log").read_text(), timeout=30)
        assert hashlib.sha1(destination.read_bytes()).digest() == hashlib.sha1(expected).digest()
        assert not checkpoint.exists()
        with peer.lock:
            resumed_requests = {index for name, index in peer.requests[request_count:]
                                if name == "source.bin"}
        assert not (verified - ({damaged} if damaged is not None else set())) & resumed_requests
        if damaged is not None:
            assert damaged in resumed_requests
        print(f"PASS: {case}: {len(verified)} verified pieces recovered")
    finally:
        stop(client)
        log.close()


def main():
    pieces = [bytes([index + 1]) * PIECE for index in range(12)]
    expected = b"".join(pieces)
    peer = PeerServer({"source.bin": pieces}, delay=0.25)
    tracker_port, backup_port, client_port = free_port(), free_port(), free_port()
    with tempfile.TemporaryDirectory(prefix="dfs-resume-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {tracker_port}\n127.0.0.1 {backup_port}\n")
        tracker = subprocess.Popen([str(ROOT / "tracker/tracker"), str(config), "1"],
                                   cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE)
        try:
            def tracker_ready():
                try:
                    with socket.create_connection(("127.0.0.1", tracker_port), timeout=1):
                        return True
                except OSError:
                    return False
            wait_for(tracker_ready)
            pwd = hashlib.sha1(b"secret").hexdigest()
            assert command(tracker_port, f"CREATE_USER seed {pwd}") == "SUCCESS"
            assert command(tracker_port, f"CREATE_USER leecher {pwd}") == "SUCCESS"
            seed_token = command(tracker_port, f"LOGIN seed {pwd} 127.0.0.1 {peer.server_address[1]}").split()[1]
            leecher_token = command(tracker_port, f"LOGIN leecher {pwd} 127.0.0.1 {client_port}").split()[1]
            assert command(tracker_port, "CREATE_GROUP group seed", seed_token) == "SUCCESS"
            assert command(tracker_port, "JOIN_GROUP group leecher", leecher_token) == "SUCCESS"
            assert command(tracker_port, "ACCEPT_REQUEST group leecher seed", seed_token) == "SUCCESS"
            hashes = " ".join(hashlib.sha1(piece).hexdigest() for piece in pieces)
            full = hashlib.sha1(expected).hexdigest()
            assert command(tracker_port, f"UPLOAD_FILE group seed source.bin {len(expected)} {full} {hashes}",
                           seed_token) == "SUCCESS"

            run_interrupted_case(config, work, client_port, peer, expected, "checkpoint", False)
            run_interrupted_case(config, work, client_port, peer, expected, "lost-checkpoint", True)

            different = work / "different.bin"
            different.write_bytes(b"keep this unrelated file")
            pathlib.Path(str(different) + ".dfs.resume").write_text(
                "DFS_RESUME_V1\n" + "0" * 40 + "\n" + "0" * len(pieces) + "\n")
            client, log = start_client(config, work, client_port, "different")
            try:
                send(client, f"download file group source.bin {different}")
                wait_for(lambda: "checkpoint belongs to different file content" in
                         (work / "client-different.log").read_text())
                assert different.read_bytes() == b"keep this unrelated file"
                print("PASS: different file identity was not overwritten")
            finally:
                stop(client)
                log.close()

            peer.behavior = "good"
            client, log = start_client(config, work, client_port, "paused")
            try:
                destination = work / "paused.bin"
                checkpoint = pathlib.Path(str(destination) + ".dfs.resume")
                send(client, f"download file group source.bin {destination}")
                wait_for(lambda: (value if 2 <= value.count("1") < len(value) else None)
                         if (value := checkpoint_bits(checkpoint)) else None, timeout=15)
                peer.behavior = "offline"
                time.sleep(3)
                assert f"[C] [group] source.bin" not in (work / "client-paused.log").read_text()
                send(client, "show downloads")
                wait_for(lambda: "Paused" in (work / "client-paused.log").read_text())
                peer.behavior = "good"
                wait_for(lambda: f"[C] [group] source.bin" in
                         (work / "client-paused.log").read_text(), timeout=30)
                assert hashlib.sha1(destination.read_bytes()).digest() == hashlib.sha1(expected).digest()
                print("PASS: partially completed download paused and resumed when peer returned")
            finally:
                stop(client)
                log.close()
        finally:
            stop(tracker)
            peer.close()


if __name__ == "__main__":
    main()
