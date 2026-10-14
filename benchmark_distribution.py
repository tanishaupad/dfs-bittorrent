"""Compare distribution from one source with cooperative peer distribution.

Build the C++ binaries, then run this script. It uses loopback, two trackers,
three real clients, and a fixed per-piece upload delay to model limited uplinks.
Results are written to benchmarks/results.json.
"""

import hashlib
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from statistics import median


ROOT = pathlib.Path(__file__).resolve().parent
OUTPUT = ROOT / "benchmarks/results.json"
FILE_SIZE = 8 * 1024 * 1024
DELAY_MS = 25
REPEATS = 3


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.025)
    raise AssertionError("benchmark timed out")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


class ClientProcess:
    def __init__(self, port, config, directory, disable_sharing=False):
        env = os.environ.copy()
        env["DFS_UPLOAD_DELAY_MS"] = str(DELAY_MS)
        if disable_sharing:
            env["DFS_DISABLE_SHARING"] = "1"
        self.process = subprocess.Popen(
            [str(ROOT / "client/client"), f"127.0.0.1:{port}", str(config)],
            cwd=directory, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self.lines = []
        self.cv = threading.Condition()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.wait_text("Client listening")

    def _read(self):
        for line in self.process.stdout:
            with self.cv:
                self.lines.append(line.strip())
                self.cv.notify_all()

    def mark(self):
        with self.cv:
            return len(self.lines)

    def wait_text(self, text, after=0, timeout=20):
        deadline = time.monotonic() + timeout
        with self.cv:
            while True:
                for line in self.lines[after:]:
                    if text in line:
                        return line
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f"client output missing {text}: {self.lines[-8:]}")
                self.cv.wait(remaining)

    def send(self, command, expected="SUCCESS"):
        marker = self.mark()
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        result = self.wait_text(expected, marker)
        if "ERROR" in result:
            raise AssertionError(result)
        return result

    def uploaded_bytes(self):
        return int(self.send("show stats", "UPLOADED_BYTES").split("UPLOADED_BYTES ")[1])

    def close(self):
        stop(self.process)


def trial(mode, data, number):
    with tempfile.TemporaryDirectory(prefix=f"dfs-benchmark-{mode}-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        ports = [free_port() for _ in range(5)]
        config.write_text(f"127.0.0.1 {ports[0]}\n127.0.0.1 {ports[1]}\n")
        (work / "data.bin").write_bytes(data)
        trackers = []
        clients = []
        try:
            for number_of_tracker in (1, 2):
                trackers.append(subprocess.Popen(
                    [str(ROOT / "tracker/tracker"), str(config), str(number_of_tracker)],
                    cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                ))
            for port in ports[:2]:
                def ready():
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                            return True
                    except OSError:
                        return False
                wait_for(ready)

            alice = ClientProcess(ports[2], config, work)
            clients.append(alice)
            bob = ClientProcess(ports[3], config, work, disable_sharing=mode == "centralized")
            clients.append(bob)
            carol = ClientProcess(ports[4], config, work)
            clients.append(carol)
            for client, name in ((alice, "alice"), (bob, "bob"), (carol, "carol")):
                client.send(f"create user {name} password")
                client.send(f"login {name} password")
            alice.send("create group group")
            for client, name in ((bob, "bob"), (carol, "carol")):
                client.send("join group group")
                alice.send(f"accept request group {name}")
            alice.send("upload file group data.bin")

            b_path, c_path = work / "bob.bin", work / "carol.bin"
            start = time.perf_counter()
            marker_b = bob.mark()
            bob.send(f"download file group data.bin {b_path}", "SUCCESS download started")
            if mode == "cooperative":
                bob.wait_text("Sharing verified pieces", marker_b)
                time.sleep(0.12)
            elif mode == "seeded":
                bob.wait_text("[C] [group] data.bin", marker_b)
            carol.send(f"download file group data.bin {c_path}", "SUCCESS download started")
            bob.wait_text("[C] [group] data.bin", marker_b)
            carol.wait_text("[C] [group] data.bin")
            seconds = time.perf_counter() - start
            expected = hashlib.sha1(data).digest()
            assert hashlib.sha1(b_path.read_bytes()).digest() == expected
            assert hashlib.sha1(c_path.read_bytes()).digest() == expected
            result = {
                "mode": mode,
                "trial": number,
                "seconds": round(seconds, 4),
                "source_uploaded_bytes": alice.uploaded_bytes(),
                "peer_uploaded_bytes": bob.uploaded_bytes(),
            }
            if mode == "centralized" and result["peer_uploaded_bytes"] != 0:
                raise AssertionError("centralized run unexpectedly used peer uploads")
            if mode in ("cooperative", "seeded") and result["peer_uploaded_bytes"] == 0:
                raise AssertionError("peer-assisted run did not use peer uploads")
            return result
        finally:
            for client in clients:
                client.close()
            for tracker in trackers:
                stop(tracker)


def main():
    data = os.urandom(FILE_SIZE)
    runs = []
    modes = ("centralized", "cooperative", "seeded")
    for number in range(1, REPEATS + 1):
        for offset in range(len(modes)):
            mode = modes[(number - 1 + offset) % len(modes)]
            result = trial(mode, data, number)
            runs.append(result)
            print(f"{mode} trial {number}: {result['seconds']:.4f}s, "
                  f"peer upload {result['peer_uploaded_bytes']} bytes", flush=True)
    central = median(run["seconds"] for run in runs if run["mode"] == "centralized")
    cooperative = median(run["seconds"] for run in runs if run["mode"] == "cooperative")
    seeded = median(run["seconds"] for run in runs if run["mode"] == "seeded")
    summary = {
        "date_utc": datetime.now(timezone.utc).isoformat(),
        "file_size_bytes": FILE_SIZE,
        "upload_delay_ms_per_piece": DELAY_MS,
        "trials_per_mode": REPEATS,
        "environment": "single host, loopback TCP, two trackers, three C++ clients",
        "centralized_median_seconds": round(central, 4),
        "cooperative_median_seconds": round(cooperative, 4),
        "seeded_median_seconds": round(seeded, 4),
        "cooperative_relative_speedup": round(central / cooperative, 3),
        "seeded_relative_speedup": round(central / seeded, 3),
        "runs": runs,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Saved {OUTPUT}")


if __name__ == "__main__":
    main()
