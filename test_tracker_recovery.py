"""Test two trackers accepting independent writes during a partition and healing.

The partition uses separate tracker config files with unreachable peer ports.
Both trackers remain available to clients throughout the partition.
"""

import hashlib
import pathlib
import socket
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parent


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command(port, message):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall((message + "\n").encode())
        data = bytearray()
        while not data.endswith(b"\n"):
            fragment = sock.recv(1)
            if not fragment:
                raise ConnectionError("tracker closed response")
            data.extend(fragment)
        return data[:-1].decode()


def wait_for(check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (ConnectionError, OSError):
            pass
        time.sleep(0.1)
    raise AssertionError("trackers did not converge")


def start_tracker(number, config, work):
    process = subprocess.Popen([str(ROOT / "tracker/tracker"), str(config), str(number)],
                               cwd=work, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    return process


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=4)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def create_group(port, user, group):
    password = hashlib.sha1(b"secret").hexdigest()
    assert command(port, f"CREATE_USER {user} {password}") == "SUCCESS"
    assert command(port, f"LOGIN {user} {password} 127.0.0.1 9999") == "SUCCESS"
    assert command(port, f"CREATE_GROUP {group} {user}") == "SUCCESS"


def main():
    p1, p2, missing1, missing2 = [free_port() for _ in range(4)]
    with tempfile.TemporaryDirectory(prefix="dfs-trackers-") as directory:
        work = pathlib.Path(directory)
        broken1, broken2, healed = [work / name for name in
                                     ("partition1.txt", "partition2.txt", "healed.txt")]
        broken1.write_text(f"127.0.0.1 {p1}\n127.0.0.1 {missing2}\n")
        broken2.write_text(f"127.0.0.1 {missing1}\n127.0.0.1 {p2}\n")
        healed.write_text(f"127.0.0.1 {p1}\n127.0.0.1 {p2}\n")
        processes = []
        try:
            t1 = start_tracker(1, broken1, work)
            t2 = start_tracker(2, broken2, work)
            processes.extend((t1, t2))
            wait_for(lambda: command(p1, "LIST_GROUPS").startswith("SUCCESS") and
                     command(p2, "LIST_GROUPS").startswith("SUCCESS"))
            create_group(p1, "alice", "alpha")
            create_group(p2, "bob", "beta")
            assert "beta" not in command(p1, "LIST_GROUPS")
            assert "alpha" not in command(p2, "LIST_GROUPS")
            stop(t1)
            stop(t2)
            processes.clear()

            t1 = start_tracker(1, healed, work)
            t2 = start_tracker(2, healed, work)
            processes.extend((t1, t2))
            wait_for(lambda: all(group in command(port, "LIST_GROUPS")
                                 for port in (p1, p2) for group in ("alpha", "beta")))

            stop(t1)
            processes.remove(t1)
            (work / "tracker1_state.txt").unlink()
            create_group(p2, "charlie", "gamma")
            t1 = start_tracker(1, healed, work)
            processes.append(t1)
            wait_for(lambda: "gamma" in command(p1, "LIST_GROUPS"))
            assert command(p1, "LIST_GROUPS") == command(p2, "LIST_GROUPS") or \
                   set(command(p1, "LIST_GROUPS").split()) == set(command(p2, "LIST_GROUPS").split())
            print("PASS: partition edits converged and restarted tracker caught up")
        finally:
            for process in processes:
                stop(process)


if __name__ == "__main__":
    main()
