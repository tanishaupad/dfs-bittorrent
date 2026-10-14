"""Transfer and verify a 1 GiB file with bounded client memory.

The test uses temporary files and local loopback sockets. Set DFS_LARGE_TEST_MIB
to run a smaller smoke test; the default exercises the assignment's 1 GiB limit.
"""

import hashlib
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import time

from test_tracker_recovery import command, free_port, stop, wait_for


ROOT = pathlib.Path(__file__).resolve().parent
SIZE_MIB = int(os.environ.get("DFS_LARGE_TEST_MIB", "1024"))
TIMEOUT = 900


def file_hash(path):
    digest = hashlib.sha1()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def rss_kib(process):
    result = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)],
                            capture_output=True, text=True)
    try:
        return int(result.stdout.strip())
    except ValueError:
        return 0


def send(process, message):
    process.stdin.write(message + "\n")
    process.stdin.flush()


def wait_log(path, marker, processes, samples, timeout=TIMEOUT, after=0):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        for name, process in processes.items():
            samples[name] = max(samples.get(name, 0), rss_kib(process))
            if process.poll() is not None:
                raise AssertionError(f"{name} exited early: {path.read_text()[-1000:]}")
        if marker in path.read_text(errors="replace")[after:]:
            return time.monotonic() - start
        time.sleep(1)
    raise AssertionError(f"timed out waiting for {marker}: {path.read_text()[-1000:]}")


def main():
    tracker1_port, tracker2_port, source_port, receiver_port = [free_port() for _ in range(4)]
    password = hashlib.sha1(b"large-file-password").hexdigest()
    with tempfile.TemporaryDirectory(prefix="dfs-large-file-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {tracker1_port}\n127.0.0.1 {tracker2_port}\n")
        original = work / "large.bin"
        downloaded = work / "downloaded.bin"
        block = os.urandom(1024 * 1024)
        with original.open("wb") as file:
            for _ in range(SIZE_MIB):
                file.write(block)
        expected_hash = file_hash(original)
        processes = {}
        logs = {}
        samples = {}
        try:
            for number in (1, 2):
                logs[f"tracker{number}"] = (work / f"tracker{number}.log").open("w+")
                processes[f"tracker{number}"] = subprocess.Popen(
                    [str(ROOT / "tracker/tracker"), str(config), str(number)],
                    cwd=work, stdin=subprocess.PIPE, stdout=logs[f"tracker{number}"],
                    stderr=subprocess.STDOUT, text=True)

            def ready(port):
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True
            wait_for(lambda: ready(tracker1_port) and ready(tracker2_port))
            for user in ("source", "receiver"):
                assert command(tracker1_port, f"CREATE_USER {user} {password}") == "SUCCESS"
            owner = command(tracker1_port,
                            f"LOGIN source {password} 127.0.0.1 {source_port}").split()[1]
            recipient = command(tracker1_port,
                                f"LOGIN receiver {password} 127.0.0.1 {receiver_port}").split()[1]
            assert command(tracker1_port, "CREATE_GROUP big source", owner) == "SUCCESS"
            assert command(tracker1_port, "JOIN_GROUP big receiver", recipient) == "SUCCESS"
            assert command(tracker1_port, "ACCEPT_REQUEST big receiver source", owner) == "SUCCESS"
            wait_for(lambda: command(tracker2_port, "LIST_FILES big receiver", recipient) == "SUCCESS")

            for name, port in (("source", source_port), ("receiver", receiver_port)):
                logs[name] = (work / f"{name}.log").open("w+")
                processes[name] = subprocess.Popen(
                    [str(ROOT / "client/client"), f"127.0.0.1:{port}", str(config)],
                    cwd=work, stdin=subprocess.PIPE, stdout=logs[name],
                    stderr=subprocess.STDOUT, text=True)
                wait_log(work / f"{name}.log", "Client listening", processes, samples, 15)
                send(processes[name], f"login {name} large-file-password")
                wait_log(work / f"{name}.log", "SUCCESS", processes, samples, 15)

            source_log = work / "source.log"
            upload_offset = len(source_log.read_text(errors="replace"))
            send(processes["source"], f"upload file big {original}")
            upload_seconds = wait_log(source_log, "SUCCESS", processes, samples,
                                      after=upload_offset)
            send(processes["receiver"], f"download file big large.bin {downloaded}")
            transfer_seconds = wait_log(work / "receiver.log", "[C] [big] large.bin",
                                        processes, samples)
            assert downloaded.stat().st_size == SIZE_MIB * 1024 * 1024
            assert file_hash(downloaded) == expected_hash
            stats_offset = len(source_log.read_text(errors="replace"))
            send(processes["source"], "show stats")
            wait_log(source_log, "UPLOADED_BYTES", processes, samples, 15,
                     after=stats_offset)
            uploaded = [int(value) for value in re.findall(
                r"UPLOADED_BYTES (\d+)", source_log.read_text(errors="replace"))]
            assert uploaded and uploaded[-1] >= SIZE_MIB * 1024 * 1024
            assert samples["source"] < 256 * 1024
            assert samples["receiver"] < 256 * 1024
            print(f"PASS: {SIZE_MIB} MiB transfer and SHA-1 verified; "
                  f"upload {upload_seconds:.1f}s, download {transfer_seconds:.1f}s; "
                  f"source sent {uploaded[-1]} payload bytes; "
                  f"peak sampled RSS source {samples['source'] / 1024:.1f} MiB, "
                  f"receiver {samples['receiver'] / 1024:.1f} MiB")
        finally:
            for process in processes.values():
                stop(process)
            for log in logs.values():
                log.close()


if __name__ == "__main__":
    main()
