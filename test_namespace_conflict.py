"""Verify single-writer names and immutable content behind each filename."""

import hashlib
import pathlib
import socket
import tempfile

from test_tracker_recovery import command, free_port, start_tracker, stop, wait_for


def main():
    primary_port, backup_port = free_port(), free_port()
    password = hashlib.sha1(b"secret").hexdigest()
    with tempfile.TemporaryDirectory(prefix="dfs-namespace-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {primary_port}\n127.0.0.1 {backup_port}\n")
        processes = [start_tracker(1, config, work), start_tracker(2, config, work)]
        try:
            def ready(port):
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True
            wait_for(lambda: ready(primary_port) and ready(backup_port))
            assert command(primary_port, f"CREATE_USER owner {password}") == "SUCCESS"
            assert command(primary_port, f"CREATE_USER bob {password}") == "SUCCESS"
            owner = command(primary_port, f"LOGIN owner {password} 127.0.0.1 9998").split()[1]
            bob = command(primary_port, f"LOGIN bob {password} 127.0.0.1 9999").split()[1]
            assert command(primary_port, "CREATE_GROUP group owner", owner) == "SUCCESS"
            assert command(primary_port, "JOIN_GROUP group bob", bob) == "SUCCESS"
            assert command(primary_port, "ACCEPT_REQUEST group bob owner", owner) == "SUCCESS"
            wait_for(lambda: command(backup_port, "LIST_FILES group bob", bob) == "SUCCESS")

            assert command(backup_port, f"CREATE_USER stolen {password}").startswith("ERROR")
            assert command(backup_port, "CREATE_GROUP stolen bob", bob).startswith("ERROR")
            original = b"same immutable bytes"
            changed = b"different file bytes"
            original_hash = hashlib.sha1(original).hexdigest()
            changed_hash = hashlib.sha1(changed).hexdigest()
            upload = f"UPLOAD_FILE group {{}} file.bin {len(original)} {original_hash} {original_hash}"
            assert command(backup_port, upload.format("bob"), bob).startswith("ERROR")
            assert command(primary_port, upload.format("owner"), owner) == "SUCCESS"
            wait_for(lambda: "file.bin" in command(backup_port, "LIST_FILES group bob", bob))
            assert command(backup_port, upload.format("bob"), bob) == "SUCCESS"
            conflict = f"UPLOAD_FILE group bob file.bin {len(changed)} {changed_hash} {changed_hash}"
            assert command(backup_port, conflict, bob).startswith("ERROR")
            assert command(primary_port, conflict, bob).startswith("ERROR")
            metadata = command(backup_port, "DOWNLOAD_FILE group bob file.bin", bob)
            assert metadata.startswith(f"SUCCESS {len(original)} {original_hash} {original_hash} PEERS")
            print("PASS: namespace claims are primary-only; identical files merge, conflicts fail")
        finally:
            for process in processes:
                stop(process)


if __name__ == "__main__":
    main()
