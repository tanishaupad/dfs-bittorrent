"""A committed journal entry survives a stale snapshot; a failed append changes nothing."""

import hashlib
import pathlib
import tempfile

from test_tracker_recovery import command, free_port, start_tracker, stop, wait_for


def main():
    port, missing = free_port(), free_port()
    password = hashlib.sha1(b"secret").hexdigest()
    with tempfile.TemporaryDirectory(prefix="dfs-commit-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {port}\n127.0.0.1 {missing}\n")
        process = start_tracker(1, config, work)
        try:
            wait_for(lambda: command(port, f"CREATE_USER alice {password}") == "SUCCESS")
            token = command(port, f"LOGIN alice {password} 127.0.0.1 9999").split()[1]
            snapshot = work / "tracker1_state.txt"
            old_snapshot = snapshot.read_bytes()

            assert command(port, "CREATE_GROUP committed alice", token) == "SUCCESS"
            assert "committed" in command(port, "LIST_GROUPS", token)
            stop(process)

            # Simulate a crash after the durable journal append and before
            # the newer snapshot was installed.
            snapshot.write_bytes(old_snapshot)
            process = start_tracker(1, config, work)
            wait_for(lambda: "committed" in command(port, "LIST_GROUPS", token))

            # Make the journal path unwritable. An uncommitted operation must
            # neither enter memory nor appear after a restart.
            journal = work / "tracker1_journal.txt"
            saved_journal = work / "saved_journal.txt"
            journal.rename(saved_journal)
            journal.mkdir()
            try:
                assert command(port, "CREATE_GROUP rejected alice", token) == \
                    "ERROR tracker journal write failed"
                assert "rejected" not in command(port, "LIST_GROUPS", token)
            finally:
                journal.rmdir()
                saved_journal.rename(journal)
            stop(process)
            process = start_tracker(1, config, work)
            wait_for(lambda: "committed" in command(port, "LIST_GROUPS", token))
            assert "rejected" not in command(port, "LIST_GROUPS", token)
            print("PASS: journal commit, stale snapshot replay, and failed append rollback")
        finally:
            stop(process)


if __name__ == "__main__":
    main()
