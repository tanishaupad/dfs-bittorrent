"""A stale approval cannot restore membership after a removal, even out of order."""

import hashlib
import pathlib
import socket
import tempfile

from test_tracker_recovery import command, free_port, start_tracker, stop, wait_for


def sync_operation(port, secret, operation_id, operation, clock=None):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall((f"SYNC_HELLO {secret}\n").encode())
        assert read_line(sock) == "SYNC_OK"
        prefix = f"{clock[0]} {clock[1]} " if clock is not None else ""
        sock.sendall((f"SYNC_OP {operation_id} {prefix}{operation}\n").encode())
        assert read_line(sock) == f"SYNC_ACK {operation_id}"


def read_line(sock):
    data = bytearray()
    while not data.endswith(b"\n"):
        fragment = sock.recv(1)
        if not fragment:
            raise ConnectionError("tracker closed response")
        data.extend(fragment)
    return data[:-1].decode()


def main():
    port, missing = free_port(), free_port()
    password = hashlib.sha1(b"secret").hexdigest()
    with tempfile.TemporaryDirectory(prefix="dfs-membership-") as directory:
        work = pathlib.Path(directory)
        config = work / "trackers.txt"
        config.write_text(f"127.0.0.1 {port}\n127.0.0.1 {missing}\n")
        tracker = start_tracker(1, config, work)
        try:
            wait_for(lambda: command(port, "CREATE_USER owner " + password) == "SUCCESS")
            assert command(port, "CREATE_USER bob " + password) == "SUCCESS"
            owner = command(port, f"LOGIN owner {password} 127.0.0.1 9998").split()[1]
            bob = command(port, f"LOGIN bob {password} 127.0.0.1 9999").split()[1]
            assert command(port, "CREATE_GROUP group owner", owner) == "SUCCESS"
            secret = (work / "tracker_sync.key").read_text().strip()

            # The network delivers a removal before the approval and request.
            sync_operation(port, secret, "2:1", "LEAVE_GROUP group bob old-request")
            sync_operation(port, secret, "2:2", "ACCEPT_REQUEST group bob owner old-request")
            sync_operation(port, secret, "2:3", "JOIN_GROUP group bob old-request")
            assert command(port, "LIST_REQUESTS group owner", owner) == "SUCCESS"
            assert command(port, "LIST_FILES group bob", bob).startswith("ERROR")

            # A fresh request has a distinct ID and can be approved normally.
            assert command(port, "JOIN_GROUP group bob", bob) == "SUCCESS"
            assert command(port, "LIST_REQUESTS group owner", owner) == "SUCCESS bob"
            assert command(port, "ACCEPT_REQUEST group bob owner", owner) == "SUCCESS"
            assert command(port, "LIST_FILES group bob", bob) == "SUCCESS"
            assert command(port, "LEAVE_GROUP group bob", bob) == "SUCCESS"
            assert command(port, "LIST_FILES group bob", bob).startswith("ERROR")
            journal = (work / "tracker1_journal.txt").read_text().splitlines()
            fresh_join = next(line for line in reversed(journal)
                              if "\tJOIN_GROUP group bob " in line)
            fresh_id = fresh_join.rsplit(" ", 1)[1]
            sync_operation(port, secret, "2:4",
                           f"ACCEPT_REQUEST group bob owner {fresh_id}")
            assert command(port, "LIST_FILES group bob", bob).startswith("ERROR")

            # Two distinct requests race with a removal. Causal metadata makes
            # removal win regardless of network delivery order.
            initial_ids = {}
            for group in ("early", "late"):
                assert command(port, f"CREATE_GROUP {group} owner", owner) == "SUCCESS"
                assert command(port, f"JOIN_GROUP {group} bob", bob) == "SUCCESS"
                assert command(port, f"ACCEPT_REQUEST {group} bob owner", owner) == "SUCCESS"
                lines = (work / "tracker1_journal.txt").read_text().splitlines()
                initial_ids[group] = next(line for line in reversed(lines)
                                          if f"JOIN_GROUP {group} bob " in line).rsplit(" ", 1)[1]
            last = (work / "tracker1_journal.txt").read_text().splitlines()[-1].split("\t")
            base = (int(last[1]), int(last[2]))
            remove_clock = (base[0] + 1, base[1])
            join_clock = (base[0], base[1] + 1)
            approve_clock = (base[0], base[1] + 2)
            for group, removal_first in (("early", True), ("late", False)):
                old_id = initial_ids[group]
                new_id = f"new-{group}"
                removal = (f"LEAVE_GROUP {group} bob {old_id}", remove_clock)
                request = (f"JOIN_GROUP {group} bob {new_id}", join_clock)
                approval = (f"ACCEPT_REQUEST {group} bob owner {new_id}", approve_clock)
                events = (removal, request, approval) if removal_first else (request, approval, removal)
                for index, (operation, clock) in enumerate(events):
                    sync_operation(port, secret, f"2:{100 + (group == 'late') * 10 + index}",
                                   operation, clock)
                assert command(port, f"LIST_FILES {group} bob", bob).startswith("ERROR")

            # A genuinely later request/approval may restore access.
            sync_operation(port, secret, "2:200", "JOIN_GROUP early bob fresh-after-removal",
                           (base[0] + 2, base[1] + 2))
            sync_operation(port, secret, "2:201",
                           "ACCEPT_REQUEST early bob owner fresh-after-removal",
                           (base[0] + 3, base[1] + 2))
            assert command(port, "LIST_FILES early bob", bob) == "SUCCESS"
            stop(tracker)
            (work / "tracker1_state.txt").unlink()
            tracker = start_tracker(1, config, work)
            wait_for(lambda: command(port, "LIST_FILES group bob", bob).startswith("ERROR"))
            assert command(port, "LIST_REQUESTS group owner", owner) == "SUCCESS"
            assert command(port, "LIST_FILES early bob", bob) == "SUCCESS"
            assert command(port, "LIST_FILES late bob", bob).startswith("ERROR")
            print("PASS: concurrent removal wins in both orders; causal rejoin and replay worked")
        finally:
            stop(tracker)


if __name__ == "__main__":
    main()
