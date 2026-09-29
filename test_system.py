import subprocess
import time
import os
import hashlib
import shutil

# Paths
TRACKER_BIN = "./tracker/tracker"
CLIENT_BIN = "./client/client"
TRACKER_INFO = "tracker_info.txt"

processes = []

def cleanup():
    print("[Test] Cleaning up processes and temp files...")
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=1)
        except Exception:
            pass
    # Clean files
    for f in os.listdir("."):
        if f.startswith("tracker") and f.endswith(".txt") and f != "tracker_info.txt":
            os.remove(f)
        if f.startswith("client") and f.endswith(".txt"):
            os.remove(f)
    if os.path.exists("dummy_large.bin"):
        os.remove("dummy_large.bin")
    if os.path.exists("dummy_downloaded.bin"):
        os.remove("dummy_downloaded.bin")
    if os.path.exists("dummy_downloaded3.bin"):
        os.remove("dummy_downloaded3.bin")

def sha1_hash(filepath):
    h = hashlib.sha1()
    with open(filepath, 'rb') as file:
        while True:
            chunk = file.read(4096)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def main():
    cleanup()

    # Create a 1.2MB dummy file for transfer testing
    print("[Test] Creating 1.2MB dummy file...")
    with open("dummy_large.bin", "wb") as f:
        # Write 1.2 * 1024 * 1024 bytes (slightly more than 2 full chunks of 512KB)
        f.write(b"A" * (int(1.2 * 1024 * 1024)))
    orig_hash = sha1_hash("dummy_large.bin")
    print(f"[Test] Hash of dummy_large.bin: {orig_hash}")

    # 1. Start Tracker 1
    print("[Test] Starting Tracker 1...")
    t1 = subprocess.Popen([TRACKER_BIN, TRACKER_INFO, "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(t1)
    time.sleep(1)

    # 2. Start Tracker 2 (which will sync with Tracker 1)
    print("[Test] Starting Tracker 2...")
    t2 = subprocess.Popen([TRACKER_BIN, TRACKER_INFO, "2"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(t2)
    time.sleep(1)

    # 3. Start Client 1
    print("[Test] Starting Client 1...")
    c1 = subprocess.Popen([CLIENT_BIN, "127.0.0.1:9000", TRACKER_INFO], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(c1)
    time.sleep(1)

    # 4. Start Client 2
    print("[Test] Starting Client 2...")
    c2 = subprocess.Popen([CLIENT_BIN, "127.0.0.1:9001", TRACKER_INFO], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(c2)
    time.sleep(1)

    # Helper function to send command and read response
    def send_cmd(proc, cmd, expected_marker=None, wait=1.0):
        print(f"[Send -> Client] {cmd}")
        proc.stdin.write(cmd + "\n")
        proc.stdin.flush()
        time.sleep(wait)
        # Flush and read stdout
        output = ""
        # Set stdout to non-blocking
        os.set_blocking(proc.stdout.fileno(), False)
        try:
            output = proc.stdout.read() or ""
        except Exception:
            pass
        os.set_blocking(proc.stdout.fileno(), True)
        if output:
            print(f"[Recv <- Client] {output.strip()}")
        return output

    # Test create and login
    send_cmd(c1, "create user user1 pwd1", "SUCCESS")
    send_cmd(c1, "login user1 pwd1", "SUCCESS")
    send_cmd(c1, "create group group1", "SUCCESS")

    send_cmd(c2, "create user user2 pwd2", "SUCCESS")
    send_cmd(c2, "login user2 pwd2", "SUCCESS")
    send_cmd(c2, "join group group1", "SUCCESS")

    # Client 1 accepts Client 2 request
    send_cmd(c1, "list requests group1")
    send_cmd(c1, "accept request group1 user2", "SUCCESS")

    # Client 1 uploads file
    send_cmd(c1, "upload file group1 dummy_large.bin", "SUCCESS")

    # Client 2 lists files in group1
    send_cmd(c2, "list files group1")

    # Client 2 downloads file from Client 1
    print("[Test] Client 2 downloading file...")
    send_cmd(c2, "download file group1 dummy_large.bin dummy_downloaded.bin", "SUCCESS")

    # Wait for download completion
    time.sleep(5)

    # Check if download complete output is visible on Client 2
    # Set stdout to non-blocking to check output
    os.set_blocking(c2.stdout.fileno(), False)
    c2_out = ""
    try:
        c2_out = c2.stdout.read() or ""
    except Exception:
        pass
    os.set_blocking(c2.stdout.fileno(), True)
    if c2_out:
        print(f"[Recv <- Client 2] {c2_out.strip()}")

    # Verify file hash
    if os.path.exists("dummy_downloaded.bin"):
        dl_hash = sha1_hash("dummy_downloaded.bin")
        print(f"[Test] Downloaded file hash: {dl_hash}")
        if dl_hash == orig_hash:
            print("[Test] Success: Integrity check matched!")
        else:
            print("[Test] Error: Integrity mismatch!")
    else:
        print("[Test] Error: Downloaded file not created!")

    # Start Client 3
    print("[Test] Starting Client 3...")
    c3 = subprocess.Popen([CLIENT_BIN, "127.0.0.1:9002", TRACKER_INFO], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(c3)
    time.sleep(1)

    send_cmd(c3, "create user user3 pwd3", "SUCCESS")
    send_cmd(c3, "login user3 pwd3", "SUCCESS")
    send_cmd(c3, "join group group1", "SUCCESS")

    # Client 1 accepts Client 3 request
    send_cmd(c1, "accept request group1 user3", "SUCCESS")

    # Client 3 downloads file. Chunks should be pulled from both Client 1 and Client 2 in parallel.
    print("[Test] Client 3 downloading file...")
    send_cmd(c3, "download file group1 dummy_large.bin dummy_downloaded3.bin", "SUCCESS")
    time.sleep(5)

    os.set_blocking(c3.stdout.fileno(), False)
    c3_out = ""
    try:
        c3_out = c3.stdout.read() or ""
    except Exception:
        pass
    os.set_blocking(c3.stdout.fileno(), True)
    if c3_out:
        print(f"[Recv <- Client 3] {c3_out.strip()}")

    if os.path.exists("dummy_downloaded3.bin"):
        dl3_hash = sha1_hash("dummy_downloaded3.bin")
        print(f"[Test] Client 3 downloaded file hash: {dl3_hash}")
        if dl3_hash == orig_hash:
            print("[Test] Success: Client 3 Integrity matched!")
        else:
            print("[Test] Error: Client 3 Integrity mismatch!")
    else:
        print("[Test] Error: Client 3 downloaded file not created!")

    # Test Tracker Failover
    print("[Test] Killing Tracker 1...")
    t1.terminate()
    t1.wait()
    print("[Test] Tracker 1 stopped.")

    # Verify Client 2 transparently fails over to Tracker 2
    print("[Test] Verifying transparent failover on Client 2...")
    res = send_cmd(c2, "list files group1")
    if "SUCCESS" in res or "dummy_large.bin" in res:
        print("[Test] Success: Transparent failover working! Client retrieved files list from Tracker 2.")
    else:
        # Check if failover message was logged
        time.sleep(1)
        os.set_blocking(c2.stdout.fileno(), False)
        c2_out = ""
        try:
            c2_out = c2.stdout.read() or ""
        except Exception:
            pass
        os.set_blocking(c2.stdout.fileno(), True)
        if "Primary tracker offline" in c2_out:
            print("[Test] Success: Failover detected.")
        print(f"[Test] Failover check output: {c2_out.strip()}")

    # End
    cleanup()
    print("[Test] Finished testing.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        cleanup()
