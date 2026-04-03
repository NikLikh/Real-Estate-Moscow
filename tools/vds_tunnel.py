"""SSH SOCKS5 туннель к VDS."""

import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from config.settings import PROJECT_ROOT, load_scraper_config
from scraper.proxy import _port_open

load_dotenv(PROJECT_ROOT / ".env")

VDS_HOST = os.getenv("VDS_HOST", "")
VDS_USER = os.getenv("VDS_USER", "")
VDS_PASSWORD = os.getenv("VDS_PASSWORD", "")
SSH_KEY = Path.home() / ".ssh" / "id_ed25519"


def get_socks_port():
    cfg = load_scraper_config()
    return int(cfg.get("vds_socks_port", 9080))


def _wait_port(port, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _port_open("127.0.0.1", port, timeout=2):
            return True
        time.sleep(0.5)
    return False


def setup_key():
    if not VDS_HOST or not VDS_USER or not VDS_PASSWORD:
        print("set VDS_HOST, VDS_USER, VDS_PASSWORD in .env")
        sys.exit(1)

    # генерируем ключ если нет
    if not SSH_KEY.exists():
        SSH_KEY.parent.mkdir(parents=True, exist_ok=True)
        print(f"generating SSH key: {SSH_KEY}")
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-f", str(SSH_KEY), "-N", ""],
            check=True,
        )
    else:
        print(f"SSH key exists: {SSH_KEY}")

    pub_key = SSH_KEY.with_suffix(".pub").read_text().strip()
    print(f"public key: {pub_key[:50]}...")

    # копируем на VDS через ssh с паролем
    # Windows OpenSSH хранит authorized_keys в ProgramData для админов
    # для обычных юзеров ключи лежат в ~/.ssh/authorized_keys
    remote_cmd = (
        f"mkdir -p C:\\Users\\{VDS_USER}\\.ssh && "
        f'echo {pub_key} >> C:\\Users\\{VDS_USER}\\.ssh\\authorized_keys'
    )

    print(f"copying key to {VDS_USER}@{VDS_HOST}...")
    print("enter password when prompted")
    result = subprocess.run(
        [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            f"{VDS_USER}@{VDS_HOST}",
            remote_cmd,
        ],
    )
    if result.returncode == 0:
        print("key installed!")
        # проверяем
        test = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
             f"{VDS_USER}@{VDS_HOST}", "echo connected"],
            capture_output=True, text=True,
        )
        if "connected" in test.stdout:
            print("key auth works -- sshpass не нужен")
        else:
            print("key auth failed -- возможно нужно поправить sshd_config на VDS")
            print("  на VDS PowerShell: notepad C:\\ProgramData\\ssh\\sshd_config")
            print("  убрать строку: Match Group administrators")
            print("  и строку: AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys")
            print("  затем: Restart-Service sshd")
    else:
        print("failed to copy key")


def tunnel():
    if not VDS_HOST or not VDS_USER:
        print("set VDS_HOST and VDS_USER in .env")
        sys.exit(1)

    port = get_socks_port()
    print(f"VDS: {VDS_USER}@{VDS_HOST}, SOCKS port: {port}")

    print(f"checking SSH on {VDS_HOST}:22...", end=" ")
    if not _port_open(VDS_HOST, 22):
        print("FAIL -- VDS не отвечает")
        sys.exit(1)
    print("ok")

    ssh_cmd = [
        "ssh", "-D", str(port), "-N",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ExitOnForwardFailure=yes",
        f"{VDS_USER}@{VDS_HOST}",
    ]

    # если ключа нет и пароля тоже, дальше нечего делать
    if not SSH_KEY.exists() and not VDS_PASSWORD:
        print("no SSH key and no VDS_PASSWORD -- run: python -m tools.vds_tunnel setup")
        sys.exit(1)

    print(f"starting tunnel: socks5://127.0.0.1:{port}")
    if VDS_PASSWORD and not SSH_KEY.exists():
        print("enter password when prompted")

    proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if _wait_port(port):
        print(f"tunnel ready on :{port}")
        print("press Ctrl+C to stop")
    else:
        print("tunnel failed to start")
        proc.terminate()
        stderr = proc.stderr.read().decode(errors="replace")
        if stderr:
            print(f"ssh: {stderr[:300]}")
        sys.exit(1)

    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nstopping tunnel...")
        proc.terminate()
        proc.wait(timeout=5)
        print("done")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        setup_key()
    else:
        tunnel()


if __name__ == "__main__":
    main()
