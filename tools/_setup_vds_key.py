"""Установка SSH-ключа на VDS через paramiko."""

import os
from pathlib import Path

import paramiko
from dotenv import load_dotenv

from config.settings import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


def _exec(client, cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    return stdout.read(), stderr.read()


def main():
    host = os.getenv("VDS_HOST")
    user = os.getenv("VDS_USER")
    pwd = os.getenv("VDS_PASSWORD")

    if not all([host, user, pwd]):
        print("set VDS_HOST, VDS_USER, VDS_PASSWORD in .env")
        return

    # ищем публичный ключ
    for p in [Path.home() / ".ssh" / "id_ed25519.pub", Path("C:/Home/.ssh/id_ed25519.pub")]:
        if p.exists():
            pub_key = p.read_text().strip()
            break
    else:
        print("SSH key not found, run: ssh-keygen -t ed25519")
        return

    print(f"key: {pub_key[:50]}...")
    print(f"connecting to {user}@{host}...")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=15)
    print("connected!")

    # Windows OpenSSH для админов читает ключи из ProgramData, а не ~/.ssh
    # ставим ключ в оба места
    paths = [
        f"C:\\Users\\{user}\\.ssh",
        "C:\\ProgramData\\ssh",
    ]

    for ssh_dir in paths:
        _exec(client, f"if not exist {ssh_dir} mkdir {ssh_dir}")
        _exec(client, f"echo {pub_key} >> {ssh_dir}\\authorized_keys")

    # для ProgramData нужно название administrators_authorized_keys
    _exec(client, f"echo {pub_key} >> C:\\ProgramData\\ssh\\administrators_authorized_keys")

    print("key installed in both locations")

    # фиксим sshd_config, иначе он игнорирует ~/.ssh/authorized_keys для админов
    # чтобы sshd читал ~/.ssh/authorized_keys для всех
    sshd_cfg = "C:\\ProgramData\\ssh\\sshd_config"
    out, _ = _exec(client, f"type {sshd_cfg}")
    config = out.decode("utf-8", errors="replace")

    if "Match Group administrators" in config:
        # заменяем Match Group administrators блок на закомментированный
        new_config = config.replace(
            "Match Group administrators",
            "#Match Group administrators"
        ).replace(
            "\tAuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys",
            "#\tAuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys"
        ).replace(
            "       AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys",
            "#       AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys"
        )

        # записываем обратно через powershell (echo не умеет multiline на Windows)
        # экранируем для powershell
        ps_cmd = (
            f"powershell -Command \"Set-Content -Path '{sshd_cfg}' "
            f"-Value (Get-Content '{sshd_cfg}' | ForEach-Object {{"
            f"$_ -replace '^Match Group administrators','#Match Group administrators' "
            f"-replace 'AuthorizedKeysFile __PROGRAMDATA__','#AuthorizedKeysFile __PROGRAMDATA__'"
            f"}})\""
        )
        _exec(client, ps_cmd)
        print("sshd_config patched (commented out admin override)")

        # рестарт sshd
        _exec(client, "powershell -Command Restart-Service sshd")
        print("sshd restarted")

    client.close()
    print(f"done! test with: ssh {user}@{host} echo ok")


if __name__ == "__main__":
    main()
