# backup_to_dropbox.py
#
# Daily Postgres backup (Railway) -> Dropbox.
# Designed to work with your Railway env vars:
#   PGHOST=postgres.railway.internal
#   PGPORT=5432
#   POSTGRES_DB=railway
#   POSTGRES_USER=postgres
#   POSTGRES_PASSWORD=...
#
# And Dropbox env vars:
#   DROPBOX_TOKEN=...
#   DROPBOX_FOLDER=/payscope/PayScope/db_backups   (optional; default below)
#
# Requirements:
#   pip install requests
#   Container must have: pg_dump (install postgresql-client in Dockerfile)

from __future__ import annotations

import os
import shutil
import socket
import subprocess
from datetime import datetime, timezone

import requests


# ----------------------------
# Helpers
# ----------------------------
def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v or not v.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v.strip()


def _tcp_check(host: str, port: int, timeout: int = 5) -> None:
    """Fail fast if host/port is unreachable."""
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.close()


def _dropbox_upload(token: str, dropbox_path: str, local_path: str) -> None:
    """Upload a file to Dropbox using content upload endpoint."""
    with open(local_path, "rb") as f:
        r = requests.post(
            "https://content.dropboxapi.com/2/files/upload",
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": (
                    '{"path": "%s", "mode": "add", "autorename": true, "mute": false}'
                    % dropbox_path.replace('"', '\\"')
                ),
                "Content-Type": "application/octet-stream",
            },
            data=f,
            timeout=300,
        )

    if r.status_code >= 300:
        raise RuntimeError(f"Dropbox upload failed: {r.status_code} {r.text}")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    # Railway Postgres env vars (yours)
    pghost = _require_env("PGHOST")
    pgport = int(os.environ.get("PGPORT", "5432").strip())
    dbname = _require_env("POSTGRES_DB")
    dbuser = _require_env("POSTGRES_USER")
    dbpass = _require_env("POSTGRES_PASSWORD")

    # Dropbox env vars
    dropbox_token = _require_env("DROPBOX_TOKEN")
    dropbox_folder = os.environ.get("DROPBOX_FOLDER", "/payscope/PayScope/db_backups").strip()
    if not dropbox_folder.startswith("/"):
        dropbox_folder = "/" + dropbox_folder
    dropbox_folder = dropbox_folder.rstrip("/")

    # Preflight: pg_dump exists
    pg_dump_path = shutil.which("pg_dump")
    print("pg_dump path:", pg_dump_path)
    if not pg_dump_path:
        raise RuntimeError("pg_dump not found. Install postgresql-client in this container/service.")

    # Print pg_dump version for auditability
    try:
        ver = subprocess.check_output([pg_dump_path, "--version"]).decode().strip()
        print(ver)
    except Exception as e:
        print("Warning: could not read pg_dump version:", repr(e))

    # Preflight: can reach DB host/port
    print(f"Checking TCP connectivity to Postgres: {pghost}:{pgport} ...")
    _tcp_check(pghost, pgport, timeout=5)
    print("✅ Postgres host/port reachable")

    # Build filename + paths
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_UTC")
    filename = f"payscope_{dbname}_{ts}.dump"  # pg_dump custom format output
    local_path = f"/tmp/{filename}"
    dropbox_path = f"{dropbox_folder}/{filename}"

    # Run pg_dump (custom format is compressed + pg_restore friendly)
    env = {
        **os.environ,
        "PGHOST": pghost,
        "PGPORT": str(pgport),
        "PGDATABASE": dbname,
        "PGUSER": dbuser,
        "PGPASSWORD": dbpass,
    }

    cmd = [
        pg_dump_path,
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        "--file",
        local_path,
        dbname,
    ]

    print("Creating DB dump...")
    subprocess.run(cmd, env=env, check=True)
    print("✅ Dump created:", local_path)

    # Upload to Dropbox
    print("Uploading to Dropbox:", dropbox_path)
    _dropbox_upload(dropbox_token, dropbox_path, local_path)
    print("✅ Backup uploaded:", dropbox_path)


if __name__ == "__main__":
    main()
