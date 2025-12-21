import os
import subprocess
from datetime import datetime, timezone
import requests

# ----------------------------
# Railway Postgres env vars (yours)
# ----------------------------
PGHOST = os.environ["PGHOST"]
PGPORT = os.environ.get("PGPORT", "5432")
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# ----------------------------
# Dropbox env vars
# ----------------------------
DROPBOX_TOKEN = os.environ["DROPBOX_TOKEN"]
DROPBOX_FOLDER = os.environ.get("DROPBOX_FOLDER", "/payscope/PayScope/db_backups")

# ----------------------------
# Build filename
# ----------------------------
ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_UTC")
filename = f"payscope_{POSTGRES_DB}_{ts}.dump"   # custom pg_dump format
local_path = f"/tmp/{filename}"

# ----------------------------
# Run pg_dump (custom format = pg_restore-friendly)
# ----------------------------
env = {
    **os.environ,
    "PGHOST": PGHOST,
    "PGPORT": PGPORT,
    "PGDATABASE": POSTGRES_DB,     # what pg_dump expects
    "PGUSER": POSTGRES_USER,
    "PGPASSWORD": POSTGRES_PASSWORD,
}

cmd = [
    "pg_dump",
    "--format=custom",     # compressed + supports pg_restore
    "--no-owner",
    "--no-privileges",
    "--file", local_path,
]

print("Creating DB dump...")
subprocess.run(cmd, env=env, check=True)
print("Dump created:", local_path)

# ----------------------------
# Upload to Dropbox
# ----------------------------
dropbox_path = f"{DROPBOX_FOLDER}/{filename}"

print("Uploading to Dropbox:", dropbox_path)
with open(local_path, "rb") as f:
    r = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers={
            "Authorization": f"Bearer {DROPBOX_TOKEN}",
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

print("✅ Backup uploaded:", dropbox_path)
