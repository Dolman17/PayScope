# run.py
from __future__ import annotations
import os
from app import create_app
from app.cli import purge_records, import_data, geocode_missing

app = create_app()

# Register CLI commands
app.cli.add_command(purge_records)
app.cli.add_command(import_data)
app.cli.add_command(geocode_missing)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
