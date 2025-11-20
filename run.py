# run.py
from __future__ import annotations
import os

from app import create_app

# Import CLI commands (safe even if Railway doesn't use them)
try:
    from app.cli import purge_records, import_data, geocode_missing
except Exception:
    purge_records = None
    import_data = None
    geocode_missing = None

app = create_app()

# Register CLI commands only if available
if purge_records:
    app.cli.add_command(purge_records)
if import_data:
    app.cli.add_command(import_data)
if geocode_missing:
    app.cli.add_command(geocode_missing)

if __name__ == "__main__":
    # Local dev server only — Railway will use Gunicorn
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
