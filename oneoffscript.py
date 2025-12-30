from app import create_app
from extensions import db
from models import JobRoleMapping

app = create_app()
with app.app_context():
    mappings = JobRoleMapping.query.all()
    changed = 0

    for m in mappings:
        original = m.canonical_role or ""
        cleaned = clean_canonical_label(original)
        # Skip if cleaner results in empty / identical
        if not cleaned or cleaned == original:
            continue

        m.canonical_role = cleaned
        changed += 1

    db.session.commit()
    print(f"Updated {changed} canonical roles")
