from app import create_app
from extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    sql = text("""
        SELECT
            COALESCE(
                NULLIF(TRIM(job_role_group),''),
                NULLIF(TRIM(job_role),''),
                '(blank)'
            ) AS role,
            COUNT(*) AS c
        FROM job_record
        WHERE sector = 'Other'
           OR sector IS NULL
           OR TRIM(sector) = ''
        GROUP BY 1
        ORDER BY c DESC
        LIMIT 50
    """)
    rows = db.session.execute(sql).all()

    for role, count in rows:
        print(f"{role}: {count}")
