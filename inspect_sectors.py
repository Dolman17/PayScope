from app import create_app
from extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    sql = """
    SELECT
        COALESCE(NULLIF(TRIM(sector), ''), '(blank)') AS sector_value,
        COUNT(*) AS c
    FROM job_record
    WHERE sector IS NULL OR TRIM(sector) = '' OR sector = 'Other'
    GROUP BY COALESCE(NULLIF(TRIM(sector), ''), '(blank)')
    ORDER BY c DESC
    LIMIT 50
    """
    rows = db.session.execute(text(sql)).all()
    print(rows)
