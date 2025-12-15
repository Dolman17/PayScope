from app import create_app
from models import JobPosting, CronRunLog

app = create_app()

with app.app_context():
    # 1) Reed imported status
    total = JobPosting.query.filter_by(source_site="reed").count()
    not_imported = JobPosting.query.filter_by(source_site="reed", imported=False).count()
    print("reed total:", total, "reed not imported:", not_imported)

    # 2) Latest cron log (stats should show reed source counters)
    log = CronRunLog.query.order_by(CronRunLog.id.desc()).first()
    if log:
        print("cron:", log.status, log.message)
        rs = (log.run_stats or "")[:1500]
        print("run_stats (head):", rs)
    else:
        print("cron: no CronRunLog rows found")
