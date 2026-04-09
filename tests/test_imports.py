from __future__ import annotations


def test_sector_filter_returns_expected_records(app, sample_job_records):
    from models import JobRecord

    with app.app_context():
        rows = JobRecord.query.filter(JobRecord.sector == "Social Care").all()
        assert len(rows) == 2
        assert {r.company_name for r in rows} == {"Alpha Care", "Bravo Care"}


def test_role_group_filter_returns_expected_records(app, sample_job_records):
    from models import JobRecord

    with app.app_context():
        rows = JobRecord.query.filter(
            JobRecord.job_role_group == "Care & Support Worker"
        ).all()
        assert len(rows) == 2
        assert all(r.sector == "Social Care" for r in rows)


def test_county_filter_returns_expected_records(app, sample_job_records):
    from models import JobRecord

    with app.app_context():
        rows = JobRecord.query.filter(JobRecord.county == "Cheshire").all()
        assert len(rows) == 1
        assert rows[0].company_name == "City Nursing"


def test_combined_filters_narrow_correctly(app, sample_job_records):
    from models import JobRecord

    with app.app_context():
        rows = (
            JobRecord.query.filter(JobRecord.sector == "Social Care")
            .filter(JobRecord.job_role_group == "Care & Support Worker")
            .filter(JobRecord.county == "Staffordshire")
            .all()
        )
        assert len(rows) == 2


def test_average_pay_for_social_care_is_stable(app, sample_job_records):
    from models import JobRecord

    with app.app_context():
        rows = JobRecord.query.filter(JobRecord.sector == "Social Care").all()
        avg = sum(r.pay_rate for r in rows) / len(rows)
        assert round(avg, 2) == 12.85