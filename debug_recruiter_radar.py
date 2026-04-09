from __future__ import annotations

from datetime import date, datetime, timedelta
import math
from typing import Iterable

from sqlalchemy import func, or_

from app import create_app
from models import JobRecord
from app.blueprints.recruiter import _geocode_flexible_location, _bounding_box


SEARCH_ROLE = "senior"
SEARCH_LOCATION = "WS13 6BL"
RADIUS_MILES = 25
LOOKBACK_DAYS = 180
LIMIT = 100


def miles_between(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Haversine distance in miles.
    """
    r_miles = 3958.8

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_miles * c


def print_header(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def print_records(records: Iterable[JobRecord], centre_lat: float | None = None, centre_lon: float | None = None) -> None:
    for r in records:
        distance = None
        if (
            centre_lat is not None
            and centre_lon is not None
            and r.latitude is not None
            and r.longitude is not None
        ):
            distance = miles_between(centre_lat, centre_lon, float(r.latitude), float(r.longitude))

        print(
            {
                "id": r.id,
                "company_name": r.company_name,
                "job_role": r.job_role,
                "job_role_group": r.job_role_group,
                "postcode": r.postcode,
                "county": r.county,
                "latitude": r.latitude,
                "longitude": r.longitude,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "distance_miles": round(distance, 2) if distance is not None else None,
            }
        )


def main() -> None:
    app = create_app()

    with app.app_context():
        print_header("Recruiter Radar Diagnostic")

        print(f"SEARCH_ROLE     = {SEARCH_ROLE}")
        print(f"SEARCH_LOCATION = {SEARCH_LOCATION}")
        print(f"RADIUS_MILES    = {RADIUS_MILES}")
        print(f"LOOKBACK_DAYS   = {LOOKBACK_DAYS}")

        centre_lat, centre_lon = _geocode_flexible_location(SEARCH_LOCATION)
        print(f"\nGeocoded centre: lat={centre_lat}, lon={centre_lon}")

        if centre_lat is None or centre_lon is None:
            print("\nCould not geocode the search location. Stopping.")
            return

        bbox = _bounding_box(centre_lat, centre_lon, RADIUS_MILES)
        print(f"Bounding box: {bbox}")

        today = date.today()
        start_dt = datetime.combine(today - timedelta(days=LOOKBACK_DAYS), datetime.min.time())

        role_query = (
            JobRecord.query
            .filter(
                or_(
                    func.lower(func.coalesce(JobRecord.job_role, "")).like(f"%{SEARCH_ROLE.lower()}%"),
                    func.lower(func.coalesce(JobRecord.job_role_group, "")).like(f"%{SEARCH_ROLE.lower()}%"),
                )
            )
            .order_by(JobRecord.created_at.desc())
        )

        all_role_matches = role_query.limit(LIMIT).all()
        total_role_matches = role_query.count()

        print_header(f"1) ALL role matches containing '{SEARCH_ROLE}'")
        print(f"Count = {total_role_matches}")
        print_records(all_role_matches, centre_lat, centre_lon)

        with_coords_query = role_query.filter(
            JobRecord.latitude.isnot(None),
            JobRecord.longitude.isnot(None),
        )
        with_coords = with_coords_query.limit(LIMIT).all()
        total_with_coords = with_coords_query.count()

        print_header("2) Role matches with coordinates")
        print(f"Count = {total_with_coords}")
        print_records(with_coords, centre_lat, centre_lon)

        within_bbox_query = with_coords_query.filter(
            JobRecord.latitude >= bbox["min_lat"],
            JobRecord.latitude <= bbox["max_lat"],
            JobRecord.longitude >= bbox["min_lon"],
            JobRecord.longitude <= bbox["max_lon"],
        )
        within_bbox = within_bbox_query.limit(LIMIT).all()
        total_within_bbox = within_bbox_query.count()

        print_header(f"3) Role matches within {RADIUS_MILES} mile bounding box")
        print(f"Count = {total_within_bbox}")
        print_records(within_bbox, centre_lat, centre_lon)

        recent_within_bbox_query = within_bbox_query.filter(
            JobRecord.created_at >= start_dt,
        )
        recent_within_bbox = recent_within_bbox_query.limit(LIMIT).all()
        total_recent_within_bbox = recent_within_bbox_query.count()

        print_header(f"4) Role matches within bbox and within last {LOOKBACK_DAYS} days")
        print(f"Count = {total_recent_within_bbox}")
        print_records(recent_within_bbox, centre_lat, centre_lon)

        print_header("5) Summary")
        print(
            {
                "total_role_matches": total_role_matches,
                "with_coordinates": total_with_coords,
                "within_radius_bbox": total_within_bbox,
                "within_radius_and_lookback": total_recent_within_bbox,
            }
        )

        print_header("6) Likely cause")
        if total_role_matches == 0:
            print("No matching role text exists in job_role or job_role_group.")
        elif total_with_coords == 0:
            print("Matching roles exist, but none of them have latitude/longitude.")
        elif total_within_bbox == 0:
            print("Matching roles with coordinates exist, but none fall inside the 25-mile search area.")
        elif total_recent_within_bbox == 0:
            print("Matching local roles exist, but they are older than the lookback window.")
        else:
            print("Rows do exist for this search. If Recruiter Radar still shows none, the route logic or template integration needs checking.")


if __name__ == "__main__":
    main()