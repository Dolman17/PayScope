import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from app import app, db, JobRecord

def enhanced_backfill_counties():
    geolocator = Nominatim(user_agent="payratemap")
    updated = 0
    failed = 0

    with app.app_context():
        records = JobRecord.query.filter(
            JobRecord.latitude.isnot(None),
            JobRecord.longitude.isnot(None),
            (JobRecord.county.is_(None) | (JobRecord.county == ""))
        ).all()

        print(f"🧭 Found {len(records)} records that need county backfilling.")

        for record in records:
            try:
                print(f"🌍 Reverse geocoding for record {record.id}: {record.latitude}, {record.longitude}")
                location = geolocator.reverse((record.latitude, record.longitude), exactly_one=True, timeout=10)

                if location and location.raw.get("address"):
                    address = location.raw["address"]
                    county = address.get("county") or address.get("state_district") or address.get("region") or address.get("suburb")

                    if county:
                        record.county = county
                        db.session.commit()
                        updated += 1
                        print(f"✅ Updated record {record.id} with county: {county}")
                    else:
                        print(f"⚠️ No suitable county found for record {record.id}. Address: {address}")
                        failed += 1
                else:
                    print(f"⚠️ No location found for record {record.id}")
                    failed += 1

                time.sleep(2)

            except (GeocoderTimedOut, GeocoderServiceError) as e:
                print(f"❌ Geocoding failed for record {record.id}: {e}")
                failed += 1

        print(f"\n✅ Finished. Updated: {updated}, Failed: {failed}")

if __name__ == "__main__":
    enhanced_backfill_counties()

