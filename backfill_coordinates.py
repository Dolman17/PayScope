from app import app, db, JobRecord
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

geolocator = Nominatim(user_agent="payratemap")

def geocode_postcode(postcode):
    try:
        location = geolocator.geocode(postcode, timeout=10)
        if location:
            return location.latitude, location.longitude
    except GeocoderTimedOut:
        pass
    return None, None

with app.app_context():
    records = JobRecord.query.filter((JobRecord.latitude == None) | (JobRecord.longitude == None)).all()
    print(f"🔍 Found {len(records)} records to update")

    updated = 0
    for record in records:
        lat, lon = geocode_postcode(record.postcode)
        if lat and lon:
            record.latitude = lat
            record.longitude = lon
            updated += 1
            print(f"✅ Updated record {record.id} → ({lat}, {lon})")
        else:
            print(f"⚠️ Could not geocode postcode: {record.postcode}")

    db.session.commit()
    print(f"🎉 Done. {updated} records updated.")
