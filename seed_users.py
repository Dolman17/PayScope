from app import app, db, User
from werkzeug.security import generate_password_hash

with app.app_context():
    # Check for existing superuser
    if not User.query.filter_by(username='superuser').first():
        superuser = User(
            username='superuser',
            password=generate_password_hash('superpassword'),
            admin_level=1
        )
        db.session.add(superuser)
        print("✅ Superuser created.")
    else:
        print("ℹ️ Superuser already exists.")

    # Check for existing admin
    if not User.query.filter_by(username='admin').first():
        admin = User(
            username='admin',
            password=generate_password_hash('adminpassword'),
            admin_level=2
        )
        db.session.add(admin)
        print("✅ Admin created.")
    else:
        print("ℹ️ Admin already exists.")

    db.session.commit()
