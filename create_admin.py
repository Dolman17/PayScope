from app import app, db, User
from werkzeug.security import generate_password_hash

with app.app_context():
    hashed_pw = generate_password_hash("wuthkp", method='pbkdf2:sha256')
    admin = User(username="admin", password=hashed_pw)

    db.session.add(admin)
    db.session.commit()
    print("✅ Admin user created.")
