from app import app, db, User
from werkzeug.security import generate_password_hash

with app.app_context():
    user = User.query.filter_by(username="admin").first()

    if user:
        user.password = generate_password_hash("password123", method='pbkdf2:sha256')
        db.session.commit()
        print("✅ Password successfully reset for admin.")
    else:
        print("❌ Admin user does not exist.")
