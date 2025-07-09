from app import app, db, User
from werkzeug.security import generate_password_hash

with app.app_context():
    user = User.query.filter_by(username="admin").first()

    if user:
        # Reset existing admin user's password
        user.password = generate_password_hash("newpassword123", method='pbkdf2:sha256')
        print("🔄 Admin password reset.")
    else:
        # Or create a new admin user
        new_admin = User(username="admin", password=generate_password_hash("wuhtkp", method='pbkdf2:sha256'))
        db.session.add(new_admin)
        print("✅ Admin user created.")

    db.session.commit()
