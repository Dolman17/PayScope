from app import app, User

with app.app_context():
    user = User.query.filter_by(username="admin").first()
    if user:
        print("✅ Admin user found:", user.username)
        print("🔑 Password hash:", user.password)
    else:
        print("❌ Admin user not found.")
