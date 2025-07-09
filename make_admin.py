from app import app, db, User

with app.app_context():
    username = "admin"  # Replace with the correct username
    user = User.query.filter_by(username=username).first()
    if user:
        user.is_admin = True
        db.session.commit()
        print(f"✅ User '{username}' is now an admin.")
    else:
        print(f"❌ User '{username}' not found.")
