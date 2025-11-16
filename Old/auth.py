from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, UserMixin, current_user
from werkzeug.security import check_password_hash
from models import User  # Assuming your model is defined in models.py

auth = Blueprint('auth', __name__)
login_manager = LoginManager()
login_manager.login_view = 'auth.login'

# Load user from session
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


# Login route
@auth.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            flash("Login successful", "success")
            return redirect(url_for("upload"))  # Adjust as needed
        else:
            flash("Invalid credentials", "error")

    return render_template("login.html")


# Logout route
@auth.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out", "success")
    return redirect(url_for("auth.login"))
