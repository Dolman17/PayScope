from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, UserMixin

auth = Blueprint('auth', __name__)
login_manager = LoginManager()

# Dummy admin user
class Admin(UserMixin):
    id = 1
    username = "admin"
    password = "wuhtkp"

@login_manager.user_loader
def load_user(user_id):
    return Admin()

@auth.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        if username == Admin.username and password == Admin.password:
            login_user(Admin())
            return redirect(url_for("upload"))
        else:
            flash("Invalid credentials", "error")
    return render_template("login.html")

@auth.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
