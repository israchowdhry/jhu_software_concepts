from flask import Blueprint, render_template

# create a blueprint to organize page routes
pages_bp = Blueprint("pages", __name__)

@pages_bp.get("/")
def home():
    # home page route
    return render_template("home.html", active_page="home")

@pages_bp.get("/projects")
def projects():
    # projects page route
    return render_template("projects.html", active_page="projects")

@pages_bp.get("/contact")
def contact():
    # contact page route
    return render_template("contact.html", active_page="contact")