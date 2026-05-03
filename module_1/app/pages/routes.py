import json
from pathlib import Path
from flask import Blueprint, render_template

# create a blueprint to organize page routes
pages_bp = Blueprint("pages", __name__)

@pages_bp.get("/")
def home():
    # home page route
    return render_template("home.html", active_page="home")

@pages_bp.get("/projects")
def projects():
    data_path = Path("data/projects.json")

    with open(data_path, "r", encoding="utf-8") as f:
        projects_data = json.load(f)

    return render_template(
        "projects.html",
        projects=projects_data,
        active_page="projects"
    )

@pages_bp.get("/contact")
def contact():
    # contact page route
    return render_template("contact.html", active_page="contact")

