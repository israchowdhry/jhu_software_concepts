from flask import Flask
from app.pages.routes import pages_bp

def create_app():
    app = Flask(__name__)

    # import and register the blueprint that controls page routes
    app.register_blueprint(pages_bp)

    return app