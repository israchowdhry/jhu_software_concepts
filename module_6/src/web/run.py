"""Flask service entrypoint for Docker."""

from __future__ import annotations

from flask import Flask

from src.web.app.app import create_app


def main() -> None:
    """Start the Flask web server."""
    app: Flask = create_app()
    app.run(host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
