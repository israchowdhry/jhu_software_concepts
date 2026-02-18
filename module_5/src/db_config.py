"""
Database configuration utilities.

Reads PostgreSQL connection settings from environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    # Always load the .env from the project root (module_5/.env)
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")
except ImportError:
    # In CI, env vars are typically injected by the runner.
    # Locally, install python-dotenv to load values from a .env file.
    pass


def get_db_settings() -> dict[str, str]:
    """
    Load DB connection settings from environment variables.

    Required:
      - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Missing required DB environment variables: {', '.join(missing)}"
        )

    return {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }