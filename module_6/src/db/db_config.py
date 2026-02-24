"""
Database configuration helpers.

This module reads PostgreSQL connection settings from environment variables
and can build a DATABASE_URL string safely (URL-encoding the password).
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def get_db_settings() -> dict[str, str]:
    """
    Read DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD from the environment.

    :return: Mapping with host/port/dbname/user/password values.
    :rtype: dict[str, str]
    :raises RuntimeError: If any required environment variable is missing.
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


def build_db_url_from_env() -> str:
    """
    Build a PostgreSQL DATABASE_URL from DB_* environment variables.

    The password is URL-encoded to handle special characters safely.

    :return: A PostgreSQL connection URL.
    :rtype: str
    """
    settings = get_db_settings()
    pw = quote_plus(settings["password"])
    return (
        f"postgresql://{settings['user']}:{pw}"
        f"@{settings['host']}:{settings['port']}/{settings['dbname']}"
    )

def resolve_db_url(db_url: str | None = None) -> str:
    """
    Resolve a database URL.

    Priority:
    1) explicit db_url argument
    2) DATABASE_URL environment variable
    3) build from DB_* variables (if all present)

    :param db_url: Optional explicit override.
    :type db_url: str | None
    :return: Resolved PostgreSQL URL.
    :rtype: str
    :raises RuntimeError: If no URL can be resolved.
    """
    if db_url:
        return db_url

    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    if all(os.getenv(k) for k in required):
        return build_db_url_from_env()

    raise RuntimeError("DATABASE_URL is not set")
