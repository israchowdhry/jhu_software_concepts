# src/db_config.py
from __future__ import annotations

import os
from urllib.parse import quote_plus

def get_db_settings() -> dict[str, str]:
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required DB environment variables: {', '.join(missing)}")

    return {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }

def build_db_url_from_env() -> str:
    s = get_db_settings()
    # URL-encode password safely
    pw = quote_plus(s["password"])
    return f"postgresql://{s['user']}:{pw}@{s['host']}:{s['port']}/{s['dbname']}"
