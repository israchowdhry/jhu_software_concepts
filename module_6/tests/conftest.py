import os

import psycopg
import pytest

from src.web.app.app import create_app
import src.web.app.app as app_module


@pytest.fixture
def db_url():
    """
    DATABASE_URL used by:
      - src/load_data.py
      - src/query_data.py
    Use env var if set; otherwise default to local postgres.
    """
    return os.getenv(
        "DATABASE_URL",
        "postgresql://admin:password@localhost:5432/appdb"
    )


@pytest.fixture
def reset_db(db_url):
    """
    Ensures the applicants table exists and is empty before a DB test runs.
    """
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS applicants (
                    p_id SERIAL PRIMARY KEY,
                    program TEXT,
                    comments TEXT,
                    date_added DATE,
                    url TEXT UNIQUE,
                    status TEXT,
                    term TEXT,
                    us_or_international TEXT,
                    gpa FLOAT,
                    gre FLOAT,
                    gre_v FLOAT,
                    gre_aw FLOAT,
                    degree TEXT,
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
            """)
            cur.execute("DELETE FROM applicants;")
        conn.commit()
    return True


@pytest.fixture
def client(monkeypatch, db_url):
    """
    Returns a Flask test client using create_app() factory.

    Also:
    - sets DATABASE_URL for code under test
    - resets app state so tests don't leak into each other
    """
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Reset shared state
    app_module.PULL_STATE["running"] = False
    app_module.PULL_STATE["message"] = ""

    # Backward-compatible globals (still exist in the module)
    app_module.RESULTS_CACHE[:] = []
    app_module.HAS_RESULTS = False

    # New code uses app-attached state
    app_module.app.results_cache = []
    app_module.app.has_results = False

    app = create_app()
    return app.test_client()
