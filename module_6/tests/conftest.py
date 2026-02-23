import os
import pytest
import psycopg
import threading

from src.app import create_app
import src.app as app_module


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
        "postgresql://postgres:postgres@localhost:5432/postgres"
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
    - forces background pull thread to run inline (no sleep needed)
    - resets global state so tests don't leak into each other
    """
    # Make sure DB URL is available for query_data/load_data
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Force threading.Thread(...).start() to run immediately in tests
    real_thread = threading.Thread

    class InlineThread(real_thread):
        def start(self):
            self.run()

    monkeypatch.setattr(threading, "Thread", InlineThread)

    # Reset shared state
    app_module.PULL_STATE["running"] = False
    app_module.PULL_STATE["message"] = ""
    app_module.RESULTS_CACHE[:] = []
    app_module.HAS_RESULTS = False

    app = create_app()
    return app.test_client()
