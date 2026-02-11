import os
import pytest
import psycopg
import threading

from src import app as app_module

@pytest.fixture
def db_url():
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

@pytest.fixture
def reset_db(db_url):
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
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Run background thread inline
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

    return app_module.app.test_client()
