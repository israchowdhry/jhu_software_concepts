import os
import pytest
import psycopg
import threading

from src.app import create_app

# Database url fixture
@pytest.fixture
def db_url():
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

# Reset database fixture
@pytest.fixture
def reset_db(db_url):
    """
    Ensures the applicants table exists and is empty
    before tests that require database interaction.
    """
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Create table if it does not exist
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
            # Clear table before test
            cur.execute("DELETE FROM applicants;")
        conn.commit()
    return True

# Flask test client fixture
@pytest.fixture
def client(monkeypatch, db_url):
    """
    Creates a Flask test client using the app factory.

    Also:
    - Injects DATABASE_URL into environment
    - Forces background threads to run inline (no async)
    """

    # Ensure DATABASE_URL is available for load_data / query_data
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Run background thread inline (no sleeps)
    real_thread = threading.Thread

    class InlineThread(real_thread):
        def start(self):
            self.run()

    monkeypatch.setattr(threading, "Thread", InlineThread)
    # Create fresh app instance using factory
    app = create_app()
    app.config["TESTING"] = True

    return app.test_client()
