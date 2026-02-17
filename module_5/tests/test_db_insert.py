import json
import pytest
import psycopg
import src.app as app_module
from src.load_data import load_data
import src.clean as clean_module
import src.load_data as load_module
import src.query_data as qd



@pytest.mark.db
def test_insert_on_pull_creates_rows(client, reset_db, db_url, monkeypatch, tmp_path):
    """
    Requirement:
    - Before pull: applicants table empty
    - After POST /pull-data: rows exist with required non-null fields
    """

    def fake_scrape():
        return [{
            "university": "Test University",
            "program_name": "Computer Science",
            "comments": "Test comment",
            "date_added": "January 1, 2026",
            "entry_url": "http://example.com/unique1",
            "applicant_status": "Accepted",
            "start_term": "Fall 2026",
            "international_american": "American",
            "gpa": 3.8,
            "gre_score": 165,
            "gre_v_score": 160,
            "gre_aw": 4.5,
            "degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Test University",
        }]

    monkeypatch.setattr(app_module, "scrape_data", fake_scrape)
    monkeypatch.setattr(app_module, "clean_data", lambda rows: rows)

    # Use real load_data() so we truly test DB insert behavior
    monkeypatch.setattr(app_module, "load_data", lambda path: load_data(path, db_url=db_url))

    # Write JSONL to a temp file (so tests don't touch project files)
    app_module.JSONL_PATH = str(tmp_path / "rows.jsonl")

    # Before: empty
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 0

    # Ensure not busy before calling /pull-data
    app_module.PULL_STATE["running"] = False

    resp = client.post("/pull-data")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # After: row exists and required fields are non-null
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT program, url, status, term FROM applicants;")
            row = cur.fetchone()
            assert row is not None
            assert all(row)  # none should be NULL


@pytest.mark.db
def test_duplicate_pull_does_not_create_duplicates(client, reset_db, db_url, monkeypatch, tmp_path):
    """
    Requirement:
    - Pulling same data twice should not create duplicate rows (url is UNIQUE)
    """

    def fake_scrape():
        return [{
            "university": "Test University",
            "program_name": "Computer Science",
            "comments": "Test comment",
            "date_added": "January 1, 2026",
            "entry_url": "http://example.com/unique2",
            "applicant_status": "Accepted",
            "start_term": "Fall 2026",
            "international_american": "American",
            "gpa": 3.8,
            "gre_score": 165,
            "gre_v_score": 160,
            "gre_aw": 4.5,
            "degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Test University",
        }]

    monkeypatch.setattr(app_module, "scrape_data", fake_scrape)
    monkeypatch.setattr(app_module, "clean_data", lambda rows: rows)
    monkeypatch.setattr(app_module, "load_data", lambda path: load_data(path, db_url=db_url))

    app_module.JSONL_PATH = str(tmp_path / "rows.jsonl")

    client.post("/pull-data")
    client.post("/pull-data")

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 1


@pytest.mark.db
def test_query_function_returns_expected_type(reset_db, db_url):
    """
    Requirement:
    - Query function returns expected structure/type used by analysis template
    We verify q1() returns an int.
    """
    from src import query_data

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO applicants (program, url, status, term)
                VALUES (%s, %s, %s, %s)
            """, ("X - CS", "http://example.com/q", "Accepted", "Fall 2026"))
        conn.commit()

    result = query_data.q1(db_url=db_url)

    assert isinstance(result, int)
    assert result == 1

@pytest.mark.db
def test_clean_data_skips_missing_combined_html():
    rows = [{"combined_html": None, "entry_url": "x"}]
    assert clean_module.clean_data(rows) == []


@pytest.mark.db
def test_clean_data_skips_when_less_than_4_columns():
    combined_html = """
    <table>
      <tr><td>Only</td><td>Two</td><td>Cols</td></tr>
    </table>
    """
    rows = [{"combined_html": combined_html, "entry_url": "x"}]
    assert clean_module.clean_data(rows) == []


@pytest.mark.db
def test_clean_data_waitlisted_and_american(monkeypatch):
    """
    Covers:
    - waitlist decision branch
    - american branch
    - entry_url None => skips detail fetch branch
    """
    combined_html = """
    <table>
      <tr>
        <td>Some University</td>
        <td>Psychology · PsyD</td>
        <td>January 1, 2026</td>
        <td>Waitlisted</td>
      </tr>
      <tr><td colspan="4">Fall 2026 American</td></tr>
    </table>
    """
    rows = [{"combined_html": combined_html, "entry_url": None}]
    cleaned = clean_module.clean_data(rows)
    assert len(cleaned) == 1
    assert cleaned[0]["applicant_status"] == "Waitlisted"
    assert cleaned[0]["international_american"] == "American"
    # Degree detection PsyD branch
    assert cleaned[0]["degree"] == "PsyD"


@pytest.mark.db
def test_save_and_load_data_roundtrip(tmp_path):
    """
    Covers clean.py save_data + load_data functions.
    """
    data = [{"a": 1}, {"b": 2}]
    p = tmp_path / "out.json"
    clean_module.save_data(data, filename=str(p))
    loaded = clean_module.load_data(filename=str(p))
    assert loaded == data

@pytest.mark.db
def test_load_data_raises_when_database_url_missing(tmp_path, monkeypatch):
    # Covers src/load_data.py line where it raises if DATABASE_URL missing
    monkeypatch.delenv("DATABASE_URL", raising=False)
    p = tmp_path / "empty.jsonl"
    p.write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        load_module.load_data(str(p))


@pytest.mark.db
def test_query_data_get_conn_raises_when_database_url_missing(monkeypatch):
    # Covers src/query_data.py get_conn raise branch
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        qd.get_conn()