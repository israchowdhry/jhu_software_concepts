import json

import psycopg
import runpy
import pytest

from src.db.load_data import load_data
import src.db.load_data as load_module
import src.worker.etl.clean as clean_module
import src.worker.etl.query_data as qd


@pytest.mark.db
def test_load_data_inserts_rows(reset_db, db_url, tmp_path):
    p = tmp_path / "rows.jsonl"
    p.write_text(
        json.dumps(
            {
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
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 0

    load_data(str(p), db_url=db_url)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT program, url, status, term FROM applicants;")
            row = cur.fetchone()
            assert row is not None
            assert all(row)


@pytest.mark.db
def test_load_data_is_idempotent_on_url_unique(reset_db, db_url, tmp_path):
    p = tmp_path / "rows.jsonl"
    row = {
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
    }
    p.write_text(json.dumps(row) + "\n" + json.dumps(row) + "\n", encoding="utf-8")

    load_data(str(p), db_url=db_url)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 1


@pytest.mark.db
def test_query_function_returns_expected_type(reset_db, db_url):
    from src.worker.etl import query_data

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
def test_save_and_load_data_roundtrip(tmp_path):
    data = [{"a": 1}, {"b": 2}]
    p = tmp_path / "out.json"
    clean_module.save_data(data, filename=str(p))
    loaded = clean_module.load_data(filename=str(p))
    assert loaded == data


@pytest.mark.db
def test_load_data_raises_when_database_url_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    p = tmp_path / "empty.jsonl"
    p.write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        load_module.load_data(str(p))


@pytest.mark.db
def test_query_data_get_conn_raises_when_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        qd.get_conn()


@pytest.mark.db
def test_db_config_raises_when_missing_env(monkeypatch):
    import src.db.db_config as db_config

    for k in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]:
        monkeypatch.delenv(k, raising=False)

    with pytest.raises(RuntimeError, match="Missing required DB environment variables"):
        db_config.get_db_settings()


@pytest.mark.db
def test_db_config_build_db_url_encodes_password(monkeypatch):
    import src.db.db_config as db_config

    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "gradcafe")
    monkeypatch.setenv("DB_USER", "gradcafe_app")
    monkeypatch.setenv("DB_PASSWORD", "p@ss:word!")

    url = db_config.build_db_url_from_env()
    assert url.startswith("postgresql://gradcafe_app:")
    assert "localhost:5432/gradcafe" in url
    assert "p@ss:word!" not in url


@pytest.mark.db
def test_db_config_resolve_db_url_db_star_fallback(monkeypatch):
    import src.db.db_config as db_config

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "gradcafe")
    monkeypatch.setenv("DB_USER", "gradcafe_app")
    monkeypatch.setenv("DB_PASSWORD", "dummy")

    resolved = db_config.resolve_db_url(None)
    assert "localhost" in resolved
    assert "/gradcafe" in resolved


@pytest.mark.db
def test_query_data_clamp_limit_bad_input_and_bounds():
    import src.worker.etl.query_data as qd_mod

    assert qd_mod.clamp_limit("not-an-int") == qd_mod.DEFAULT_LIMIT
    assert qd_mod.clamp_limit(None) == qd_mod.DEFAULT_LIMIT
    assert qd_mod.clamp_limit(0) == qd_mod.MIN_LIMIT
    assert qd_mod.clamp_limit(9999) == qd_mod.MAX_LIMIT


@pytest.mark.db
def test_query_data_resolve_db_url_db_star_fallback(monkeypatch):
    import src.worker.etl.query_data as qd_mod

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "gradcafe")
    monkeypatch.setenv("DB_USER", "gradcafe_app")
    monkeypatch.setenv("DB_PASSWORD", "dummy")

    resolved = qd_mod._resolve_db_url(None)  # pylint: disable=protected-access
    assert "localhost" in resolved
    assert "/gradcafe" in resolved


@pytest.mark.db
def test_query_data_resolve_db_url_raises_when_all_missing(monkeypatch):
    import src.worker.etl.query_data as qd_mod

    monkeypatch.delenv("DATABASE_URL", raising=False)
    for k in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]:
        monkeypatch.delenv(k, raising=False)

    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        qd_mod._resolve_db_url(None)  # pylint: disable=protected-access


@pytest.mark.db
def test_q3_returns_nones_when_no_row(monkeypatch):
    import src.worker.etl.query_data as qd_mod

    monkeypatch.setattr(qd_mod, "fetch_row", lambda *_a, **_k: None)
    assert qd_mod.q3() == (None, None, None, None)


@pytest.mark.db
def test_load_data_skips_blank_lines(tmp_path, db_url, reset_db) -> None:
    p = tmp_path / "sample.jsonl"
    p.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "university": "Test U",
                        "program_name": "CS",
                        "comments": "c",
                        "date_added": "January 01, 2026",
                        "entry_url": "http://example.com/blankline-1",
                        "applicant_status": "Accepted",
                        "start_term": "Fall 2026",
                        "international_american": "American",
                        "gpa": 3.9,
                        "gre_score": 165,
                        "gre_v_score": 160,
                        "gre_aw": 4.5,
                        "degree": "Masters",
                    }
                ),
                "",
                json.dumps(
                    {
                        "university": "Test U2",
                        "program_name": "CS",
                        "comments": "c2",
                        "date_added": "January 02, 2026",
                        "entry_url": "http://example.com/blankline-2",
                        "applicant_status": "Rejected",
                        "start_term": "Fall 2026",
                        "international_american": "International",
                        "gpa": 3.7,
                        "gre_score": 160,
                        "gre_v_score": 155,
                        "gre_aw": 4.0,
                        "degree": "Masters",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    from src.db.load_data import load_data as ld
    ld(str(p), db_url=db_url)


@pytest.mark.db
def test_load_data_dunder_main_runs_without_crashing(monkeypatch, tmp_path):
    """
    Covers the __main__ block in src/db/load_data.py safely.

    We:
    - provide DATABASE_URL so resolve_db_url doesn't raise
    - provide a dummy llm_extend_applicant_data.jsonl file so file-open won't fail
    - patch psycopg.connect so no real DB connection happens
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")

    # Ensure expected file exists relative to CWD
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llm_extend_applicant_data.jsonl").write_text("", encoding="utf-8")

    import psycopg

    class FakeCursor:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            # Commonly used patterns:
            # - SELECT COUNT(*) -> (0,)
            # - SELECT 1 WHERE EXISTS -> None or (1,)
            return (0,)

        def fetchall(self):
            return []

    class FakeConn:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: FakeConn())

    runpy.run_module("src.db.load_data", run_name="__main__")

import io
from datetime import datetime

import src.worker.consumer as consumer


@pytest.mark.db
def test_consumer_env_helpers_defaults_and_overrides(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    assert consumer._database_url() == "postgresql://x"

    monkeypatch.delenv("SEED_JSON", raising=False)
    assert consumer._seed_json_path().endswith("llm_extend_applicant_data.jsonl")

    monkeypatch.setenv("SEED_JSON", "/tmp/a.jsonl")
    assert consumer._seed_json_path() == "/tmp/a.jsonl"

    monkeypatch.delenv("TARGET_TABLE", raising=False)
    assert consumer._target_table() == "applicants"
    monkeypatch.setenv("TARGET_TABLE", "t")
    assert consumer._target_table() == "t"

    monkeypatch.delenv("ID_KEY", raising=False)
    assert consumer._id_key() == "url"
    monkeypatch.setenv("ID_KEY", "entry_url")
    assert consumer._id_key() == "entry_url"


@pytest.mark.db
def test_consumer_parse_date_added(monkeypatch):
    assert consumer._parse_date_added(None) is None
    assert consumer._parse_date_added("") is None
    assert consumer._parse_date_added("not-a-date") is None
    dt = consumer._parse_date_added("March 11, 2024")
    assert isinstance(dt, datetime)


@pytest.mark.db
def test_consumer_iter_jsonl_skips_blank_lines(monkeypatch):
    content = '\n{"a": 1}\n\n{"b": 2}\n'
    monkeypatch.setattr("builtins.open", lambda *_a, **_k: io.StringIO(content))
    rows = list(consumer._iter_jsonl("dummy.jsonl"))
    assert rows == [{"a": 1}, {"b": 2}]


@pytest.mark.db
def test_consumer_filter_newer_than_watermark(monkeypatch):
    rows = [
        {"date_added": "March 10, 2024"},
        {"date_added": "March 11, 2024"},
        {"date_added": "March 12, 2024"},
        {"date_added": None},
    ]
    filtered, max_seen = consumer._filter_newer_than_watermark(rows, "March 11, 2024")
    assert {"date_added": "March 12, 2024"} in filtered
    assert {"date_added": None} in filtered
    assert {"date_added": "March 10, 2024"} not in filtered
    assert {"date_added": "March 11, 2024"} not in filtered
    assert max_seen == "March 12, 2024"


@pytest.mark.db
def test_consumer_read_watermark_none_when_row_missing(monkeypatch):
    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): return None
        def fetchone(self): return None  # triggers row is None branch

    class FakeConn:
        def cursor(self): return FakeCursor()

    assert consumer._read_watermark(FakeConn(), "gradcafe") is None


@pytest.mark.db
def test_consumer_read_watermark_none_when_value_null(monkeypatch):
    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): return None
        def fetchone(self): return (None,)  # triggers row[0] is None branch

    class FakeConn:
        def cursor(self): return FakeCursor()

    assert consumer._read_watermark(FakeConn(), "gradcafe") is None


@pytest.mark.db
def test_consumer_read_watermark_returns_string(monkeypatch):
    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): return None
        def fetchone(self): return ("March 11, 2024",)

    class FakeConn:
        def cursor(self): return FakeCursor()

    assert consumer._read_watermark(FakeConn(), "gradcafe") == "March 11, 2024"


@pytest.mark.db
def test_consumer_write_watermark_executes(monkeypatch):
    executed = {"n": 0}

    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): executed["n"] += 1

    class FakeConn:
        def cursor(self): return FakeCursor()

    consumer._write_watermark(FakeConn(), "gradcafe", "March 12, 2024")
    assert executed["n"] == 1


@pytest.mark.db
def test_consumer_filter_updates_max_seen(monkeypatch):
    rows = [
        {"date_added": "March 12, 2024"},
        {"date_added": "March 13, 2024"},
    ]
    filtered, max_seen = consumer._filter_newer_than_watermark(rows, "March 11, 2024")
    assert len(filtered) == 2
    assert max_seen == "March 13, 2024"


@pytest.mark.db
def test_consumer_ensure_watermark_table_executes_sql():
    hits = {"execute": 0}

    class C:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): hits["execute"] += 1

    class Conn:
        def cursor(self): return C()

    consumer._ensure_watermark_table(Conn())
    assert hits["execute"] == 1


@pytest.mark.db
def test_consumer_normalize_row_llm_fallbacks_hit():
    # This forces execution of lines 135-140 including llm-generated OR llm_generated fallback
    raw = {
        "university": "U",
        "program_name": "P",
        "comments": "c",
        "date_added": "March 11, 2024",
        "entry_url": "http://x",
        "applicant_status": "Accepted",
        "start_term": "Fall 2025",
        "international_american": "International",
        "gpa": 3.9,
        "gre_score": 330,
        "gre_v_score": 165,
        "gre_aw": 4.5,
        "degree": "MS",
        # use snake_case version so the `or` branch is exercised
        "llm_generated_program": "CS",
        "llm_generated_university": "U",
    }
    out = consumer._normalize_row(raw)
    assert out["program"] == "U - P"
    assert out["llm_generated_program"] == "CS"
    assert out["llm_generated_university"] == "U"


@pytest.mark.db
def test_consumer_insert_applicants_batch_executes_executemany(monkeypatch):
    # Forces 211-266 including building SQL + values + with conn.cursor(): cur.executemany
    monkeypatch.setenv("TARGET_TABLE", "applicants")
    monkeypatch.setenv("ID_KEY", "url")

    calls = {"executemany": 0}

    class Cur:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def executemany(self, *_a, **_k): calls["executemany"] += 1

    class Conn:
        def cursor(self): return Cur()

    rows = [
        {
            "program": "U - P",
            "comments": "c",
            "date_added": "March 11, 2024",
            "url": "http://x",
            "status": "Accepted",
            "term": "Fall 2025",
            "us_or_international": "International",
            "gpa": 3.9,
            "gre": 330,
            "gre_v": 165,
            "gre_aw": 4.5,
            "degree": "MS",
            "llm_generated_program": "CS",
            "llm_generated_university": "U",
        }
    ]

    consumer._insert_applicants_batch(Conn(), rows)
    assert calls["executemany"] == 1


@pytest.mark.db
def test_consumer_handle_recompute_analytics_executes_analyze():
    # Covers 313-315
    executed = {"n": 0}

    class Cur:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): executed["n"] += 1

    class Conn:
        def cursor(self): return Cur()

    consumer.handle_recompute_analytics(Conn(), {})
    assert executed["n"] == 1


@pytest.mark.db
def test_query_data_dunder_main_prints_all_questions(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake:fake@127.0.0.1:5432/fake")

    import psycopg

    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False

        def execute(self, *_a, **_k): return None

        def fetchone(self):
            # Return MANY columns so row[0], row[1], row[2], row[3] etc never IndexError.
            # Use numbers that can print cleanly.
            return (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        def fetchall(self):
            # For extra_1 loop printing (program, n) or (n, program) depending on query
            return [("Some Program", 2), ("Other Program", 1)]

    class FakeConn:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def cursor(self): return FakeCursor()
        def commit(self): return None
        def rollback(self): return None

    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: FakeConn())

    printed = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: printed.append(" ".join(map(str, args))))

    runpy.run_module("src.worker.etl.query_data", run_name="__main__")

    out = "\n".join(printed)

    assert "Q1 (Count Fall 2026)" in out
    assert "Q2 (% International Applicants)" in out
    assert "Q3 (Averages of scores):" in out
    assert "avg_gpa" in out
    assert "avg_gre_q" in out
    assert "avg_gre_v" in out
    assert "avg_gre_aw" in out
    assert "Q4 (Avg GPA American Fall 2026)" in out
    assert "Q5 (% Acceptances Fall 2026)" in out
    assert "Q6 (Avg GPA Fall 2026 Acceptances)" in out
    assert "Q7 (JHU Masters CS count)" in out
    assert "Q8 (2026 PhD CS Acceptances" in out
    assert "Q9 (2026 PhD CS Acceptances using LLM fields)" in out
    assert "Extra Q1:" in out
    assert "Extra Q2:" in out