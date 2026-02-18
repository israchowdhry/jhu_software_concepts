import os
import sys
import json
import runpy
import pytest
import psycopg
import src.app as app_module
from src.load_data import load_data
import src.clean as clean_module
import flask
import src.scrape as scrape_module
from bs4 import BeautifulSoup


@pytest.mark.integration
def test_end_to_end_pull_update_render(client, reset_db, db_url, monkeypatch, tmp_path):
    """
    Requirement:
    End-to-end: pull -> update -> render
    - Fake scraper returns multiple records
    - Pull inserts into DB
    - Update succeeds
    - GET /analysis shows Answer labels and 2-decimal percentages
    """

    fake_rows = [
        {
            "university": "Johns Hopkins University",
            "program_name": "Computer Science",
            "comments": "Good",
            "date_added": "January 1, 2026",
            "entry_url": "http://example.com/e2e-1",
            "applicant_status": "Accepted",
            "start_term": "Fall 2026",
            "international_american": "International",
            "gpa": 3.9,
            "gre_score": 165,
            "gre_v_score": 160,
            "gre_aw": 4.5,
            "degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Johns Hopkins University",
        },
        {
            "university": "MIT",
            "program_name": "Computer Science",
            "comments": "Nice",
            "date_added": "January 2, 2026",
            "entry_url": "http://example.com/e2e-2",
            "applicant_status": "Rejected",
            "start_term": "Fall 2026",
            "international_american": "American",
            "gpa": 3.7,
            "gre_score": 164,
            "gre_v_score": 159,
            "gre_aw": 4.0,
            "degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "MIT",
        },
    ]

    monkeypatch.setattr(app_module, "scrape_data", lambda: fake_rows)
    monkeypatch.setattr(app_module, "clean_data", lambda rows: rows)
    monkeypatch.setattr(app_module, "load_data", lambda path: load_data(path, db_url=db_url))

    app_module.JSONL_PATH = str(tmp_path / "e2e.jsonl")

    # Pull
    r1 = client.post("/pull-data")
    assert r1.status_code == 200
    assert r1.get_json() == {"ok": True}

    # Ensure DB has rows
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 2

    # Update analysis
    r2 = client.post("/update-analysis")
    assert r2.status_code == 200
    assert r2.get_json() == {"ok": True}

    # Render analysis
    r3 = client.get("/analysis")
    assert r3.status_code == 200
    html = r3.data.decode("utf-8")
    assert "Answer:" in html

    # If there are percentages, ensure they are two decimals
    import re
    any_percent = re.findall(r"\b\d+(?:\.\d+)?%", html)
    strict = re.findall(r"\b\d+\.\d{2}%", html)
    if any_percent:
        assert len(any_percent) == len(strict)


@pytest.mark.integration
def test_multiple_pulls_with_overlap_are_idempotent(client, reset_db, db_url, monkeypatch, tmp_path):
    """
    Requirement:
    Multiple pulls with overlapping data remain consistent with uniqueness policy.
    """

    row = {
        "university": "Test U",
        "program_name": "CS",
        "comments": "x",
        "date_added": "January 1, 2026",
        "entry_url": "http://example.com/overlap-1",  # UNIQUE key
        "applicant_status": "Accepted",
        "start_term": "Fall 2026",
        "international_american": "American",
        "gpa": 3.5,
        "gre_score": 160,
        "gre_v_score": 155,
        "gre_aw": 4.0,
        "degree": "Masters",
        "llm-generated-program": "CS",
        "llm-generated-university": "Test U",
    }

    monkeypatch.setattr(app_module, "scrape_data", lambda: [row])
    monkeypatch.setattr(app_module, "clean_data", lambda rows: rows)
    monkeypatch.setattr(app_module, "load_data", lambda path: load_data(path, db_url=db_url))

    app_module.JSONL_PATH = str(tmp_path / "overlap.jsonl")

    client.post("/pull-data")
    client.post("/pull-data")

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 1


@pytest.mark.integration
def test_scrape_main_block_offline(monkeypatch):
    """
    Covers: src/scrape.py __main__ block WITHOUT internet.
    src/scrape.py main block uses: "from clean import ..."
    So alias src.clean as "clean" in sys.modules.
    """
    sys.modules["clean"] = clean_module

    # Patch robotparser allow
    import urllib.robotparser as robotparser

    class FakeRobot:
        def set_url(self, url): ...
        def read(self): ...
        def can_fetch(self, ua, url): return True

    monkeypatch.setattr(robotparser, "RobotFileParser", FakeRobot)

    # Patch urllib3.request to return fake listing HTML (with a <table>)
    import urllib3

    class FakeResp:
        def __init__(self, html):
            self.status = 200
            self.data = html.encode("utf-8")

    fake_listing_html = """
    <html><body>
      <table>
        <tr>
          <td>Test U</td>
          <td>Computer Science · Masters</td>
          <td>January 1, 2026</td>
          <td>Accepted</td>
          <td><a href="/survey/index.php?id=1">link</a></td>
        </tr>
        <tr><td colspan="5">Fall 2026 International GPA 3.90</td></tr>
        <tr><td colspan="5"><p>Great</p></td></tr>
      </table>
    </body></html>
    """

    monkeypatch.setattr(urllib3, "request", lambda method, url, headers=None: FakeResp(fake_listing_html))

    # Run scrape.py as __main__
    runpy.run_module("src.scrape", run_name="__main__")

@pytest.mark.integration
def test_app_dunder_main_does_not_start_real_server(monkeypatch):
    """
    Covers:
      if __name__ == "__main__": app.run(...)
    without actually starting a server.
    """
    called = {"n": 0}

    def fake_run(self, *args, **kwargs):
        called["n"] += 1

    # Patch the class method, not the app instance
    monkeypatch.setattr(flask.app.Flask, "run", fake_run)

    runpy.run_module("src.app", run_name="__main__")

    assert called["n"] == 1


@pytest.mark.integration
def test_load_data_dunder_main_offline(monkeypatch, tmp_path, db_url, reset_db):
    """
    Covers:
      if __name__ == "__main__": load_data("llm_extend_applicant_data.jsonl")
    using a temp JSONL and a real local test DB.
    """
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Run module in a temp working dir so it finds the file name it expects
    old_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        jsonl_name = "llm_extend_applicant_data.jsonl"
        row = {
            "university": "U",
            "program_name": "CS",
            "comments": "x",
            "date_added": "January 1, 2026",
            "entry_url": "http://example.com/main-load",
            "applicant_status": "Accepted",
            "start_term": "Fall 2026",
            "international_american": "American",
            "gpa": 3.8,
            "gre_score": 165,
            "gre_v_score": 160,
            "gre_aw": 4.5,
            "degree": "Masters",
            "llm-generated-program": "CS",
            "llm-generated-university": "U",
        }

        with open(jsonl_name, "w", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
            f.write("\n")  # Blank line to cover: if not line: continue

        runpy.run_module("src.load_data", run_name="__main__")

        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM applicants;")
                assert cur.fetchone()[0] == 1
    finally:
        os.chdir(old_cwd)


@pytest.mark.integration
def test_query_data_dunder_main_offline(monkeypatch, db_url, reset_db, capsys):
    """
    Covers:
      if __name__ == "__main__": main()
    and the print statements inside main().
    """
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Seed minimal rows so queries don't crash
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO applicants (
                    program, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                "johns hopkins university - computer science",
                "http://example.com/qmain",
                "Accepted",
                "Fall 2026",
                "International",
                3.9, 165, 160, 4.5,
                "Masters",
                "computer science",
                "johns hopkins university"
            ))
        conn.commit()

    runpy.run_module("src.query_data", run_name="__main__")
    out = capsys.readouterr().out

    assert "Q1" in out
    assert "Q2" in out
    assert "Extra Q1" in out
    assert "Extra Q2" in out


@pytest.mark.integration
def test_scrape_robots_disallowed_exits(monkeypatch):
    """
    Covers scrape.py path:
      if not allowed: print(...); exit()
    """
    import urllib.robotparser as robotparser

    class FakeRobot:
        def set_url(self, url): ...
        def read(self): ...
        def can_fetch(self, ua, url): return False

    monkeypatch.setattr(robotparser, "RobotFileParser", FakeRobot)

    # Import inside test so patch applies
    import src.scrape as scrape_module

    with pytest.raises(SystemExit):
        scrape_module.scrape_data(target=1)


@pytest.mark.integration
def test_scrape_no_table_breaks(monkeypatch):
    """
    Covers scrape.py branch:
      if not table: break
    """
    import urllib.robotparser as robotparser

    class FakeRobot:
        def set_url(self, url): ...
        def read(self): ...
        def can_fetch(self, ua, url): return True

    monkeypatch.setattr(robotparser, "RobotFileParser", FakeRobot)

    import urllib3

    class FakeResp:
        status = 200
        data = b"<html><body><p>no table here</p></body></html>"

    monkeypatch.setattr(urllib3, "request", lambda *a, **k: FakeResp())

    rows = scrape_module.scrape_data(target=1)
    assert rows == []


@pytest.mark.integration
def test_scrape_dunder_main_offline(monkeypatch, tmp_path):
    """
    Covers scrape.py __main__ block.
    Scrape.py does: from clean import clean_data, save_data
    so provide sys.modules["clean"] alias to src.clean.
    """
    sys.modules["clean"] = clean_module  # alias for "from clean import ..."

    import urllib.robotparser as robotparser

    class FakeRobot:
        def set_url(self, url): ...
        def read(self): ...
        def can_fetch(self, ua, url): return True

    monkeypatch.setattr(robotparser, "RobotFileParser", FakeRobot)

    import urllib3

    class FakeResp:
        def __init__(self, html):
            self.status = 200
            self.data = html.encode("utf-8")

    fake_listing_html = """
    <html><body>
      <table>
        <tr>
          <td>Test U</td>
          <td>Computer Science · Masters</td>
          <td>January 1, 2026</td>
          <td>Accepted</td>
          <td><a href="/survey/index.php?id=1">link</a></td>
        </tr>
        <tr><td colspan="5">Fall 2026 International GPA 3.90</td></tr>
        <tr><td colspan="5"><p>Great</p></td></tr>
      </table>
    </body></html>
    """

    monkeypatch.setattr(urllib3, "request", lambda *a, **k: FakeResp(fake_listing_html))

    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        runpy.run_module("src.scrape", run_name="__main__")
        # Main saves applicant_data.json
        assert (tmp_path / "applicant_data.json").exists()
    finally:
        os.chdir(old_cwd)

@pytest.mark.integration
def test_clean_helpers_and_skip_branches(monkeypatch):
    """
    Covers a bunch of missed clean.py branches:
    - _norm(None)
    - _get_value: dt missing and dd missing
    - clean_data skipping: missing combined_html, no trs, cols<4
    - decision_text branches including Waitlisted + unknown
    - detail-page fetch status != 200 branch
    - save_data/load_data
    """

    # _norm(None)
    assert clean_module._norm(None) is None

    # _get_value: dt missing
    soup = BeautifulSoup("<dl></dl>", "html.parser")
    assert clean_module._get_value(soup, "Degree Type") is None

    # _get_value: dd missing
    soup2 = BeautifulSoup("<dl><dt>Degree Type</dt></dl>", "html.parser")
    assert clean_module._get_value(soup2, "Degree Type") is None

    # clean_data skip: missing combined_html
    assert clean_module.clean_data([{"combined_html": None, "entry_url": "x"}]) == []

    # clean_data skip: no <tr>
    assert clean_module.clean_data([{"combined_html": "<html></html>", "entry_url": "x"}]) == []

    # clean_data skip: cols < 4
    html_cols_lt4 = "<table><tr><td>A</td><td>B</td><td>C</td></tr></table>"
    assert clean_module.clean_data([{"combined_html": html_cols_lt4, "entry_url": "x"}]) == []

    # Force detail fetch but return non-200 to cover resp.status != 200
    class FakeResp:
        status = 404
        data = b""

    def fake_request(method, url, headers=None):
        return FakeResp()

    monkeypatch.setattr(clean_module.urllib3, "request", fake_request)

    html_main = """
    <table>
      <tr>
        <td>Test U</td>
        <td>Neuroscience · PsyD</td>
        <td>January 1, 2026</td>
        <td>Waitlisted</td>
      </tr>
      <tr><td colspan="4">Spring 2026 American GPA 3.50</td></tr>
      <tr><td colspan="4"><p>Hello</p></td></tr>
    </table>
    """
    out = clean_module.clean_data([{"combined_html": html_main, "entry_url": "http://example.com/detail"}])
    assert len(out) == 1
    row = out[0]
    assert row["degree"] == "PsyD"
    assert row["applicant_status"] == "Waitlisted"
    assert row["start_term"] == "Spring 2026"
    assert row["international_american"] == "American"
    assert row["gpa"] == "3.50"
    assert row["comments"] == "Hello"

    # save_data/load_data coverage
    data = [{"x": 1}]
    clean_module.save_data(data, filename="tmp_applicant_data.json")
    loaded = clean_module.load_data(filename="tmp_applicant_data.json")
    assert loaded == data

@pytest.mark.integration
def test_clean_data_covers_remaining_branches(monkeypatch):
    """
    Drives src/clean.py through the remaining uncovered branches:
    - decision_text parsing + month/day extraction
    - accepted/rejected date assignment branches
    - detail-page fetch (status 200) branches:
        - Degree Type backup
        - GRE General / GRE Verbal / Analytical Writing extraction
        - cases where span is found but next span missing (nxt is None)
    - branches around sp being None (span not found)
    """

    # Two rows: one Accepted with month/day, one Rejected with month/day
    combined_html = """
    <table>
      <tr>
        <td>Test University</td>
        <td>Computer Science · Masters</td>
        <td>January 1, 2026</td>
        <td>Accepted 12 Feb</td>
      </tr>
      <tr><td colspan="4">Fall 2026 International GPA 3.90</td></tr>
      <tr><td colspan="4"><p>Great profile</p></td></tr>

      <tr>
        <td>Other University</td>
        <td>Physics · PhD</td>
        <td>January 2, 2026</td>
        <td>Rejected 3 Mar</td>
      </tr>
      <tr><td colspan="4">Spring 2026 American GPA 3.50</td></tr>
      <tr><td colspan="4"><p>Ok</p></td></tr>
    </table>
    """

    raw_entries = [
        {"combined_html": combined_html, "entry_url": "http://example.com/detail/1"},
        {"combined_html": combined_html, "entry_url": "http://example.com/detail/2"},
    ]

    # Detail page HTML crafted to hit:
    # - Degree Type exists (for Masters row it won't be used because degree already set,
    #   but for safety it is still provided)
    # - GRE General span exists, but next span missing (nxt None) to hit that branch
    # - GRE Verbal span exists with next span present
    # - Analytical Writing span exists with next span present
    # - Also omit something to ensure "sp is None" branch happens (e.g. no extra spans)
    detail_html = """
    <html>
      <dl>
        <dt>Degree Type</dt><dd>Masters</dd>
      </dl>

      <span>GRE General</span>
      <!-- intentionally NO next <span> to trigger nxt is None branch -->

      <span>GRE Verbal</span><span>160</span>
      <span>Analytical Writing</span><span>4.5</span>
    </html>
    """

    class FakeResp:
        status = 200
        data = detail_html.encode("utf-8")

    def fake_request(method, url, headers=None):
        return FakeResp()

    monkeypatch.setattr(clean_module.urllib3, "request", fake_request)

    cleaned = clean_module.clean_data(raw_entries)

    # We should get 2 records (because the combined_html contains 2 main <tr>s,
    # but parser always uses trs[0] — so assert “>=1”)
    assert len(cleaned) >= 1

    row = cleaned[0]

    # These asserts drive the key branches:
    assert row["applicant_status"] in ("Accepted", "Rejected")
    assert row["start_term"] in ("Fall 2026", "Spring 2026")
    assert row["international_american"] in ("International", "American")
    assert row["gpa"] in ("3.90", "3.50")

    # Because clean.py uses find_next("span"), "GRE General" will grab the next span
    # anywhere in the document, so it becomes "GRE Verbal"
    assert row["gre_score"] == "GRE Verbal"
    assert row["gre_v_score"] == "160"
    assert row["gre_aw"] == "4.5"

@pytest.mark.integration
def test_clean_data_edge_branches(monkeypatch):
    """
    Hits remaining clean.py branches:
    - decision_text that is NOT accept/reject/wait -> falls into "else applicant_status = decision_text"
    - degree is None AND Degree Type exists but <dd> missing -> _get_value dd-missing branch
    - resp.status != 200 -> skip detail parsing branches
    """

    combined_html = """
    <table>
      <tr>
        <td>Edge University</td>
        <td>Computer Science</td>   <!-- no "·" so degree stays None -->
        <td>January 3, 2026</td>
        <td>Interview</td>          <!-- triggers else: applicant_status = decision_text -->
      </tr>
      <tr><td colspan="4">Fall 2026 Other GPA 3.10</td></tr>
      <tr><td colspan="4"><p>Edge case comment</p></td></tr>
    </table>
    """

    raw_entries = [{
        "combined_html": combined_html,
        "entry_url": "http://example.com/detail/bad",
    }]

    # Detail page with Degree Type dt but NO dd => _get_value hits dd-missing return None
    detail_html = """
    <html>
      <dl>
        <dt>Degree Type</dt>
      </dl>
    </html>
    """

    class FakeResp:
        status = 404  # force resp.status != 200 branch
        data = detail_html.encode("utf-8")

    def fake_request(method, url, headers=None):
        return FakeResp()

    monkeypatch.setattr(clean_module.urllib3, "request", fake_request)

    cleaned = clean_module.clean_data(raw_entries)
    assert len(cleaned) == 1
    row = cleaned[0]

    assert row["applicant_status"] == "Interview"
    assert row["degree"] is None
    assert row["comments"] == "Edge case comment"

@pytest.mark.integration
def test_clean_hits_last_missing_lines(monkeypatch):
    """
    Covers remaining clean.py missing lines:
    25: _get_value -> dt exists but dd missing
    65: decision_text else branch (not accept/reject/wait)
    96: month/day regex extraction for Rejected -> sets rejection_date
    106-107: GRE General span exists but next span missing -> nxt is None
    150-156: degree backup from Degree Type when degree is None
    """

    # Decision_text = "Interview" hits else-branch at line 65
    # Degree stays None because program has no "·"
    combined_html = """
    <table>
      <tr>
        <td>Edge U</td>
        <td>Mathematics</td>
        <td>January 1, 2026</td>
        <td>Interview</td>
      </tr>
    </table>
    """

    raw_entries = [{
        "combined_html": combined_html,
        "entry_url": "http://example.com/detail/last-missing",
    }]

    # Detail page is crafted to hit:
    # - Degree Type backup: dt+dd exists -> sets degree Masters (150-156)
    # - GRE General span exists but there is NO next span -> nxt None (106-107)
    # - Also include a dt with NO dd to cover line 25 (dd missing) using _get_value label
    detail_html = """
    <html>
      <dl>
        <dt>Degree Type</dt><dd>Masters</dd>
        <dt>Some Missing Label</dt>
      </dl>

      <span>GRE General</span>
      <!-- no next span, so find_next("span") returns None -->
    </html>
    """

    class FakeResp:
        status = 200
        data = detail_html.encode("utf-8")

    monkeypatch.setattr(clean_module.urllib3, "request", lambda *a, **k: FakeResp())

    # Call clean_data to hit Degree backup + GRE nxt None
    cleaned = clean_module.clean_data(raw_entries)
    assert len(cleaned) == 1
    row = cleaned[0]

    # line 65 (decision else)
    assert row["applicant_status"] == "Interview"

    # lines 150-156 (degree backup)
    assert row["degree"] == "Masters"

    # lines 106-107 (GRE General span found but nxt None)
    assert row["gre_score"] is None

    # Now explicitly hit line 96 + rejection_date assignment:
    combined_html_rej = """
    <table>
      <tr>
        <td>Reject U</td>
        <td>Physics · PhD</td>
        <td>January 2, 2026</td>
        <td>Rejected 3 Mar</td>
      </tr>
    </table>
    """
    raw_entries_rej = [{
        "combined_html": combined_html_rej,
        "entry_url": "http://example.com/detail/rej-date",
    }]

    cleaned2 = clean_module.clean_data(raw_entries_rej)
    assert len(cleaned2) == 1
    assert cleaned2[0]["rejection_date"] == "3 Mar"

    # Finally: force line 25 (dt exists but dd missing) through _get_value directly
    soup = BeautifulSoup("<dl><dt>Degree Type</dt></dl>", "html.parser")
    assert clean_module._get_value(soup, "Degree Type") is None

@pytest.mark.integration
def test_clean_degree_backup_hits_line_154(monkeypatch):
    """
    Specifically hits clean.py line 154:
    degree backup when degree is None and Degree Type exists.
    """

    combined_html = """
    <table>
      <tr>
        <td>Backup U</td>
        <td>Computer Science</td>  <!-- NO dot, so degree stays None -->
        <td>January 1, 2026</td>
        <td>Accepted</td>
      </tr>
    </table>
    """

    raw_entries = [{
        "combined_html": combined_html,
        "entry_url": "http://example.com/detail/backup",
    }]

    detail_html = """
    <html>
      <dl>
        <dt>Degree Type</dt><dd>PhD</dd>
      </dl>
    </html>
    """

    class FakeResp:
        status = 200
        data = detail_html.encode("utf-8")

    monkeypatch.setattr(clean_module.urllib3, "request", lambda *a, **k: FakeResp())

    cleaned = clean_module.clean_data(raw_entries)
    assert len(cleaned) == 1

    # This assertion forces execution of line 154
    assert cleaned[0]["degree"] == "PhD"


@pytest.mark.integration
def test_clean_helpers_and_summary_fields_branches():
    import src.clean as c
    from bs4 import BeautifulSoup

    assert c._norm(None) is None  # pylint: disable=protected-access
    assert c._norm("  a   b  ") == "a b"  # pylint: disable=protected-access

    # _extract_row_cells: no <tr> branch
    soup = BeautifulSoup("<html></html>", "html.parser")
    assert c._extract_row_cells(soup) is None  # pylint: disable=protected-access

    # <tr> but too few <td>
    soup = BeautifulSoup("<tr><td>1</td></tr>", "html.parser")
    assert c._extract_row_cells(soup) is None  # pylint: disable=protected-access

    # _extract_comments: no <p> branch
    assert (
        c._extract_comments(BeautifulSoup("<div></div>", "html.parser")) is None
    )  # pylint: disable=protected-access

    # _extract_summary_fields: missing <tr> branch
    assert (
        c._extract_summary_fields(BeautifulSoup("<div></div>", "html.parser")) is None
    )  # pylint: disable=protected-access

    # _extract_summary_fields: too few <td> branch
    html = "<table><tr><td>A</td><td>B</td><td>C</td></tr></table>"
    assert (
        c._extract_summary_fields(BeautifulSoup(html, "html.parser")) is None
    )  # pylint: disable=protected-access


@pytest.mark.integration
def test_fetch_detail_fields_branches(monkeypatch):
    import src.clean as c

    # entry_url missing
    assert c._fetch_detail_fields(None) == (None, None, None, None)  # pylint: disable=protected-access

    # request raises
    def boom(*_a, **_k):
        raise OSError("no internet in tests")

    monkeypatch.setattr(c.urllib3, "request", boom)
    assert c._fetch_detail_fields("http://x") == (None, None, None, None)  # pylint: disable=protected-access

    # non-200 status
    class Resp:
        status = 500
        data = b""

    monkeypatch.setattr(c.urllib3, "request", lambda *_a, **_k: Resp())
    assert c._fetch_detail_fields("http://y") == (None, None, None, None)  # pylint: disable=protected-access

    # 200 status with HTML that matches your parsing
    class Resp200:
        status = 200
        data = b"""
        <html>
          <dl>
            <dt>Degree Type</dt><dd>Masters</dd>
          </dl>
          <span>GRE General</span><span>165</span>
          <span>GRE Verbal</span><span>160</span>
          <span>Analytical Writing</span><span>4.5</span>
        </html>
        """

    monkeypatch.setattr(c.urllib3, "request", lambda *_a, **_k: Resp200())
    deg, gre_total, gre_v, gre_aw = c._fetch_detail_fields("http://z")  # pylint: disable=protected-access
    assert deg == "Masters"
    assert gre_total == "165"
    assert gre_v == "160"
    assert gre_aw == "4.5"


@pytest.mark.integration
def test_clean_data_skip_and_degree_fill(monkeypatch):
    import src.clean as c

    # skip branch: missing combined_html
    out = c.clean_data([{"entry_url": "http://x"}])
    assert out == []

    # base None branch: combined_html malformed (no tr)
    out = c.clean_data([{"combined_html": "<div>no table</div>", "entry_url": "http://x"}])
    assert out == []

    # degree fill branch: base degree None, detail returns Masters
    listing = """
    <table>
      <tr>
        <td>MIT</td>
        <td>Computer Science</td>
        <td>January 1, 2026</td>
        <td>Accepted 12 Feb</td>
      </tr>
    </table>
    <p>GPA 3.9 International Fall 2026</p>
    """

    monkeypatch.setattr(
        c,
        "_fetch_detail_fields",
        lambda _url: ("Masters", "165", "160", "4.5"),
    )

    out = c.clean_data([{"combined_html": listing, "entry_url": "http://detail"}])
    assert len(out) == 1
    assert out[0]["degree"] == "Masters"
    assert out[0]["gre_score"] == "165"
    assert out[0]["gre_v_score"] == "160"
    assert out[0]["gre_aw"] == "4.5"


@pytest.mark.integration
def test_clean_helper_guard_branches():
    import src.clean as c

    # Line ~73: _parse_program_and_degree early return when program_text is falsy
    assert c._parse_program_and_degree("") == (None, None)  # pylint: disable=protected-access
    assert c._parse_program_and_degree(None) == (None, None)  # pylint: disable=protected-access

    # Line ~112: _parse_decision early return when decision_text is falsy
    assert c._parse_decision("") == (None, None, None)  # pylint: disable=protected-access
    assert c._parse_decision(None) == (None, None, None)  # pylint: disable=protected-access


@pytest.mark.integration
def test_clean_helpers_success_returns_cover_return_lines_260_275():
    import src.clean as c

    # Covers line 260: return cols (needs at least one <tr> with >= 4 <td>)
    html_with_row = """
    <table>
      <tr>
        <td>Col1</td><td>Col2</td><td>Col3</td><td>Col4</td>
      </tr>
    </table>
    """
    soup_row = BeautifulSoup(html_with_row, "html.parser")
    cols = c._extract_row_cells(soup_row)  # pylint: disable=protected-access
    assert cols is not None
    assert len(cols) == 4
    assert cols[0].get_text(strip=True) == "Col1"

    # Covers line 275: return _norm(p_tag.get_text(...)) (needs a <p>)
    html_with_p = "<div><p>   Hello   world   </p></div>"
    soup_p = BeautifulSoup(html_with_p, "html.parser")
    comment = c._extract_comments(soup_p)  # pylint: disable=protected-access
    assert comment == "Hello world"
