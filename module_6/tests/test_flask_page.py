import pytest
from bs4 import BeautifulSoup

import src.web.app.app as app_module
from src.web.app.app import create_app


@pytest.mark.web
def test_get_analysis_200(client):
    """
    Requirement:
    - GET /analysis returns 200
    """
    resp = client.get("/analysis")
    assert resp.status_code == 200


@pytest.mark.web
def test_analysis_page_has_required_buttons_and_answer_label(client, monkeypatch):
    """
    Requirement:
    - Page has Pull Data and Update Analysis buttons
    - Page includes a visible Answer label at least once
    """
    # Ensure there is at least one result so "Answer:" renders in the template
    def fake_build_results():
        return [{"question": "Q1", "answer": "A1"}]

    monkeypatch.setattr(app_module, "build_results", fake_build_results)

    # Force recompute so it uses our fake results
    app_module.app.has_results = False
    app_module.app.results_cache = []

    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data.decode("utf-8"), "html.parser")

    pull_btn = soup.find(attrs={"data-testid": "pull-data-btn"})
    update_btn = soup.find(attrs={"data-testid": "update-analysis-btn"})

    assert pull_btn is not None
    assert update_btn is not None

    page_text = soup.get_text(" ", strip=True)

    # Matches your index.html <h1>
    assert "Grad School Cafe Data Analysis" in page_text

    # This appears only when results render
    assert "Answer:" in page_text


@pytest.mark.web
def test_create_app_has_required_routes():
    """
    Requirement:
    - Ensure required routes exist on the app
    """
    app = create_app()
    rules = {r.rule for r in app.url_map.iter_rules()}

    assert "/" in rules
    assert "/analysis" in rules
    assert "/pull-data" in rules
    assert "/update-analysis" in rules

