import pytest
from bs4 import BeautifulSoup
from src.app import create_app


@pytest.mark.web
def test_get_analysis_200(client):
    """
    Requirement:
    - GET /analysis returns 200
    """
    resp = client.get("/analysis")
    assert resp.status_code == 200


@pytest.mark.web
def test_analysis_page_has_required_buttons_and_answer_label(client):
    """
    Requirement:
    - Page has Pull Data and Update Analysis buttons
    - Page includes "Analysis" and at least one "Answer:"
    """
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data.decode("utf-8"), "html.parser")

    pull_btn = soup.find(attrs={"data-testid": "pull-data-btn"})
    update_btn = soup.find(attrs={"data-testid": "update-analysis-btn"})

    assert pull_btn is not None
    assert update_btn is not None

    page_text = soup.get_text(" ", strip=True)
    assert "Analysis" in page_text
    assert "Answer:" in page_text


@pytest.mark.web
def test_create_app_has_required_routes():
    """
    Requirement:
    - Test app factory / config
    - Ensure required routes exist on the app
    """
    app = create_app()
    rules = {r.rule for r in app.url_map.iter_rules()}

    assert "/" in rules
    assert "/analysis" in rules
    assert "/pull-data" in rules
    assert "/update-analysis" in rules
