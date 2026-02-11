import pytest
from bs4 import BeautifulSoup
from src.app import create_app

@pytest.mark.web
def test_get_analysis_200(client, reset_db):
    """
    Ensure that the /analysis endpoint exists
    and returns a successful HTTP response.
    """
    resp = client.get("/analysis")
    assert resp.status_code == 200

@pytest.mark.web
def test_analysis_page_has_required_buttons(client, reset_db):
    """
    Ensure the analysis page renders:
    - Pull Data button
    - Update Analysis button
    - At least one "Answer:" label
    """

    # Request the page
    resp = client.get("/analysis")
    # Parse returned HTML
    soup = BeautifulSoup(resp.data.decode("utf-8"), "html.parser")

    # Locate buttons using stable data-testid selectors
    pull_btn = soup.find(attrs={"data-testid": "pull-data-btn"})
    update_btn = soup.find(attrs={"data-testid": "update-analysis-btn"})

    # Ensure both buttons exist
    assert pull_btn is not None
    assert update_btn is not None

    # Ensure at least one analysis answer is rendered
    assert "Answer:" in soup.get_text()

# Test app factory registers required routes
@pytest.mark.web
def test_create_app_has_required_routes():
    """
    Ensure create_app() returns a Flask app
    that contains all required routes.
    """

    # Create a fresh app instance using the factory
    app = create_app()

    # Extract all required route paths
    rules = {r.rule for r in app.url_map.iter_rules()}

    # Verify required endpoints exist
    assert "/analysis" in rules
    assert "/pull-data" in rules
    assert "/update-analysis" in rules
    assert "/" in rules




