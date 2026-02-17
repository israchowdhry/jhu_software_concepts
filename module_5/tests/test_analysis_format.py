import re
import pytest
from bs4 import BeautifulSoup
import src.app as app_module


@pytest.mark.analysis
def test_all_cards_have_answer_label(client, monkeypatch):
    """
    Requirement:
    - Page includes Answer labels for rendered analysis
    - Confirm every rendered card has an Answer block
    """
    def fake_build_results():
        return [
            {"question": "Q1", "answer": "Something"},
            {"question": "Q2", "answer": "Percent International: 39.28%"},
            {"question": "Q3", "answer": "Acceptance percent: 12.00%"},
        ]

    # Force deterministic results and avoid DB calls
    monkeypatch.setattr(app_module, "build_results", fake_build_results)
    app_module.HAS_RESULTS = False

    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data.decode("utf-8"), "html.parser")

    cards = soup.find_all("div", class_="card")
    answers = soup.find_all("p", class_="a")

    assert len(cards) > 0
    assert len(cards) == len(answers)

    for a in answers:
        assert a.get_text(strip=True).startswith("Answer:")


@pytest.mark.analysis
def test_all_percentages_are_two_decimals(client, monkeypatch):
    """
    Requirement:
    - Any percentage on the page must be formatted with exactly two decimals
      Example: 39.28%
    """
    def fake_build_results():
        return [
            {"question": "Q1", "answer": "Percent International: 39.28%"},
            {"question": "Q2", "answer": "Acceptance percent: 12.00%"},
        ]

    monkeypatch.setattr(app_module, "build_results", fake_build_results)
    app_module.HAS_RESULTS = False

    resp = client.get("/analysis")
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    # Find ANY percentages like 10%, 10.0%, 10.00%, etc.
    any_percent_pattern = re.compile(r"\b\d+(?:\.\d+)?%")
    found = any_percent_pattern.findall(html)

    # If there are percentages, they ALL must be X.XX%
    strict_two_decimal = re.compile(r"\b\d+\.\d{2}%")
    valid = strict_two_decimal.findall(html)

    if found:
        assert len(valid) == len(found)
