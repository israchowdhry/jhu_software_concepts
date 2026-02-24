import re
import pytest
from bs4 import BeautifulSoup
import src.web.app.app as app_module


@pytest.mark.analysis
def test_all_cards_have_answer_label(client, monkeypatch):
    def fake_build_results():
        return [
            {"question": "Q1", "answer": "Something"},
            {"question": "Q2", "answer": "Percent International: 39.28%"},
            {"question": "Q3", "answer": "Acceptance percent: 12.00%"},
        ]

    monkeypatch.setattr(app_module, "build_results", fake_build_results)
    app_module.app.has_results = False

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
    def fake_build_results():
        return [
            {"question": "Q1", "answer": "Percent International: 39.28%"},
            {"question": "Q2", "answer": "Acceptance percent: 12.00%"},
        ]

    monkeypatch.setattr(app_module, "build_results", fake_build_results)
    app_module.app.has_results = False

    resp = client.get("/analysis")
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    any_percent_pattern = re.compile(r"\b\d+(?:\.\d+)?%")
    found = any_percent_pattern.findall(html)

    strict_two_decimal = re.compile(r"\b\d+\.\d{2}%")
    valid = strict_two_decimal.findall(html)

    if found:
        assert len(valid) == len(found)
