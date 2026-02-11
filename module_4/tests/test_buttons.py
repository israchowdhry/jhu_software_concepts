import pytest
from src import app as app_module


@pytest.mark.buttons
def test_post_pull_data_returns_200_and_triggers_loader(client, reset_db, monkeypatch):
    """
    When not busy:
    POST /pull-data returns 200 and {"ok": true}
    scrape_data > clean_data > load_data are called
    """

    calls = {"scrape": 0, "clean": 0, "load": 0}

    # Fake scrape_data(): returns raw rows
    def fake_scrape():
        calls["scrape"] += 1
        return [{"entry_url": "http://example.com/1"}]

    # Fake clean_data(raw_rows): returns cleaned rows
    def fake_clean(raw_rows):
        calls["clean"] += 1
        # return a “cleaned” row
        return [{"entry_url": "http://example.com/1"}]

    # Fake load_data(jsonl_path): pretend it inserted rows
    def fake_load(path):
        calls["load"] += 1
        return None

    # Patch the functions used by _background_pull()
    monkeypatch.setattr(app_module, "scrape_data", fake_scrape)
    monkeypatch.setattr(app_module, "clean_data", fake_clean)
    monkeypatch.setattr(app_module, "load_data", fake_load)

    resp = client.post("/pull-data")

    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # Because threading is forced inline by conftest.py,
    # the background pull runs immediately and these should be called once.
    assert calls["scrape"] == 1
    assert calls["clean"] == 1
    assert calls["load"] == 1


@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(client, monkeypatch):
    """
    When not busy:
    POST /update-analysis returns 200 and {"ok": true}
    It recomputes results (we fake build_results so no DB dependency)
    """

    app_module.PULL_STATE["running"] = False

    # Track whether build_results() was called
    called = {"build": 0}

    def fake_build_results():
        called["build"] += 1
        return [{"question": "Q", "answer": "A"}]

    monkeypatch.setattr(app_module, "build_results", fake_build_results)

    resp = client.post("/update-analysis")

    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}
    assert called["build"] == 1


@pytest.mark.buttons
def test_busy_gating_update_analysis_returns_409_and_no_update(client, monkeypatch):
    """
    When busy:
    POST /update-analysis returns 409 and {"busy": true}
    It must NOT recompute analysis (build_results not called)
    """

    # Force busy state
    app_module.PULL_STATE["running"] = True

    def should_not_run():
        raise AssertionError("build_results should NOT run when busy")

    monkeypatch.setattr(app_module, "build_results", should_not_run)

    resp = client.post("/update-analysis")

    assert resp.status_code == 409
    assert resp.get_json() == {"busy": True}


@pytest.mark.buttons
def test_busy_gating_pull_data_returns_409_and_does_not_start_pull(client, monkeypatch):
    """
    When busy:
    POST /pull-data returns 409 and {"busy": true}
    It must NOT start a background pull
    """

    # Force busy state
    app_module.PULL_STATE["running"] = True

    def should_not_start(*args, **kwargs):
        raise AssertionError("Should not start pull thread when busy")

    # If /pull-data route tries to start a thread, this catches it.
    monkeypatch.setattr(app_module.threading, "Thread", should_not_start)

    resp = client.post("/pull-data")

    assert resp.status_code == 409
    assert resp.get_json() == {"busy": True}
