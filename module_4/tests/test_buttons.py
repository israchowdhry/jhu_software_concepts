import pytest
import src.app as app_module


@pytest.mark.buttons
def test_post_pull_data_returns_200_and_triggers_loader(client, monkeypatch, tmp_path):
    """
    Requirement:
    - POST /pull-data returns 200 and {"ok": true} when not busy
    - Must trigger scrape > clean > load (mocked)
    """

    calls = {"scrape": 0, "clean": 0, "load": 0}

    def fake_scrape():
        calls["scrape"] += 1
        return [{"entry_url": "http://example.com/1"}]

    def fake_clean(raw_rows):
        calls["clean"] += 1
        return [{"entry_url": "http://example.com/1"}]

    def fake_load(path):
        calls["load"] += 1
        return None

    # Avoid writing to a real project file
    app_module.JSONL_PATH = str(tmp_path / "fake.jsonl")

    # Ensure not busy before calling /pull-data
    app_module.PULL_STATE["running"] = False

    monkeypatch.setattr(app_module, "scrape_data", fake_scrape)
    monkeypatch.setattr(app_module, "clean_data", fake_clean)
    monkeypatch.setattr(app_module, "load_data", fake_load)

    resp = client.post("/pull-data")

    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # Thread is forced inline by conftest, background runs immediately
    assert calls["scrape"] == 1
    assert calls["clean"] == 1
    assert calls["load"] == 1


@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(client, monkeypatch):
    """
    Requirement:
    - POST /update-analysis returns 200 and {"ok": true} when not busy
    - Should recompute results (fake build_results to avoid DB dependency)
    """

    app_module.PULL_STATE["running"] = False

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
    Requirement:
    - When pull is in progress:
      POST /update-analysis returns 409 and {"busy": true}
      and does not run build_results()
    """
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
      and must not start a background thread.
    """
    # Force busy state
    app_module.PULL_STATE["running"] = True

    def should_not_start(*args, **kwargs):
        raise AssertionError("Should not start pull thread when busy")

    # If /pull-data tries to spawn a thread, we fail the test
    monkeypatch.setattr(app_module.threading, "Thread", should_not_start)

    resp = client.post("/pull-data")

    assert resp.status_code == 409
    assert resp.get_json() == {"busy": True}

@pytest.mark.buttons
def test_pull_data_failure_sets_error_message(client, monkeypatch, tmp_path):
    """
    Covers app.py exception path in _background_pull():
      except Exception as e -> sets "Pull failed: ..."
      finally > sets running False
    """
    app_module.PULL_STATE["running"] = False
    app_module.JSONL_PATH = str(tmp_path / "rows.jsonl")

    def boom():
        raise RuntimeError("kaboom")

    monkeypatch.setattr(app_module, "scrape_data", boom)

    resp = client.post("/pull-data")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # Thread is inline (via conftest), so message is already set
    assert "Pull failed" in app_module.PULL_STATE["message"]
    assert "kaboom" in app_module.PULL_STATE["message"]
    assert app_module.PULL_STATE["running"] is False
