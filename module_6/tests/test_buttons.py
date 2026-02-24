import pytest
import pika

import src.web.app.app as app_module


@pytest.mark.buttons
def test_post_pull_data_queues_task_and_returns_202(client, monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_publish(kind: str, payload=None, headers=None):
        calls.append((kind, payload or {}))

    monkeypatch.setattr(app_module, "publish_task", fake_publish)

    resp = client.post("/pull-data")

    assert resp.status_code == 202
    assert resp.get_json() == {"queued": True}
    assert calls == [("scrape_new_data", {})]
    assert "queued" in app_module.PULL_STATE["message"].lower()


@pytest.mark.buttons
def test_post_update_analysis_queues_task_sets_has_results_false(client, monkeypatch):
    app_module.app.has_results = True

    calls: list[tuple[str, dict]] = []

    def fake_publish(kind: str, payload=None, headers=None):
        calls.append((kind, payload or {}))

    monkeypatch.setattr(app_module, "publish_task", fake_publish)

    resp = client.post("/update-analysis")

    assert resp.status_code == 202
    assert resp.get_json() == {"queued": True}
    assert calls == [("recompute_analytics", {})]
    assert app_module.app.has_results is False
    assert "queued" in app_module.PULL_STATE["message"].lower()


@pytest.mark.buttons
def test_pull_data_returns_503_when_rabbitmq_unavailable(client, monkeypatch):
    def boom(*_a, **_k):
        raise pika.exceptions.AMQPError("no broker")

    monkeypatch.setattr(app_module, "publish_task", boom)

    resp = client.post("/pull-data")

    assert resp.status_code == 503
    body = resp.get_json()
    assert body["queued"] is False
    assert "no broker" in body["error"]


@pytest.mark.buttons
def test_update_analysis_returns_503_when_rabbitmq_unavailable(client, monkeypatch):
    def boom(*_a, **_k):
        raise OSError("connection refused")

    monkeypatch.setattr(app_module, "publish_task", boom)

    resp = client.post("/update-analysis")

    assert resp.status_code == 503
    body = resp.get_json()
    assert body["queued"] is False
    assert "connection refused" in body["error"]
