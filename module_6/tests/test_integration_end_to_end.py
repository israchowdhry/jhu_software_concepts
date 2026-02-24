import json
import runpy
from types import SimpleNamespace

import psycopg
import pytest
import flask

import src.web.app.app as app_module


@pytest.mark.integration
def test_end_to_end_seed_db_then_render_analysis_page(client, reset_db, db_url, tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", db_url)

    from src.db.load_data import load_data

    p = tmp_path / "seed.jsonl"
    rows = [
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
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    load_data(str(p), db_url=db_url)

    app_module.app.has_results = False
    r = client.get("/analysis")
    assert r.status_code == 200
    html = r.data.decode("utf-8")
    assert "Answer:" in html

    import re
    any_percent = re.findall(r"\b\d+(?:\.\d+)?%", html)
    strict = re.findall(r"\b\d+\.\d{2}%", html)
    if any_percent:
        assert len(any_percent) == len(strict)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            assert cur.fetchone()[0] == 2


@pytest.mark.integration
def test_run_py_main_calls_flask_run(monkeypatch):
    called = {"n": 0}

    def fake_run(self, *args, **kwargs):
        called["n"] += 1

    monkeypatch.setattr(flask.app.Flask, "run", fake_run)
    runpy.run_module("src.web.run", run_name="__main__")
    assert called["n"] == 1


@pytest.mark.integration
def test_app_dunder_main_calls_flask_run(monkeypatch):
    called = {"n": 0}

    def fake_run(self, *args, **kwargs):
        called["n"] += 1

    monkeypatch.setattr(flask.app.Flask, "run", fake_run)
    runpy.run_module("src.web.app.app", run_name="__main__")
    assert called["n"] == 1


@pytest.mark.integration
def test_publisher_publish_task_happy_path(monkeypatch):
    import src.web.publisher as publisher

    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    published = {"count": 0}

    class FakeChannel:
        def exchange_declare(self, **_k): return None

        def queue_declare(self, **_k): return None

        def queue_bind(self, **_k): return None

        def basic_publish(self, **_k): published["count"] += 1

    class FakeConn:
        def __init__(self, *_a, **_k): self._ch = FakeChannel()

        def channel(self): return self._ch

        def close(self): return None

    monkeypatch.setattr(publisher.pika, "BlockingConnection", lambda *_a, **_k: FakeConn())
    monkeypatch.setattr(publisher.pika, "URLParameters", lambda *_a, **_k: object())

    publisher.publish_task("scrape_new_data", payload={"x": 1})
    assert published["count"] == 1


@pytest.mark.integration
def test_consumer_on_message_ack_and_nack_paths(monkeypatch):
    import src.worker.consumer as consumer

    events: list[tuple[str, int, bool | None]] = []

    class FakeChannel:
        def basic_ack(self, *, delivery_tag):
            events.append(("ack", delivery_tag, None))

        def basic_nack(self, *, delivery_tag, requeue):
            events.append(("nack", delivery_tag, requeue))

    ch = FakeChannel()
    method = SimpleNamespace(delivery_tag=7)

    def handler(_conn, _payload): return None

    monkeypatch.setattr(consumer, "_task_map", lambda: {"scrape_new_data": handler})

    class FakeConn:
        def __enter__(self): return object()

        def __exit__(self, exc_type, exc, tb): return False

    monkeypatch.setattr(consumer.psycopg, "connect", lambda *_a, **_k: FakeConn())
    monkeypatch.setattr(consumer, "_database_url", lambda: "postgresql://x")

    good_body = json.dumps({"kind": "scrape_new_data", "payload": {}}).encode("utf-8")
    consumer._on_message(ch, method, None, good_body)

    bad_body = b"not-json"
    consumer._on_message(ch, SimpleNamespace(delivery_tag=8), None, bad_body)

    unknown_body = json.dumps({"kind": "nope", "payload": {}}).encode("utf-8")
    consumer._on_message(ch, SimpleNamespace(delivery_tag=9), None, unknown_body)

    assert ("ack", 7, None) in events
    assert ("nack", 8, False) in events
    assert ("nack", 9, False) in events


@pytest.mark.integration
def test_consumer_watermark_filtering_and_insert_noop(monkeypatch):
    import src.worker.consumer as consumer

    assert consumer._parse_date_added(None) is None
    assert consumer._parse_date_added("not-a-date") is None

    rows = [
        {"date_added": "January 1, 2026"},
        {"date_added": "January 2, 2026"},
    ]
    filtered, max_seen = consumer._filter_newer_than_watermark(rows, "January 1, 2026")
    assert len(filtered) == 1
    assert max_seen == "January 2, 2026"

    class FakeConn:
        def cursor(self):
            raise AssertionError("cursor should not be used for empty batch")

    consumer._insert_applicants_batch(FakeConn(), [])


import src.worker.consumer as consumer


@pytest.mark.integration
def test_consumer_parse_message_and_errors():
    kind, payload = consumer._parse_message(b'{"kind":"scrape_new_data","payload":{"a":1}}')
    assert kind == "scrape_new_data"
    assert payload == {"a": 1}

    kind2, payload2 = consumer._parse_message(b'{"kind":"scrape_new_data","payload":null}')
    assert payload2 == {}

    kind3, payload3 = consumer._parse_message(b'{"kind":"scrape_new_data"}')
    assert payload3 == {}

    with pytest.raises(ValueError):
        consumer._parse_message(b'{"payload":{}}')

    with pytest.raises(ValueError):
        consumer._parse_message(b'{"kind":"","payload":{}}')

    with pytest.raises(ValueError):
        consumer._parse_message(b'{"kind":123,"payload":{}}')

    with pytest.raises(ValueError):
        consumer._parse_message(b'{"kind":"x","payload":"not-a-dict"}')


@pytest.mark.integration
def test_consumer_on_message_ack_and_nack_paths(monkeypatch):
    events = []

    class FakeChannel:
        def basic_ack(self, *, delivery_tag):
            events.append(("ack", delivery_tag))
        def basic_nack(self, *, delivery_tag, requeue):
            events.append(("nack", delivery_tag, requeue))

    ch = FakeChannel()

    # success path
    monkeypatch.setattr(consumer, "_task_map", lambda: {"scrape_new_data": lambda _c, _p: None})
    monkeypatch.setattr(consumer, "_database_url", lambda: "postgresql://x")

    class FakeConn:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False

    import psycopg
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: FakeConn())

    consumer._on_message(ch, SimpleNamespace(delivery_tag=1), None, b'{"kind":"scrape_new_data","payload":{}}')
    assert ("ack", 1) in events

    # bad json -> nack
    consumer._on_message(ch, SimpleNamespace(delivery_tag=2), None, b"not-json")
    assert ("nack", 2, False) in events

    # unknown kind -> nack
    monkeypatch.setattr(consumer, "_task_map", lambda: {})
    consumer._on_message(ch, SimpleNamespace(delivery_tag=3), None, b'{"kind":"nope","payload":{}}')
    assert ("nack", 3, False) in events

    # processing failure -> nack
    def boom(_c, _p):
        raise RuntimeError("fail")

    monkeypatch.setattr(consumer, "_task_map", lambda: {"scrape_new_data": boom})
    consumer._on_message(ch, SimpleNamespace(delivery_tag=4), None, b'{"kind":"scrape_new_data","payload":{}}')
    assert ("nack", 4, False) in events


@pytest.mark.integration
def test_consumer_main_sets_up_and_closes(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

    calls = []

    class FakeChannel:
        def exchange_declare(self, **kwargs): calls.append(("exchange_declare", kwargs))
        def queue_declare(self, **kwargs): calls.append(("queue_declare", kwargs))
        def queue_bind(self, **kwargs): calls.append(("queue_bind", kwargs))
        def basic_qos(self, **kwargs): calls.append(("basic_qos", kwargs))
        def basic_consume(self, **kwargs): calls.append(("basic_consume", kwargs))
        def start_consuming(self): calls.append(("start_consuming", {}))

    fake_channel = FakeChannel()

    class FakeConn:
        def __init__(self): self.closed = False
        def channel(self): return fake_channel
        def close(self): self.closed = True

    fake_conn = FakeConn()

    import pika
    monkeypatch.setattr(pika, "URLParameters", lambda *_a, **_k: object())
    monkeypatch.setattr(pika, "BlockingConnection", lambda *_a, **_k: fake_conn)

    consumer.main()

    names = [c[0] for c in calls]
    assert "exchange_declare" in names
    assert "queue_declare" in names
    assert "queue_bind" in names
    assert ("basic_qos", {"prefetch_count": 1}) in calls
    assert "basic_consume" in names
    assert ("start_consuming", {}) in calls
    assert fake_conn.closed is True


@pytest.mark.integration
def test_consumer_dunder_main(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

    calls = []

    class FakeChannel:
        def exchange_declare(self, **kwargs): calls.append(("exchange_declare", kwargs))
        def queue_declare(self, **kwargs): calls.append(("queue_declare", kwargs))
        def queue_bind(self, **kwargs): calls.append(("queue_bind", kwargs))
        def basic_qos(self, **kwargs): calls.append(("basic_qos", kwargs))
        def basic_consume(self, **kwargs): calls.append(("basic_consume", kwargs))
        def start_consuming(self): calls.append(("start_consuming", {}))

    fake_channel = FakeChannel()

    class FakeConn:
        def channel(self): return fake_channel
        def close(self): return None

    import pika
    monkeypatch.setattr(pika, "URLParameters", lambda *_a, **_k: object())
    monkeypatch.setattr(pika, "BlockingConnection", lambda *_a, **_k: FakeConn())

    runpy.run_module("src.worker.consumer", run_name="__main__")

    assert ("start_consuming", {}) in calls


@pytest.mark.integration
def test_consumer_on_message_covers_all_paths(monkeypatch):
    events = []

    class FakeChannel:
        def basic_ack(self, *, delivery_tag):
            events.append(("ack", delivery_tag))

        def basic_nack(self, *, delivery_tag, requeue):
            events.append(("nack", delivery_tag, requeue))

    ch = FakeChannel()

    # success: parse -> handler -> psycopg.connect -> ack
    monkeypatch.setattr(consumer, "_database_url", lambda: "postgresql://x")
    monkeypatch.setattr(consumer, "_task_map", lambda: {"scrape_new_data": lambda _c, _p: None})

    class FakeConn:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False

    import psycopg
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: FakeConn())

    consumer._on_message(ch, SimpleNamespace(delivery_tag=1), None, b'{"kind":"scrape_new_data","payload":{}}')
    assert ("ack", 1) in events

    # bad json -> first except -> nack
    consumer._on_message(ch, SimpleNamespace(delivery_tag=2), None, b"not-json")
    assert ("nack", 2, False) in events

    # unknown kind -> ValueError -> first except -> nack
    monkeypatch.setattr(consumer, "_task_map", lambda: {})
    consumer._on_message(ch, SimpleNamespace(delivery_tag=3), None, b'{"kind":"nope","payload":{}}')
    assert ("nack", 3, False) in events

    # processing failure -> second except -> nack
    def boom(_c, _p):
        raise RuntimeError("fail")

    monkeypatch.setattr(consumer, "_task_map", lambda: {"scrape_new_data": boom})
    consumer._on_message(ch, SimpleNamespace(delivery_tag=4), None, b'{"kind":"scrape_new_data","payload":{}}')
    assert ("nack", 4, False) in events


@pytest.mark.integration
def test_consumer_main_covers_consume_setup_and_close(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

    calls = []

    class FakeChannel:
        def exchange_declare(self, **kwargs): calls.append(("exchange_declare", kwargs))
        def queue_declare(self, **kwargs): calls.append(("queue_declare", kwargs))
        def queue_bind(self, **kwargs): calls.append(("queue_bind", kwargs))
        def basic_qos(self, **kwargs): calls.append(("basic_qos", kwargs))
        def basic_consume(self, **kwargs): calls.append(("basic_consume", kwargs))
        def start_consuming(self): calls.append(("start_consuming", {}))

    fake_channel = FakeChannel()

    class FakeConn:
        def __init__(self): self.closed = False
        def channel(self): return fake_channel
        def close(self): self.closed = True

    fake_conn = FakeConn()

    import pika
    monkeypatch.setattr(pika, "URLParameters", lambda *_a, **_k: object())
    monkeypatch.setattr(pika, "BlockingConnection", lambda *_a, **_k: fake_conn)

    consumer.main()

    names = [c[0] for c in calls]
    assert "exchange_declare" in names
    assert "queue_declare" in names
    assert "queue_bind" in names
    assert ("basic_qos", {"prefetch_count": 1}) in calls
    assert "basic_consume" in names
    assert ("start_consuming", {}) in calls
    assert fake_conn.closed is True


@pytest.mark.integration
def test_consumer_handle_scrape_new_data_full_flow_advances_watermark(monkeypatch):
    # Fake DB cursor + conn that supports everything called by handlers
    class Cur:
        def __init__(self):
            self.executes = 0
            self.executemany_calls = 0
            self._fetchone = ("March 10, 2024",)  # existing watermark

        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): self.executes += 1
        def executemany(self, *_a, **_k): self.executemany_calls += 1
        def fetchone(self): return self._fetchone

    cur = Cur()

    class Conn:
        def cursor(self): return cur

    # Make JSONL contain a newer record so watermark advances
    monkeypatch.setattr(
        consumer,
        "_iter_jsonl",
        lambda _p: [
            {
                "date_added": "March 11, 2024",
                "university": "U",
                "program_name": "P",
                "entry_url": "http://new",
                "comments": "c",
                "applicant_status": "Accepted",
                "start_term": "Fall 2025",
                "international_american": "International",
            }
        ],
    )
    monkeypatch.setattr(consumer, "_seed_json_path", lambda: "/tmp/seed.jsonl")

    # Ensure env reads don't break
    monkeypatch.setenv("TARGET_TABLE", "applicants")
    monkeypatch.setenv("ID_KEY", "url")

    consumer.handle_scrape_new_data(Conn(), payload={})

    # Assertions: we executed SQL + batch insert + watermark upsert
    # - _ensure_watermark_table -> execute
    # - _read_watermark -> execute + fetchone
    # - _insert_applicants_batch -> executemany
    # - _write_watermark -> execute
    assert cur.executes >= 2
    assert cur.executemany_calls == 1


@pytest.mark.integration
def test_consumer_handle_scrape_new_data_hits_no_newer_rows_else_branch(monkeypatch):
    """
    Covers the else branch:
      LOGGER.info("No newer rows found; watermark unchanged ...")
    by making max_seen == since_value.
    """

    class Cur:
        def __init__(self):
            self.executes = 0
            self.executemany_calls = 0
            self._fetchone = ("March 11, 2024",)  # existing watermark == row date

        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def execute(self, *_a, **_k): self.executes += 1
        def executemany(self, *_a, **_k): self.executemany_calls += 1
        def fetchone(self): return self._fetchone

    cur = Cur()

    class Conn:
        def cursor(self): return cur

    # JSONL contains ONLY rows <= watermark, so newer_rows empty and max_seen stays == since_value
    monkeypatch.setattr(
        consumer,
        "_iter_jsonl",
        lambda _p: [
            {
                "date_added": "March 11, 2024",
                "university": "U",
                "program_name": "P",
                "entry_url": "http://same",
                "comments": "c",
                "applicant_status": "Accepted",
                "start_term": "Fall 2025",
                "international_american": "International",
            }
        ],
    )
    monkeypatch.setattr(consumer, "_seed_json_path", lambda: "/tmp/seed.jsonl")

    # Keep env reads happy
    monkeypatch.setenv("TARGET_TABLE", "applicants")
    monkeypatch.setenv("ID_KEY", "url")

    # Run
    consumer.handle_scrape_new_data(Conn(), payload={})

    # Should NOT insert anything because no newer rows
    assert cur.executemany_calls == 0
    # But should have executed table create + watermark select at least
    assert cur.executes >= 2


@pytest.mark.integration
def test_consumer_task_map_is_returned():
    task_map = consumer._task_map()
    assert isinstance(task_map, dict)
    assert task_map["scrape_new_data"] is consumer.handle_scrape_new_data
    assert task_map["recompute_analytics"] is consumer.handle_recompute_analytics