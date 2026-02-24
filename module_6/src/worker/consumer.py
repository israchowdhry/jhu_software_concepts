"""
RabbitMQ consumer for background tasks.

Consumes tasks from a durable queue with backpressure (prefetch=1),
routes by message "kind", and commits DB work before ack.

Task kinds:
- scrape_new_data
- recompute_analytics
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Callable, Iterable

import pika
import psycopg
from pika.adapters.blocking_connection import BlockingChannel
from psycopg import sql

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"

LOGGER = logging.getLogger(__name__)


def _declare_amqp(channel: BlockingChannel) -> None:
    """Declare durable AMQP entities (idempotent)."""
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)


def _database_url() -> str:
    """Read DATABASE_URL from the environment."""
    return os.environ["DATABASE_URL"]


def _seed_json_path() -> str:
    """Get the mounted JSONL path in the worker container."""
    return os.getenv("SEED_JSON", "/data/llm_extend_applicant_data.jsonl")


def _target_table() -> str:
    """Read TARGET_TABLE from the environment (defaults to applicants)."""
    return os.getenv("TARGET_TABLE", "applicants")


def _id_key() -> str:
    """Read ID_KEY from the environment (defaults to url)."""
    return os.getenv("ID_KEY", "url")


def _ensure_watermark_table(conn: psycopg.Connection) -> None:
    """
    Ensure the ingestion watermark table exists.

    Requirement:
    CREATE TABLE IF NOT EXISTS ingestion_watermarks (
        source TEXT PRIMARY KEY,
        last_seen TEXT,
        updated_at TIMESTAMPTZ DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS ingestion_watermarks (
                    source TEXT PRIMARY KEY,
                    last_seen TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
        )


def _read_watermark(conn: psycopg.Connection, source: str) -> str | None:
    """Read the last_seen watermark for a source."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT last_seen FROM ingestion_watermarks WHERE source = %s;"),
            (source,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _write_watermark(conn: psycopg.Connection, source: str, last_seen: str) -> None:
    """Upsert the watermark value after successful inserts."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO ingestion_watermarks (source, last_seen)
                VALUES (%s, %s)
                ON CONFLICT (source)
                DO UPDATE SET last_seen = EXCLUDED.last_seen, updated_at = now();
                """
            ),
            (source, last_seen),
        )


def _parse_date_added(date_str: str | None) -> datetime | None:
    """
    Parse the JSON 'date_added' string like "March 11, 2024".

    Returns None if missing/unparseable.
    """
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%B %d, %Y")
    except ValueError:
        return None


def _normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize raw JSON record to applicants schema fields.

    This matches your load_data schema:
      program, comments, date_added, url, status, term, us_or_international,
      gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
    """
    program = f"{raw.get('university')} - {raw.get('program_name')}".strip()

    llm_prog = raw.get("llm-generated-program") or raw.get("llm_generated_program")
    llm_univ = raw.get("llm-generated-university") or raw.get("llm_generated_university")

    return {
        "program": program,
        "comments": raw.get("comments"),
        "date_added": raw.get("date_added"),
        "url": raw.get("entry_url"),
        "status": raw.get("applicant_status"),
        "term": raw.get("start_term"),
        "us_or_international": raw.get("international_american"),
        "gpa": raw.get("gpa"),
        "gre": raw.get("gre_score"),
        "gre_v": raw.get("gre_v_score"),
        "gre_aw": raw.get("gre_aw"),
        "degree": raw.get("degree"),
        "llm_generated_program": llm_prog,
        "llm_generated_university": llm_univ,
    }


def _iter_jsonl(path: str) -> Iterable[dict[str, Any]]:
    """Yield dict rows from a JSONL file (skips blank lines)."""
    with open(path, "r", encoding="utf-8") as file_handle:
        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _filter_newer_than_watermark(
    rows: Iterable[dict[str, Any]],
    since: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Filter records newer than watermark.

    Watermark is stored as TEXT. We treat it as a date string in the same
    format as 'date_added' ("Month DD, YYYY"). If since is None, everything passes.

    Returns: (filtered_rows, max_seen_string)
    """
    since_dt = _parse_date_added(since)
    filtered: list[dict[str, Any]] = []
    max_dt: datetime | None = since_dt
    max_seen_str: str | None = since

    for raw in rows:
        row_dt = _parse_date_added(str(raw.get("date_added") or ""))
        if since_dt is not None and row_dt is not None and row_dt <= since_dt:
            continue

        filtered.append(raw)

        if row_dt is not None and (max_dt is None or row_dt > max_dt):
            max_dt = row_dt
            max_seen_str = raw.get("date_added")

    return filtered, max_seen_str


def _insert_applicants_batch(
    conn: psycopg.Connection,
    normalized_rows: list[dict[str, Any]],
) -> None:
    """
    Batch insert with parameterized SQL and idempotence.

    Requirement: ON CONFLICT (ID_KEY) DO NOTHING (or equivalent).
    """
    if not normalized_rows:
        return

    table = _target_table()
    id_key = _id_key()

    insert_stmt = sql.SQL(
        """
        INSERT INTO {table_ident} (
            program, comments, date_added, {id_key_ident}, status,
            term, us_or_international, gpa,
            gre, gre_v, gre_aw, degree,
            llm_generated_program, llm_generated_university
        )
        VALUES (
            %s,
            %s,
            TO_DATE(NULLIF(%s, ''), 'Month DD, YYYY'),
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT ({id_key_ident}) DO NOTHING;
        """
    ).format(
        table_ident=sql.Identifier(table),
        id_key_ident=sql.Identifier(id_key),
    )

    values = [
        (
            row["program"],
            row["comments"],
            row["date_added"],
            row["url"],
            row["status"],
            row["term"],
            row["us_or_international"],
            row["gpa"],
            row["gre"],
            row["gre_v"],
            row["gre_aw"],
            row["degree"],
            row["llm_generated_program"],
            row["llm_generated_university"],
        )
        for row in normalized_rows
    ]

    with conn.cursor() as cur:
        cur.executemany(insert_stmt, values)


def handle_scrape_new_data(conn: psycopg.Connection, payload: dict[str, Any]) -> None:
    """
    Handle scrape_new_data.

    Requirements implemented:
    - Reads last_seen at start (or uses payload["since"] if provided)
    - Fetches only records newer than watermark (here: from mounted JSONL)
    - Normalizes to applicants schema
    - Batch inserts with parameterized SQL
    - Idempotent inserts (ON CONFLICT (ID_KEY) DO NOTHING)
    - Advances watermark to max seen after successful inserts
    """
    _ensure_watermark_table(conn)
    source = "gradcafe"

    since_value = payload.get("since")
    if since_value is None:
        since_value = _read_watermark(conn, source)

    seed_path = _seed_json_path()
    raw_rows = list(_iter_jsonl(seed_path))
    newer_rows, max_seen = _filter_newer_than_watermark(raw_rows, since_value)

    normalized = [_normalize_row(r) for r in newer_rows]
    _insert_applicants_batch(conn, normalized)

    # Only advance watermark if we actually saw something newer.
    if max_seen is not None and max_seen != since_value:
        _write_watermark(conn, source, str(max_seen))
        LOGGER.info("Watermark advanced: source=%s last_seen=%s", source, max_seen)
    else:
        LOGGER.info("No newer rows found; watermark unchanged: source=%s last_seen=%s", source, since_value)

    LOGGER.info("scrape_new_data completed (seed=%s inserted=%d)", seed_path, len(normalized))


def handle_recompute_analytics(conn: psycopg.Connection, _payload: dict[str, Any]) -> None:
    """
    Handle recompute_analytics.

    Requirement:
    - Recompute summaries/materialized views used by UI within the same per-message transaction.
    Here we run ANALYZE as a safe default.
    """
    with conn.cursor() as cur:
        cur.execute(sql.SQL("ANALYZE applicants;"))
    LOGGER.info("recompute_analytics completed (ANALYZE applicants)")


def _task_map() -> dict[str, Callable[[psycopg.Connection, dict[str, Any]], None]]:
    """Map task kinds to handler functions."""
    return {
        "scrape_new_data": handle_scrape_new_data,
        "recompute_analytics": handle_recompute_analytics,
    }


def _parse_message(body: bytes) -> tuple[str, dict[str, Any]]:
    """Parse message JSON and return (kind, payload)."""
    decoded = body.decode("utf-8")
    msg = json.loads(decoded)

    kind = msg.get("kind")
    if not isinstance(kind, str) or not kind:
        raise ValueError("Message missing valid 'kind'")

    payload = msg.get("payload", {})
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError("'payload' must be a dict")

    return kind, payload


def _on_message(
    channel: BlockingChannel,
    method: pika.spec.Basic.Deliver,
    _properties: pika.spec.BasicProperties,
    body: bytes,
) -> None:
    """
    RabbitMQ callback:
    - parse JSON
    - open a DB transaction per message (commit on success, rollback on error)
    - ack only after commit
    - on failure: rollback and nack(requeue=False)
    """
    delivery_tag = method.delivery_tag

    try:
        kind, payload = _parse_message(body)

        handler = _task_map().get(kind)
        if handler is None:
            raise ValueError(f"Unknown task kind: {kind}")

        # Per-message transaction: commits on success, rollbacks on exception.
        with psycopg.connect(_database_url()) as conn:
            handler(conn, payload)

        channel.basic_ack(delivery_tag=delivery_tag)
        LOGGER.info("ACK kind=%s", kind)

    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, KeyError) as exc:
        LOGGER.exception("Bad message; NACK requeue=false: %s", exc)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

    except (psycopg.Error, pika.exceptions.AMQPError, OSError, RuntimeError) as exc:
        LOGGER.exception("Processing failure; NACK requeue=false: %s", exc)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)


def main() -> None:
    """
    Run the worker consume loop.

    Requirements:
    - long-running process connects using RABBITMQ_URL
    - declares durable entities (exchange/queue/bind)
    - basic_qos(prefetch_count=1)
    - consumes from tasks_q
    """
    logging.basicConfig(level=logging.INFO)

    rabbitmq_url = os.environ["RABBITMQ_URL"]
    params = pika.URLParameters(rabbitmq_url)

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    _declare_amqp(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=_on_message, auto_ack=False)

    LOGGER.info("Worker started. Consuming from queue=%s prefetch=1", QUEUE)
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
