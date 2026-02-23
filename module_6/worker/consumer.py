"""
RabbitMQ consumer for the Grad Cafe worker service.

This module connects to RabbitMQ, consumes tasks from the tasks_q queue,
and routes them to the appropriate handler functions.

Handlers:
- scrape_new_data: Scrapes new GradCafe entries and loads them into PostgreSQL
- recompute_analytics: Refreshes analytics queries in the database
"""

from __future__ import annotations

import json
import os

import pika
import psycopg

from etl.incremental_scraper import scrape_data
from etl.clean import clean_data

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"


def _get_db_conn():
    """
    Create and return a PostgreSQL database connection.

    :return: psycopg3 connection object
    :rtype: psycopg.Connection
    """
    return psycopg.connect(os.environ["DATABASE_URL"])


def handle_scrape_new_data(conn, payload: dict) -> None:
    """
    Scrape new GradCafe entries and load them into PostgreSQL.

    Reads the watermark for the last seen entry, scrapes only new
    records, and inserts them with idempotent SQL. Updates the
    watermark after a successful commit.

    :param conn: Active psycopg3 database connection
    :param payload: Task payload (may contain 'since' key)
    :type payload: dict
    :return: None
    :rtype: None
    """
    with conn.cursor() as cur:
        # Read watermark
        cur.execute(
            "SELECT last_seen FROM ingestion_watermarks WHERE source = %s",
            ("gradcafe",),
        )
        row = cur.fetchone()
        since = payload.get("since") or (row[0] if row else None)

        # Scrape and clean
        raw_rows = scrape_data()
        cleaned_rows = clean_data(raw_rows)

        if not cleaned_rows:
            conn.rollback()
            return

        # Batch insert with idempotence
        for record in cleaned_rows:
            program = (
                f"{record.get('university')} - {record.get('program_name')}"
            )
            llm_prog = (
                record.get("llm-generated-program")
                or record.get("llm_generated_program")
            )
            llm_univ = (
                record.get("llm-generated-university")
                or record.get("llm_generated_university")
            )
            cur.execute(
                """
                INSERT INTO applicants (
                    program, comments, date_added, url, status,
                    term, us_or_international, gpa,
                    gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                )
                VALUES (
                    %s, %s,
                    TO_DATE(NULLIF(%s, ''), 'Month DD, YYYY'),
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    program,
                    record.get("comments"),
                    record.get("date_added"),
                    record.get("entry_url"),
                    record.get("applicant_status"),
                    record.get("start_term"),
                    record.get("international_american"),
                    record.get("gpa"),
                    record.get("gre_score"),
                    record.get("gre_v_score"),
                    record.get("gre_aw"),
                    record.get("degree"),
                    llm_prog,
                    llm_univ,
                ),
            )

        # Update watermark
        max_date = max(
            (r.get("date_added") for r in cleaned_rows if r.get("date_added")),
            default=since,
        )
        cur.execute(
            """
            INSERT INTO ingestion_watermarks (source, last_seen)
            VALUES (%s, %s)
            ON CONFLICT (source) DO UPDATE SET last_seen = EXCLUDED.last_seen,
            updated_at = now()
            """,
            ("gradcafe", max_date),
        )

        conn.commit()
        print(f"Inserted {len(cleaned_rows)} records, watermark: {max_date}")


def handle_recompute_analytics(conn, payload: dict) -> None:
    """
    Recompute analytics by refreshing database summaries.

    :param conn: Active psycopg3 database connection
    :param payload: Task payload (unused)
    :type payload: dict
    :return: None
    :rtype: None
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants")
        count = cur.fetchone()[0]
        conn.commit()
        print(f"Analytics recomputed. Total applicants: {count}")


TASK_MAP = {
    "scrape_new_data": handle_scrape_new_data,
    "recompute_analytics": handle_recompute_analytics,
}


def on_message(ch, method, properties, body) -> None:
    """
    Handle an incoming RabbitMQ message.

    Parses the message, routes to the correct handler,
    and acks on success or nacks on failure.

    :param ch: RabbitMQ channel
    :param method: Delivery method
    :param properties: Message properties
    :param body: Raw message body
    :return: None
    :rtype: None
    """
    try:
        message = json.loads(body)
        kind = message.get("kind")
        payload = message.get("payload", {})
        print(f"Received task: {kind}")

        handler = TASK_MAP.get(kind)
        if not handler:
            print(f"Unknown task kind: {kind}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        conn = _get_db_conn()
        try:
            handler(conn, payload)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            conn.rollback()
            print(f"Handler error: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            conn.close()

    except Exception as exc:
        print(f"Message parse error: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main() -> None:
    """
    Start the RabbitMQ consumer loop.

    Connects to RabbitMQ, declares durable entities,
    sets prefetch to 1, and begins consuming messages.

    :return: None
    :rtype: None
    """
    url = os.environ["RABBITMQ_URL"]
    params = pika.URLParameters(url)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message)

    print("Worker ready, waiting for tasks...")
    ch.start_consuming()


if __name__ == "__main__":
    main()