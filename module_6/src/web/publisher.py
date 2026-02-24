"""
RabbitMQ publisher for the web service.

This module provides a small API used by Flask routes to enqueue tasks
to a durable RabbitMQ exchange/queue.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"


def _open_channel() -> tuple[pika.BlockingConnection, BlockingChannel]:
    """
    Open a connection/channel to RabbitMQ and declare durable entities.

    :return: (connection, channel)
    :raises KeyError: if RABBITMQ_URL is not set
    :raises pika.exceptions.AMQPError: for RabbitMQ failures
    """
    url = os.environ["RABBITMQ_URL"]
    params = pika.URLParameters(url)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    return conn, ch


def publish_task(
    kind: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
    """
    Publish a task message to the durable tasks exchange/queue.

    Message body is compact JSON with keys:
    - kind
    - ts (UTC ISO timestamp)
    - payload (dict)
    """
    body = json.dumps(
        {"kind": kind, "ts": datetime.now(timezone.utc).isoformat(), "payload": payload or {}},
        separators=(",", ":"),
    ).encode("utf-8")

    conn, ch = _open_channel()
    try:
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers=headers or {},
                content_type="application/json",
            ),
            mandatory=False,
        )
    finally:
        conn.close()
