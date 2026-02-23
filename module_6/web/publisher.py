"""
RabbitMQ publisher for the Grad Cafe web application.

This module provides functions to publish tasks to RabbitMQ
so that long-running or data-modifying work is handled
by the worker service asynchronously.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import pika

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"


def _open_channel():
    """
    Open a RabbitMQ connection and channel.

    Declares a durable direct exchange, a durable queue,
    and binds them with the routing key.

    :return: Tuple of (connection, channel)
    :rtype: tuple
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
    payload: dict | None = None,
    headers: dict | None = None,
) -> None:
    """
    Publish a task message to RabbitMQ.

    Builds a JSON message with the task kind, UTC timestamp,
    and optional payload, then publishes it as a persistent
    message to the tasks exchange.

    :param kind: Task type identifier (e.g., 'scrape_new_data')
    :type kind: str
    :param payload: Optional task payload data
    :type payload: dict or None
    :param headers: Optional AMQP message headers
    :type headers: dict or None
    :raises Exception: If publishing fails
    :return: None
    :rtype: None
    """
    body = json.dumps(
        {
            "kind": kind,
            "ts": datetime.utcnow().isoformat(),
            "payload": payload or {},
        },
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
            ),
            mandatory=False,
        )
    finally:
        conn.close()