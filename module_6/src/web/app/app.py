"""
Flask web application for Grad Cafe analytics.

Web tier responsibilities (Module 6):
- Display analytics results
- Enqueue background tasks via RabbitMQ (no long-running work in request thread)
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template

import pika

from src.web.publisher import publish_task
from src.worker.etl import query_data

BASE_DIR = Path(__file__).resolve().parent

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)

STATE_LOCK = threading.Lock()
PULL_STATE: dict[str, Any] = {"running": False, "message": ""}

# Tests expect these names at module level
RESULTS_CACHE: list[dict[str, str]] = []
HAS_RESULTS: bool = False

# App-attached state (avoid globals for mutable objects)
app.results_cache: list[dict[str, str]] = []
app.has_results: bool = False


def create_app() -> Flask:
    """Application factory used by tests and run.py."""
    return app


def build_results() -> list[dict[str, str]]:
    """Run DB queries and format into dashboard output."""
    q1 = query_data.q1()
    q2 = query_data.q2()
    q3 = query_data.q3()
    q4 = query_data.q4()
    q5 = query_data.q5()
    q6 = query_data.q6()
    q7 = query_data.q7()
    q8 = query_data.q8()
    q9 = query_data.q9()
    extra1 = query_data.extra_1()
    extra2 = query_data.extra_2()

    q3_answer = (
        f"Avg GPA: {q3[0]}, Avg GRE: {q3[1]}, "
        f"Avg GRE V: {q3[2]}, Avg GRE AW: {q3[3]}"
    )

    return [
        {"question": "How many entries have applied for Fall 2026?", "answer": f"Applicant count: {q1}"},
        {"question": "What percentage are International (not American/Other)?", "answer": f"Percent International: {q2:.2f}%"},
        {
            "question": "What is the average GPA, GRE, GRE V, GRE AW of applicants who provided these metrics?",
            "answer": q3_answer,
        },
        {"question": "What is the average GPA of American students in Fall 2026?", "answer": f"Avg GPA American: {q4}"},
        {"question": "What percent of Fall 2026 entries are Acceptances?", "answer": f"Acceptance percent: {q5:.2f}%"},
        {"question": "What is the average GPA of Fall 2026 applicants who are Acceptances?", "answer": f"Avg GPA Acceptances: {q6}"},
        {
            "question": "How many entries are from applicants who applied to JHU for a masters in Computer Science?",
            "answer": f"Count: {q7}",
        },
        {"question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU PhD in CS?", "answer": f"Count: {q8}"},
        {
            "question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU PhD in CS using LLM Generated fields?",
            "answer": f"Count using LLM fields: {q9}",
        },
        {"question": query_data.EXTRA_1_QUESTION, "answer": f"{extra1}"},
        {"question": query_data.EXTRA_2_QUESTION, "answer": f"{extra2}"},
    ]


@app.route("/analysis")
def analysis():
    """Route alias for the homepage analysis view."""
    return index()


@app.route("/")
def index():
    """Render the homepage displaying cached analysis results."""
    with STATE_LOCK:
        if not app.has_results and not PULL_STATE["running"]:
            app.results_cache = build_results()
            app.has_results = True

        results = list(app.results_cache)
        running = bool(PULL_STATE["running"])
        message = str(PULL_STATE["message"])
        has_results = bool(app.has_results)

    return render_template(
        "index.html",
        results=results,
        has_results=has_results,
        pull_running=running,
        message=message,
    )


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """
    Enqueue scrape_new_data.

    Returns:
    - 202 when queued
    - 503 if RabbitMQ is unavailable
    """
    try:
        publish_task("scrape_new_data", payload={})
    except (KeyError, pika.exceptions.AMQPError, OSError, RuntimeError, ValueError) as exc:
        return jsonify({"queued": False, "error": str(exc)}), 503

    with STATE_LOCK:
        PULL_STATE["message"] = "Request queued: scrape_new_data. Refresh in a moment."
    return jsonify({"queued": True}), 202


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """
    Enqueue recompute_analytics.

    Returns:
    - 202 when queued
    - 503 if RabbitMQ is unavailable
    """
    try:
        publish_task("recompute_analytics", payload={})
    except (KeyError, pika.exceptions.AMQPError, OSError, RuntimeError, ValueError) as exc:
        return jsonify({"queued": False, "error": str(exc)}), 503

    with STATE_LOCK:
        PULL_STATE["message"] = "Request queued: recompute_analytics. Refresh in a moment."
        app.has_results = False
    return jsonify({"queued": True}), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
