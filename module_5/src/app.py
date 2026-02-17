"""
Flask web application for Grad Cafe analytics.

This module provides the web interface for:

- Triggering the ETL pipeline (scrape → clean → load)
- Running database analytics queries
- Displaying results in a web dashboard
- Managing background tasks safely using threading

It integrates the scraping, cleaning, and database layers
into a single interactive application.
"""

import threading
import json

from flask import Flask, render_template, jsonify
from . import query_data
from .scrape import scrape_data
from .clean import clean_data
from .load_data import load_data


app = Flask(__name__)

# Store shared state in app.config instead of module globals
app.config["RESULTS_CACHE"] = []
app.config["HAS_RESULTS"] = False

def create_app():
    """
    Create and return the Flask application instance.

    This function enables application factory usage for testing.

    :return: Flask application instance.
    :rtype: flask.Flask
    """
    return app


# Shared state and thread safety
STATE_LOCK = threading.Lock()

# Tracks if Pull Data is running and displays appropriate message
PULL_STATE = {"running": False, "message": ""}

JSONL_PATH = "llm_extend_applicant_data.jsonl"


def write_jsonl(rows, path):
    """
    Write cleaned applicant records to a JSONL file.

    Each record is written as a separate JSON line.

    :param rows: List of cleaned applicant dictionaries.
    :type rows: list[dict]
    :param path: Output file path.
    :type path: str
    :return: None
    :rtype: None
    """
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def build_results():
    """
    Execute all analytics queries and construct dashboard results.

    This function runs database queries defined in ``query_data``
    and formats them into question/answer pairs for display
    in the web interface.

    :return: List of formatted analysis result dictionaries.
    :rtype: list[dict]
    """
    # Run all sql queries
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

    return [
        {"question": "How many entries have applied for Fall 2026?",
         "answer": f"Applicant count: {q1}"},
        {"question": "What percentage are International (not American/Other)?",
         "answer": f"Percent International: {q2}%"},
        {"question": "What is the average GPA, GRE, GRE V, GRE AW of "
                     "applicants who provided these metrics?",
         "answer": f"Avg GPA: {q3[0]}, Avg GRE: {q3[1]}, Avg GRE V: "
                   f"{q3[2]}, Avg GRE AW: {q3[3]}"},
        {"question": "What is the average GPA of American students in "
                     "Fall 2026?", "answer": f"Avg GPA American: {q4}"},
        {"question": "What percent of Fall 2026 entries are Acceptances?",
         "answer": f"Acceptance percent: {q5}%"},
        {"question": "What is the average GPA of Fall 2026 applicants"
                     "who are Acceptances?",
         "answer": f"Avg GPA Acceptances: {q6}"},
        {"question": "How many entries are from applicants who applied to "
                     "JHU for a masters in Computer Science?",
         "answer": f"Count: {q7}"},
        {"question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU"
                     "PhD in CS?", "answer": f"Count: {q8}"},
        {"question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU "
                     "PhD in CS using LLM Generated fields?", "answer":
            f"Count using LLM fields: {q9}"},
        {"question": query_data.EXTRA_1_QUESTION, "answer": f"{extra1}"},
        {"question": query_data.EXTRA_2_QUESTION, "answer": f"{extra2}"},
    ]


def _background_pull():
    """
    Execute the full ETL pipeline in a background thread.

    This function:
    - Scrapes new Grad Cafe entries
    - Cleans the raw HTML data
    - Writes cleaned JSONL
    - Loads data into PostgreSQL
    - Updates shared application state safely

    Thread locking ensures safe state updates.
    """
    # Runs scrape, clean, and load in the background
    with STATE_LOCK:
        PULL_STATE["running"] = True
        PULL_STATE["message"] = "Pulling new data... please wait."

    try:
        raw_rows = scrape_data()
        cleaned_rows = clean_data(raw_rows)
        write_jsonl(cleaned_rows, JSONL_PATH)
        load_data(JSONL_PATH)

        with STATE_LOCK:
            PULL_STATE["message"] = (
                "Pull complete! Click 'Update Analysis' to refresh results."
            )
    except (RuntimeError, OSError, ValueError) as exc:
        with STATE_LOCK:
            PULL_STATE["message"] = f"Pull failed: {exc}"
    finally:
        with STATE_LOCK:
            PULL_STATE["running"] = False


@app.route("/analysis")
def analysis():
    """
    Route alias for the homepage analysis view.

    :return: Rendered index page.
    :rtype: flask.Response
    """
    return index()


@app.route("/")
def index():
    """
    Render the homepage displaying cached analysis results.

    If results are not cached and no background job is running,
    results are generated automatically.

    :return: Rendered HTML page.
    :rtype: flask.Response
    """
    with STATE_LOCK:
        if not app.config["HAS_RESULTS"] and not PULL_STATE["running"]:
            app.config["RESULTS_CACHE"] = build_results()
            app.config["HAS_RESULTS"] = True

        results = app.config["RESULTS_CACHE"][:]
        running = PULL_STATE["running"]
        message = PULL_STATE["message"]
        has_results = app.config["HAS_RESULTS"]

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
    Trigger the background ETL pipeline.

    Returns HTTP 409 if a job is already running.

    :return: JSON response indicating success or busy status.
    :rtype: flask.Response
    """
    with STATE_LOCK:
        if PULL_STATE["running"]:
            return jsonify({"busy": True}), 409

    threading.Thread(target=_background_pull, daemon=True).start()
    return jsonify({"ok": True}), 200


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """
    Recompute analysis results from the database.

    Returns HTTP 409 if background ETL is currently running.

    :return: JSON response indicating success or busy status.
    :rtype: flask.Response
    """
    with STATE_LOCK:
        if PULL_STATE["running"]:
            return jsonify({"busy": True}), 409

    new_results = build_results()

    with STATE_LOCK:
        app.config["RESULTS_CACHE"] = new_results
        app.config["HAS_RESULTS"] = True
        PULL_STATE["message"] = "Analysis updated."

    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    # Run the Flask development server (development only)
    app.run(host="0.0.0.0", port=8080)
