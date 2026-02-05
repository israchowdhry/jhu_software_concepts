from flask import Flask, render_template, redirect, url_for
import threading
import json
import query_data
from scrape import scrape_data
from clean import clean_data
from load_data import load_data


app = Flask(__name__)

STATE_LOCK = threading.Lock()
PULL_STATE = {"running": False, "message": ""}

JSONL_PATH = "llm_extend_applicant_data.jsonl"

def write_jsonl(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def build_results():
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
        {"question": "How many entries have applied for Fall 2026?", "answer": f"Applicant count: {q1}"},
        {"question": "What percentage are International (not American/Other)?", "answer": f"Percent International: {q2}%"},
        {"question": "What is the average GPA, GRE, GRE V, GRE AW of applicants who provided these metrics?",
         "answer": f"Avg GPA: {q3[0]}, Avg GRE: {q3[1]}, Avg GRE V: {q3[2]}, Avg GRE AW: {q3[3]}"},
        {"question": "What is the average GPA of American students in Fall 2026?", "answer": f"Avg GPA American: {q4}"},
        {"question": "What percent of Fall 2026 entries are Acceptances?", "answer": f"Acceptance percent: {q5}%"},
        {"question": "What is the average GPA of Fall 2026 applicants who are Acceptances?", "answer": f"Avg GPA Acceptances: {q6}"},
        {"question": "How many entries are from applicants who applied to JHU for a masters in Computer Science?", "answer": f"Count: {q7}"},
        {"question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU PhD in CS?", "answer": f"Count: {q8}"},
        {"question": "How many 2026 acceptances are for GU/MIT/Stanford/CMU PhD in CS using LLM Generated fields?", "answer": f"Count using LLM fields: {q9}"},
        {"question": query_data.EXTRA_1_QUESTION, "answer": f"{extra1}"},
        {"question": query_data.EXTRA_2_QUESTION, "answer": f"{extra2}"},
    ]


def _background_pull():
    try:
        # scrape raw rows
        raw_rows = scrape_data()

        # clean them using clean.py
        cleaned_rows = clean_data(raw_rows)

        # write cleaned JSONL
        write_jsonl(cleaned_rows, JSONL_PATH)

        # load JSONL into Postgres
        load_data(JSONL_PATH)

        with STATE_LOCK:
            PULL_STATE["message"] = "Pull complete! New data (if any) has been added to the database."
    except Exception as e:
        with STATE_LOCK:
            PULL_STATE["message"] = f"Pull failed: {e}"
    finally:
        with STATE_LOCK:
            PULL_STATE["running"] = False

@app.route("/")
def index():
    with STATE_LOCK:
        running = PULL_STATE["running"]
        message = PULL_STATE["message"]

    return render_template("index.html", results=build_results(), pull_running=running, message=message)


@app.route("/pull-data", methods=["POST"])
def pull_data():
    with STATE_LOCK:
        if PULL_STATE["running"]:
            PULL_STATE["message"] = "Pull Data is already running. Please wait."
            return redirect(url_for("index"))

        PULL_STATE["running"] = True
        PULL_STATE["message"] = "Pulling new data... please wait (running in background)."

    threading.Thread(target=_background_pull, daemon=True).start()
    return redirect(url_for("index"))


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    with STATE_LOCK:
        if PULL_STATE["running"]:
            PULL_STATE["message"] = "Cannot update analysis while Pull Data is running."
            return redirect(url_for("index"))

        PULL_STATE["message"] = "Analysis updated."

    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
