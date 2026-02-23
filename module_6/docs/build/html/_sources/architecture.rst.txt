Architecture
============

This application is organized into three layers: Web (Flask), ETL, and Database/Query.

Web layer (Flask)
-----------------
Module: ``src/app.py``

Responsibilities:
- Serves the homepage route (``/`` and ``/analysis``) that renders cached analysis results.
- Exposes JSON endpoints used by the UI:
  - ``POST /pull-data`` starts an ETL refresh in a background thread.
  - ``POST /update-analysis`` recomputes and caches query results.
- Uses a shared lock (``STATE_LOCK``) and a shared state dictionary (``PULL_STATE``) to prevent concurrent runs.

ETL layer
---------
Modules: ``src/scrape.py``, ``src/clean.py``, ``src/load_data.py``

Responsibilities:
- ``scrape.py``:
  - Checks robots.txt before scraping.
  - Scrapes GradCafe listing pages and collects combined HTML per entry.
- ``clean.py``:
  - Parses HTML with BeautifulSoup and extracts fields (program, university, term, GPA, status, dates).
  - Optionally fetches entry detail pages if GRE fields are missing.
- ``load_data.py``:
  - Reads cleaned JSONL records and inserts them into Postgres.
  - Creates the ``applicants`` table if it does not exist.
  - Enforces uniqueness on URL via ``ON CONFLICT (url) DO NOTHING``.

Database/query layer
--------------------
Module: ``src/query_data.py``

Responsibilities:
- Provides connection helpers using ``DATABASE_URL``.
- Defines query functions (q1..q9, extra_1, extra_2) used by the web layer to build the analysis output.
