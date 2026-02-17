Overview & Setup
================

What this project does
----------------------
This Grad Café application scrapes GradCafe survey data, cleans and enriches it,
loads it into a PostgreSQL database, and serves analysis results through a Flask web UI.

Project layout
--------------
- src/scrape.py: scrapes listing pages (robots.txt respected)
- src/clean.py: parses HTML and normalizes fields (GPA, term, status, GRE)
- src/load_data.py: loads cleaned JSONL into Postgres (applicants table)
- src/query_data.py: SQL query helpers used by the web UI
- src/app.py: Flask routes + background ETL trigger

Requirements
------------
Install dependencies:

.. code-block:: bash

   pip install -r requirements.txt

Required environment variables
------------------------------
DATABASE_URL
  PostgreSQL connection string used by ``load_data.py`` and ``query_data.py``.

Example (PowerShell):

.. code-block:: powershell

   $env:DATABASE_URL="postgresql://postgres:postgres@localhost:5432/testdb"

Run the web app
---------------
From the repo root:

.. code-block:: bash

   python -m src.app

Then open http://localhost:8080

Run the ETL manually
--------------------
.. code-block:: bash

   python -m src.scrape
   python -m src.clean
   python -m src.load_data

Run tests
---------
.. code-block:: bash

   pytest -q
