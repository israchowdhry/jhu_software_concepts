SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (iachowdh6)

Module Info: Module 3 Assignment: Database Queries Assignment Experiment Due on 02/08/2026 at 11:59 EST

Approach:
This assignment implements a full data pipeline and analysis workflow using PostgreSQL, SQL, and Flask from previous scraped Grad School Cafe data.

load_data.py:
This script loads cleaned applicant data from jsonl file into a PostgreSQL database using psycopg(version 3). This script establishes a database connection, creates a single relational table named applicants if it does not already exist, and inserts each jsonl record as a row in the table.

The applicants table includes structured fields such as program name, admission status, applicantion term, nationality, GPA, GRE scores, degree type, llm_generated program and university fields. The url column is defined as unique and inserts use ON CONFLICT(url) DO NOTHING to prevent duplicate entries when the script is run multiple times.

The date_added column is stored using the SQL date type, and string dates from the jsonl file were converted using PostgreSQL TO_DATE function. After all the rows were processed the script commits the transaction and prints the total number of rows in the table as a confirmation (a sql query was used to achieve this.)

query_data.py:
The code description to this script can be found in the screenshots PDF file.

app.py:
The app.py file implements a Flask-based web application that displays SQL analysis results dynamically. The application queries the PostgreSQL database using functions defined in query_data.py and renders the results on a single html page.

def write_jsonl:
Saves cleaned applicant records into jsonl file. This is used because load_data.py expects a jsonl file to insert rows into postgresql.

def build_results:
This is to run all SQL analysis queries and packages them into a list (of dictionaries) that the template can display. This helps keep the Flask route clean.

def _background_pull:
Runs the pipeline scrape, clean, write, and load data. This section updates message to success or failure. It always sets Pull_state['running'] = False in finally so the app unlocks even if something does fail. Flask can serve multiple requests while the thread is running. The lock ensures state changes to Pull_state happen safely without race conditions.

def index:
This page shows the cached results stored in results_cache. It also shows whether pull data is running and the current status message. When the page is visited for the first time after the server starts, it generate analysis once so the page isn't empty. The results are copied in this so it can make a shallow copy so the HTML render uses a stable snapshot.

def pull_data: Checks pull_state["running"]. If it is already running then show message and refuse to start another pull. Otherwise start the background thread and show message update. Redirect is used because after POST requests, redirecting accidental double submissions if the user refreshes.

def update_analysis: If pull data is running: blocks and tells the user. Otherwise, run build_results (the fresh SQL queries), store the new results in results_cache and set has_results to true. Also updates the message to "Analysis updated." Running queries can take time so this runs queries outside the lock because holding the lock would block other routes from reading state.

To support the “Pull Data” requirement, the application runs scraping, cleaning, and database loading operations in a background thread using Python’s threading module. This prevents the Flask server from freezing while long-running scraping tasks are executed. A shared application state dictionary (PULL_STATE) protected by a threading lock is used to track whether a pull operation is currently running and to display a user-friendly status messages.

Analysis results are cached in memory and are only recomputed when the user clicks the “Update Analysis” button, ensuring that results do not change automatically after new data is pulled. This is to align with the assignment requirement that analysis updates occur explicitly at the user’s request.

The application provides two interactive buttons:

Pull Data: Scrapes new Grad School Cafe entries, cleans them, writes them to a JSONL file, and loads them into PostgreSQL.

Update Analysis: Re-runs all SQL queries and refreshes the displayed results if no pull operation is currently running.

NOTES: ChatGPT was used for index.html and style.css as encouraged from lecture to optimize productivity.

Known bugs: No known bugs