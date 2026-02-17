"""
Database loading utilities for the Grad Cafe dataset.

This module reads cleaned applicant records from a JSONL file
and inserts them into a PostgreSQL database. If the applicants
table does not already exist, it will be created automatically.

The function is designed to work with the ``DATABASE_URL``
environment variable (required for CI environments).
"""

import json
import os

import psycopg
from psycopg import Connection
from psycopg.cursor import Cursor


def load_data(jsonl_path, db_url=None):
    """
    Load applicant records from a JSONL file into the database.

    This function:
    - Connects to PostgreSQL using ``DATABASE_URL``.
    - Creates the ``applicants`` table if it does not exist.
    - Inserts each JSONL record into the table.
    - Skips duplicate entries based on unique URL.
    - Prints the total number of stored applicants.

    :param jsonl_path: Path to the JSONL file containing applicant records.
    :type jsonl_path: str
    :param db_url: Optional database connection URL. If not provided,
                   the function uses the ``DATABASE_URL`` environment variable.
    :type db_url: str or None
    :raises RuntimeError: If no database URL is available.
    :return: None
    :rtype: None
    """

    # Use DATABASE_URL (required for CI)
    db_url = db_url or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    # Connect to database
    with psycopg.connect(db_url) as connection: # type: Connection
        with connection.cursor() as cur: # type: Cursor
            # Create table if it does not already exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS applicants (
                    p_id SERIAL PRIMARY KEY,
                    program TEXT,
                    comments TEXT,
                    date_added DATE,
                    url TEXT UNIQUE,
                    status TEXT,
                    term TEXT,
                    us_or_international TEXT,
                    gpa FLOAT,
                    gre FLOAT,
                    gre_v FLOAT,
                    gre_aw FLOAT,
                    degree TEXT,
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
            """)
            # Read JSONL and insert rows
            with open(jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue  # Ignore blank lines

                    row = json.loads(line)

                    cur.execute("""
                        INSERT INTO applicants (
                            program, comments, date_added, url, status,
                            term, us_or_international, gpa,
                            gre, gre_v, gre_aw, degree,
                            llm_generated_program, llm_generated_university
                        )
                        VALUES (%s,%s,TO_DATE(NULLIF(%s, ''), 'Month DD, YYYY'),
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (url) DO NOTHING;
                    """, (
                        f"{row.get('university')} - {row.get('program_name')}",
                        row.get("comments"),
                        row.get("date_added"),
                        row.get("entry_url"),
                        row.get("applicant_status"),
                        row.get("start_term"),
                        row.get("international_american"),
                        row.get("gpa"),
                        row.get("gre_score"),
                        row.get("gre_v_score"),
                        row.get("gre_aw"),
                        row.get("degree"),
                        row.get("llm-generated-program"),
                        row.get("llm-generated-university"),
                    ))

            connection.commit()

            # Confirm number of applicants
            cur.execute("SELECT COUNT(*) FROM applicants;")
            print("Total rows in applicants:", cur.fetchone()[0])

if __name__ == "__main__":
    # Run the loader directly using the default JSONL file.
    load_data("llm_extend_applicant_data.jsonl")
