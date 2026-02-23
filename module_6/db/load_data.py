"""
Database loading utilities for the Grad Cafe dataset.

This module reads cleaned applicant records from a JSONL file
and inserts them into a PostgreSQL database. If the applicants
table does not already exist, it will be created automatically.

The function is designed to use DATABASE_URL for connectivity,
which supports CI usage and avoids hard-coded credentials.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg import sql

from .db_config import resolve_db_url

def _resolve_db_url(db_url: str | None = None) -> str:
    """
    Backward-compatible wrapper used by tests.

    Tests call src.load_data._resolve_db_url directly.
    """
    return resolve_db_url(db_url)

CREATE_TABLE_SQL = sql.SQL(
    """
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
"""
)

INSERT_SQL = sql.SQL(
    """
INSERT INTO applicants (
    program, comments, date_added, url, status,
    term, us_or_international, gpa,
    gre, gre_v, gre_aw, degree,
    llm_generated_program, llm_generated_university
)
VALUES (
    %s,
    %s,
    TO_DATE(NULLIF(%s, ''), 'Month DD, YYYY'),
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON CONFLICT (url) DO NOTHING;
"""
)

COUNT_SQL = sql.SQL("SELECT COUNT(*) FROM applicants;")


def load_data(jsonl_path: str, *, db_url: str | None = None) -> None:
    """
    Load applicant records from a JSONL file into the database.

    This function:
    - Connects to PostgreSQL using DATABASE_URL (or db_url override)
    - Creates the ``applicants`` table if it does not exist
    - Inserts each JSONL record into the table
    - Skips duplicate entries based on unique URL
    - Prints the total number of stored applicants

    :param jsonl_path: Path to the JSONL file containing applicant records.
    :type jsonl_path: str
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :raises RuntimeError: If DATABASE_URL is not set and db_url is None.
    :return: None
    :rtype: None
    """
    resolved = resolve_db_url(db_url)

    with psycopg.connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

            with open(jsonl_path, "r", encoding="utf-8") as file_handle:
                for line in file_handle:
                    line = line.strip()
                    if not line:
                        continue

                    row: dict[str, Any] = json.loads(line)

                    program = f"{row.get('university')} - {row.get('program_name')}"

                    llm_prog = (
                        row.get("llm-generated-program")
                        or row.get("llm_generated_program")
                    )

                    llm_univ = (
                        row.get("llm-generated-university")
                        or row.get("llm_generated_university")
                    )

                    cur.execute(
                        INSERT_SQL,
                        (
                            program,
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
                            llm_prog,
                            llm_univ,
                        ),
                    )

            conn.commit()
            cur.execute(COUNT_SQL)
            total = cur.fetchone()[0]
            print("Total rows in applicants:", total)


if __name__ == "__main__":
    load_data("llm_extend_applicant_data.jsonl")
