"""
Database loading utilities for the Grad Cafe dataset.

Reads cleaned applicant records from a JSONL file and inserts them into PostgreSQL.
Creates required tables if missing.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg import sql

from src.db.db_config import resolve_db_url

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

CREATE_WATERMARK_SQL = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS ingestion_watermarks (
        source TEXT PRIMARY KEY,
        last_seen TEXT,
        updated_at TIMESTAMPTZ DEFAULT now()
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

    :param jsonl_path: Path to the JSONL file containing applicant records.
    :param db_url: Optional DB URL override.
    :raises RuntimeError: If DATABASE_URL is not set and db_url is None.
    """
    resolved = resolve_db_url(db_url)

    with psycopg.connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(CREATE_WATERMARK_SQL)

            with open(jsonl_path, "r", encoding="utf-8") as file_handle:
                for line in file_handle:
                    stripped = line.strip()
                    if not stripped:
                        continue

                    row: dict[str, Any] = json.loads(stripped)
                    program = f"{row.get('university')} - {row.get('program_name')}"

                    llm_prog = row.get("llm-generated-program") or row.get("llm_generated_program")
                    llm_univ = row.get("llm-generated-university") or row.get("llm_generated_university")

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
            total_row = cur.fetchone()
            total = total_row[0] if total_row is not None else 0
            print("Total rows in applicants:", total)


if __name__ == "__main__":
    load_data("llm_extend_applicant_data.jsonl")
