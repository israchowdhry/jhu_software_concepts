"""
Database query utilities for Grad Cafe applicant analytics.

This module provides helper functions for connecting to the PostgreSQL
database and executing analytical queries used in the assignment.

The queries compute statistics such as applicant counts, averages,
acceptance rates, and program-level aggregations.
"""

from __future__ import annotations

from typing import Any, Sequence

import psycopg
from psycopg import sql

from .db_config import resolve_db_url

def _resolve_db_url(db_url: str | None = None) -> str:
    """
    Backward-compatible wrapper used by tests.

    Tests call src.query_data._resolve_db_url directly.
    """
    return resolve_db_url(db_url)


# Enforced query LIMIT bounds (Step 2 requirement)
MIN_LIMIT = 1
MAX_LIMIT = 100
DEFAULT_LIMIT = 25


def clamp_limit(raw_limit: Any, default: int = DEFAULT_LIMIT) -> int:
    """
    Clamp a LIMIT value to a safe integer range.

    :param raw_limit: User-supplied or external limit input.
    :type raw_limit: any
    :param default: Default limit used when input is missing/invalid.
    :type default: int
    :return: A safe LIMIT value within [MIN_LIMIT, MAX_LIMIT].
    :rtype: int
    """
    try:
        value = int(raw_limit)
    except (TypeError, ValueError):
        value = default
    return max(MIN_LIMIT, min(MAX_LIMIT, value))


def get_conn(db_url: str | None = None) -> psycopg.Connection:
    """
    Establish a connection to the PostgreSQL database.

    :param db_url: Optional DB URL override. If not provided, uses DATABASE_URL.
    :type db_url: str | None
    :return: Active PostgreSQL connection object.
    :rtype: psycopg.Connection
    :raises RuntimeError: If DATABASE_URL is not set and db_url is None.
    """
    resolved = resolve_db_url(db_url)
    return psycopg.connect(resolved)


def fetch_one(
    stmt: sql.SQL | sql.Composed,
    params: Sequence[Any] = (),
    *,
    db_url: str | None = None,
) -> Any:
    """
    Execute a SQL query and return a single scalar value.

    :param stmt: SQL statement as a psycopg composed SQL object.
    :type stmt: psycopg.sql.SQL or psycopg.sql.Composed
    :param params: Parameters to safely substitute into the query.
    :type params: Sequence[any]
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: First column of the first row returned by the query (or None).
    :rtype: any
    """
    with get_conn(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params)
            row = cur.fetchone()
            return row[0] if row else None


def fetch_row(
    stmt: sql.SQL | sql.Composed,
    params: Sequence[Any] = (),
    *,
    db_url: str | None = None,
) -> tuple[Any, ...] | None:
    """
    Execute a SQL query and return a single row.

    :param stmt: SQL statement as a psycopg composed SQL object.
    :type stmt: psycopg.sql.SQL or psycopg.sql.Composed
    :param params: Parameters to safely substitute into the query.
    :type params: Sequence[any]
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: First row returned by the query (or None).
    :rtype: tuple or None
    """
    with get_conn(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params)
            return cur.fetchone()


def fetch_all(
    stmt: sql.SQL | sql.Composed,
    params: Sequence[Any] = (),
    *,
    db_url: str | None = None,
) -> list[tuple[Any, ...]]:
    """
    Execute a SQL query and return all rows.

    :param stmt: SQL statement as a psycopg composed SQL object.
    :type stmt: psycopg.sql.SQL or psycopg.sql.Composed
    :param params: Parameters to safely substitute into the query.
    :type params: Sequence[any]
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: All rows returned by the query.
    :rtype: list[tuple]
    """
    with get_conn(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params)
            return cur.fetchall()


# Q1

Q1_SQL = sql.SQL("SELECT COUNT(*) FROM applicants WHERE term = %s LIMIT %s;")


def q1(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> int:
    """
    Return the number of applicants for Fall 2026.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Count of Fall 2026 applicants.
    :rtype: int
    """
    val = fetch_one(Q1_SQL, ("Fall 2026", clamp_limit(limit)), db_url=db_url)
    return int(val or 0)


# Q2

Q2_SQL = sql.SQL(
    """
SELECT ROUND(
    100.0 * SUM(
        CASE WHEN us_or_international NOT IN ('American','Other')
             THEN 1 ELSE 0 END
    ) / NULLIF(COUNT(*), 0),
    2
)
FROM applicants
LIMIT %s;
"""
)


def q2(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> float:
    """
    Return the percentage of international applicants (rounded to 2 decimals).

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Percentage of international applicants.
    :rtype: float
    """
    value = fetch_one(Q2_SQL, (clamp_limit(limit),), db_url=db_url)
    return float(value) if value is not None else 0.0


# Q3

Q3_SQL = sql.SQL(
    """
SELECT
  ROUND(AVG(gpa)::numeric, 2) AS avg_gpa,
  ROUND(AVG(gre)::numeric, 2) AS avg_gre_q,
  ROUND(AVG(gre_v)::numeric, 2) AS avg_gre_v,
  ROUND(AVG(gre_aw)::numeric, 2) AS avg_gre_aw
FROM applicants
WHERE (gpa IS NULL OR (gpa BETWEEN 0 AND 4.0))
  AND (gre IS NULL OR (gre BETWEEN 130 AND 170))
  AND (gre_v IS NULL OR (gre_v BETWEEN 130 AND 170))
  AND (gre_aw IS NULL OR (gre_aw BETWEEN 0 AND 6))
LIMIT %s;
"""
)


def q3(
    limit: Any = DEFAULT_LIMIT,
    *,
    db_url: str | None = None,
) -> tuple[Any, Any, Any, Any]:
    """
    Return average GPA and GRE scores (rounded to 2 decimals).

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Tuple containing (avg_gpa, avg_gre_q, avg_gre_v, avg_gre_aw).
    :rtype: tuple[float, float, float, float]
    """
    row = fetch_row(Q3_SQL, (clamp_limit(limit),), db_url=db_url)
    if not row:
        return (None, None, None, None)
    return row[0], row[1], row[2], row[3]


# Q4

Q4_SQL = sql.SQL(
    """
SELECT ROUND(AVG(gpa)::numeric, 2)
FROM applicants
WHERE term = %s
  AND us_or_international = %s
  AND gpa IS NOT NULL
LIMIT %s;
"""
)


def q4(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> float:
    """
    Return the average GPA of American students for Fall 2026.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Average GPA.
    :rtype: float
    """
    value = fetch_one(
        Q4_SQL,
        ("Fall 2026", "American", clamp_limit(limit)),
        db_url=db_url,
    )
    return float(value) if value is not None else 0.0


# Q5

Q5_SQL = sql.SQL(
    """
SELECT ROUND(
    100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
)
FROM applicants
WHERE term = %s
LIMIT %s;
"""
)


def q5(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> float:
    """
    Return percentage of Fall 2026 applicants who were accepted.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Acceptance percentage.
    :rtype: float
    """
    value = fetch_one(Q5_SQL, ("Fall 2026", clamp_limit(limit)), db_url=db_url)
    return float(value) if value is not None else 0.0


# Q6

Q6_SQL = sql.SQL(
    """
SELECT ROUND(AVG(gpa)::numeric, 2)
FROM applicants
WHERE COALESCE(term,'') ILIKE %s
  AND COALESCE(status,'') ILIKE %s
  AND gpa IS NOT NULL
LIMIT %s;
"""
)


def q6(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> float:
    """
    Return the average GPA of Fall 2026 applicants who were accepted.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Average GPA of accepted applicants.
    :rtype: float
    """
    value = fetch_one(
        Q6_SQL,
        ("%Fall 2026%", "%Accepted%", clamp_limit(limit)),
        db_url=db_url,
    )
    return float(value) if value is not None else 0.0


# Q7

Q7_SQL = sql.SQL(
    """
SELECT COUNT(*)
FROM applicants
WHERE degree ILIKE %s
  AND program ILIKE %s
  AND (program ILIKE %s OR program ILIKE %s)
LIMIT %s;
"""
)


def q7(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> int:
    """
    Return the number of entries applying to JHU for a Masters in Computer Science.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Count of JHU Masters CS applicants.
    :rtype: int
    """
    value = fetch_one(
        Q7_SQL,
        (
            "%master%",
            "%computer science%",
            "%johns hopkins%",
            "%jhu%",
            clamp_limit(limit),
        ),
        db_url=db_url,
    )
    return int(value or 0)


# Q8

Q8_SQL = sql.SQL(
    """
SELECT COUNT(*)
FROM applicants
WHERE term ILIKE %s
  AND status = %s
  AND degree ILIKE %s
  AND program ILIKE %s
  AND (
       program ILIKE %s
    OR program ILIKE %s
    OR program ILIKE %s
    OR program ILIKE %s
    OR program ILIKE %s
  )
LIMIT %s;
"""
)


def q8(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> int:
    """
    Return count of 2026 PhD CS acceptances at Georgetown, MIT,
    Stanford, or Carnegie Mellon.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Count of matching applicants.
    :rtype: int
    """
    value = fetch_one(
        Q8_SQL,
        (
            "%2026%",
            "Accepted",
            "%phd%",
            "%computer science%",
            "%georgetown%",
            "%Massachusetts Institute of Technology%",
            "%mit%",
            "%stanford%",
            "%carnegie mellon%",
            clamp_limit(limit),
        ),
        db_url=db_url,
    )
    return int(value or 0)


# Q9

Q9_SQL = sql.SQL(
    """
SELECT COUNT(*)
FROM applicants
WHERE COALESCE(term,'') ILIKE %s
  AND COALESCE(status,'') = %s
  AND COALESCE(degree,'') ILIKE %s
  AND COALESCE(llm_generated_program,'') ILIKE %s
  AND (
       COALESCE(llm_generated_university,'') ILIKE %s
    OR COALESCE(llm_generated_university,'') ILIKE %s
    OR COALESCE(llm_generated_university,'') ILIKE %s
    OR COALESCE(llm_generated_university,'') ILIKE %s
    OR COALESCE(llm_generated_university,'') ILIKE %s
  )
LIMIT %s;
"""
)


def q9(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> int:
    """
    Return count of 2026 PhD CS acceptances using LLM-generated fields.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Count of matching applicants using LLM fields.
    :rtype: int
    """
    value = fetch_one(
        Q9_SQL,
        (
            "%2026%",
            "Accepted",
            "%phd%",
            "%computer science%",
            "%georgetown%",
            "%massachusetts institute of technology%",
            "%mit%",
            "%stanford%",
            "%carnegie mellon%",
            clamp_limit(limit),
        ),
        db_url=db_url,
    )
    return int(value or 0)


# Extra Questions

EXTRA_1_QUESTION = "What are the top 3 most common universities/program names in the dataset?"
EXTRA_1_SQL = sql.SQL(
    """
SELECT program, COUNT(*) AS n
FROM applicants
WHERE program IS NOT NULL AND program <> ''
GROUP BY program
ORDER BY n DESC
LIMIT %s;
"""
)


def extra_1(limit: Any = 3, *, db_url: str | None = None) -> list[tuple[str, int]]:
    """
    Return the most common program names.

    :param limit: Number of programs to return (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: List of (program, count) tuples.
    :rtype: list[tuple[str, int]]
    """
    rows = fetch_all(EXTRA_1_SQL, (clamp_limit(limit, default=3),), db_url=db_url)
    return [(str(program), int(n)) for program, n in rows]


EXTRA_2_QUESTION = "How many people got rejected from JHU?"
EXTRA_2_SQL = sql.SQL(
    """
SELECT COUNT(*)
FROM applicants
WHERE COALESCE(status,'') = %s
  AND (program ILIKE %s OR program ILIKE %s)
LIMIT %s;
"""
)


def extra_2(limit: Any = DEFAULT_LIMIT, *, db_url: str | None = None) -> int:
    """
    Return count of applicants rejected from JHU.

    :param limit: Maximum rows allowed (clamped to 1–100).
    :type limit: any
    :param db_url: Optional DB URL override.
    :type db_url: str | None
    :return: Count of rejected JHU applicants.
    :rtype: int
    """
    value = fetch_one(
        EXTRA_2_SQL,
        ("Rejected", "%johns hopkins%", "%jhu%", clamp_limit(limit)),
        db_url=db_url,
    )
    return int(value or 0)


def main() -> None:
    """
    Execute all analytical queries and print results to the console.

    This function is primarily used for manual testing and demonstration.
    """
    print(f"Q1 (Count Fall 2026): {q1()}")
    print(f"Q2 (% International Applicants): {q2()}%")

    avg_gpa, avg_gre_q, avg_gre_v, avg_gre_aw = q3()
    print("Q3 (Averages of scores):")
    print(f"   avg_gpa   = {avg_gpa}")
    print(f"   avg_gre_q = {avg_gre_q}")
    print(f"   avg_gre_v = {avg_gre_v}")
    print(f"   avg_gre_aw = {avg_gre_aw}")

    print(f"Q4 (Avg GPA American Fall 2026): {q4()}")
    print(f"Q5 (% Acceptances Fall 2026): {q5()}%")
    print(f"Q6 (Avg GPA Fall 2026 Acceptances): {q6()}")
    print(f"Q7 (JHU Masters CS count) {q7()}")
    print(f"Q8 (2026 PhD CS Acceptances at GU/MIT/Stanford/CMU): {q8()}")
    print(f"Q9 (2026 PhD CS Acceptances using LLM fields): {q9()}")

    print(f"Extra Q1: {EXTRA_1_QUESTION}")
    for program, n in extra_1():
        print(f"  {n}  {program}")

    print(f"Extra Q2: {EXTRA_2_QUESTION} {extra_2()}")


if __name__ == "__main__":
    main()
