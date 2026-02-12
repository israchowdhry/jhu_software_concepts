"""
Database query utilities for Grad Cafe applicant analytics.

This module provides helper functions for connecting to the PostgreSQL
database and executing analytical queries used in the assignment.

The queries compute statistics such as applicant counts, averages,
acceptance rates, and program-level aggregations.
"""

import os
import psycopg


def get_conn(db_url=None):
    """
    Establish a connection to the PostgreSQL database.

    If ``db_url`` is not provided, the function attempts to read the
    ``DATABASE_URL`` environment variable.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Active PostgreSQL connection object.
    :rtype: psycopg.Connection
    :raises RuntimeError: If no database URL is available.
    """
    db_url = db_url or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(db_url)


def fetch_one(sql: str, params=(), db_url=None):
    """
    Execute a SQL query and return a single scalar value.

    :param sql: SQL query string.
    :type sql: str
    :param params: Parameters to safely substitute into the query.
    :type params: tuple
    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: First column of the first row returned by the query.
    :rtype: any
    """
    with get_conn(db_url=db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]


def fetch_row(sql: str, params=(), db_url=None):
    """
    Execute a SQL query and return a single row.

    :param sql: SQL query string.
    :type sql: str
    :param params: Parameters to safely substitute into the query.
    :type params: tuple
    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: First row returned by the query.
    :rtype: tuple
    """
    with get_conn(db_url=db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def fetch_all(sql: str, params=(), db_url=None):
    """
    Execute a SQL query and return all rows.

    :param sql: SQL query string.
    :type sql: str
    :param params: Parameters to safely substitute into the query.
    :type params: tuple
    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: All rows returned by the query.
    :rtype: list[tuple]
    """
    with get_conn(db_url=db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# -------------------------------
# Q1
# -------------------------------

Q1_SQL = "SELECT COUNT(*) FROM applicants WHERE term = %s;"


def q1(db_url=None):
    """
    Return the number of applicants for Fall 2026.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Count of Fall 2026 applicants.
    :rtype: int
    """
    return fetch_one(Q1_SQL, ("Fall 2026",), db_url=db_url)


# -------------------------------
# Q2
# -------------------------------

Q2_SQL = """
SELECT ROUND(
    100.0 * SUM(CASE WHEN us_or_international NOT IN ('American','Other') THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
)
FROM applicants;
"""


def q2(db_url=None):
    """
    Return the percentage of international applicants (rounded to 2 decimals).

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Percentage of international applicants.
    :rtype: float
    """
    return fetch_one(Q2_SQL, db_url=db_url)


# -------------------------------
# Q3
# -------------------------------

Q3_SQL = """
SELECT
  ROUND(AVG(gpa)::numeric, 2) AS avg_gpa,
  ROUND(AVG(gre)::numeric, 2) AS avg_gre_q,
  ROUND(AVG(gre_v)::numeric, 2) AS avg_gre_v,
  ROUND(AVG(gre_aw)::numeric, 2) AS avg_gre_aw
FROM applicants
WHERE (gpa IS NULL OR (gpa BETWEEN 0 AND 4.0))
  AND (gre IS NULL OR (gre BETWEEN 130 AND 170))
  AND (gre_v IS NULL OR (gre_v BETWEEN 130 AND 170))
  AND (gre_aw IS NULL OR (gre_aw BETWEEN 0 AND 6));
"""


def q3(db_url=None):
    """
    Return average GPA and GRE scores (rounded to 2 decimals).

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Tuple containing (avg_gpa, avg_gre_q, avg_gre_v, avg_gre_aw).
    :rtype: tuple[float, float, float, float]
    """
    return fetch_row(Q3_SQL, db_url=db_url)


# -------------------------------
# Q4
# -------------------------------

Q4_SQL = """
SELECT ROUND(AVG(gpa)::numeric, 2)
FROM applicants
WHERE term = %s
  AND us_or_international = %s
  AND gpa IS NOT NULL;
"""


def q4(db_url=None):
    """
    Return the average GPA of American students for Fall 2026.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Average GPA.
    :rtype: float
    """
    return fetch_one(Q4_SQL, ("Fall 2026", "American"), db_url=db_url)


# -------------------------------
# Q5
# -------------------------------

Q5_SQL = """
SELECT ROUND(
    100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
)
FROM applicants
WHERE term = %s;
"""


def q5(db_url=None):
    """
    Return percentage of Fall 2026 applicants who were accepted.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Acceptance percentage.
    :rtype: float
    """
    return fetch_one(Q5_SQL, ("Fall 2026",), db_url=db_url)


# -------------------------------
# Q6
# -------------------------------

Q6_SQL = """
SELECT ROUND(AVG(gpa)::numeric, 2)
FROM applicants
WHERE COALESCE(term,'') ILIKE %s
  AND COALESCE(status,'') ILIKE %s
  AND gpa IS NOT NULL;
"""


def q6(db_url=None):
    """
    Return the average GPA of Fall 2026 applicants who were accepted.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Average GPA of accepted applicants.
    :rtype: float
    """
    return fetch_one(Q6_SQL, ("%Fall 2026%", "%Accepted%"), db_url=db_url)


# -------------------------------
# Q7
# -------------------------------

Q7_SQL = """
SELECT COUNT(*)
FROM applicants
WHERE degree ILIKE %s
  AND program ILIKE %s
  AND (program ILIKE %s OR program ILIKE %s);
"""


def q7(db_url=None):
    """
    Return the number of entries applying to JHU for a Masters in Computer Science.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Count of JHU Masters CS applicants.
    :rtype: int
    """
    return fetch_one(Q7_SQL, (
        "%master%",
        "%computer science%",
        "%johns hopkins%",
        "%jhu%",
    ), db_url=db_url)


# -------------------------------
# Q8
# -------------------------------

Q8_SQL = """
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
  );
"""


def q8(db_url=None):
    """
    Return count of 2026 PhD CS acceptances at Georgetown, MIT,
    Stanford, or Carnegie Mellon.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Count of matching applicants.
    :rtype: int
    """
    return fetch_one(Q8_SQL, (
        "%2026%",
        "Accepted",
        "%phd%",
        "%computer science%",
        "%georgetown%",
        "%Massachusetts Institute of Technology%",
        "%mit%",
        "%stanford%",
        "%carnegie mellon%",
    ), db_url=db_url)


# -------------------------------
# Q9
# -------------------------------

Q9_SQL = """
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
  );
"""


def q9(db_url=None):
    """
    Return count of 2026 PhD CS acceptances using LLM-generated fields.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Count of matching applicants using LLM fields.
    :rtype: int
    """
    return fetch_one(Q9_SQL, (
        "%2026%",
        "Accepted",
        "%phd%",
        "%computer science%",
        "%georgetown%",
        "%massachusetts institute of technology%",
        "%mit%",
        "%stanford%",
        "%carnegie mellon%",
    ), db_url=db_url)


# -------------------------------
# Extra Questions
# -------------------------------

EXTRA_1_QUESTION = "What are the top 3 most common universities/program names in the dataset?"


def extra_1(db_url=None):
    """
    Return the top 3 most common program names.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: List of (program, count) tuples.
    :rtype: list[tuple[str, int]]
    """
    return fetch_all(EXTRA_1_SQL, db_url=db_url)


EXTRA_2_QUESTION = "How many people got rejected from JHU?"


def extra_2(db_url=None):
    """
    Return count of applicants rejected from JHU.

    :param db_url: Optional database connection URL.
    :type db_url: str or None
    :return: Count of rejected JHU applicants.
    :rtype: int
    """
    return fetch_one(EXTRA_2_SQL, (
        "Rejected",
        "%johns hopkins%",
        "%jhu%",
    ), db_url=db_url)


def main():
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
