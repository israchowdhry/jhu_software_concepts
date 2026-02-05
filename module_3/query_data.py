import psycopg

def get_conn():
    return psycopg.connect(
        dbname="gradcafe",
        user="postgres")

def fetch_one(sql: str, params=()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]


def fetch_row(sql: str, params=()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def fetch_all(sql: str, params=()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

# How many entries do you have in your database who have applied for Fall 2026?
Q1_SQL = "SELECT COUNT(*) FROM applicants WHERE term = %s;"

def q1():
    return fetch_one(Q1_SQL, ("Fall 2026",))


# % international students to 2 decimals
Q2_SQL = """
SELECT ROUND(
    100.0 * SUM(CASE WHEN us_or_international NOT IN ('American','Other') THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
)
FROM applicants;
"""

def q2():
    return fetch_one(Q2_SQL)



# Avg GPA, GRE, GRE V, GRE AW
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

def q3():
    return fetch_row(Q3_SQL)

# Avg GPA of American students in Fall 2026
Q4_SQL = """
SELECT ROUND(AVG(gpa)::numeric, 3)
FROM applicants
WHERE term = %s
  AND us_or_international = %s
  AND gpa IS NOT NULL;
"""

def q4():
    return fetch_one(Q4_SQL, ("Fall 2026", "American"))

# % of Fall 2026 entries that are Acceptances to 2 decimals
Q5_SQL = """
SELECT ROUND(
    100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
)
FROM applicants
WHERE term = %s;
"""

def q5():
    return fetch_one(Q5_SQL, ("Fall 2026",))


# Avg GPA of Fall 2026 applicants who are Acceptances
Q6_SQL = """
SELECT ROUND(AVG(gpa)::numeric, 3)
FROM applicants
WHERE COALESCE(term,'') ILIKE %s
  AND COALESCE(status,'') ILIKE %s
  AND gpa IS NOT NULL;
"""

def q6():
    return fetch_one(Q6_SQL, (
        "%Fall 2026%",
        "%Accepted%"
    ))

# How many entries applied to JHU for a masters in Computer Science?
Q7_SQL = """
SELECT COUNT(*)
FROM applicants
WHERE degree ILIKE %s
  AND program ILIKE %s
  AND (program ILIKE %s OR program ILIKE %s);
"""

def q7():
    return fetch_one(Q7_SQL, (
        "%master%",
        "%computer science%",
        "%johns hopkins%",
        "%jhu%",
    ))

# Q8: How many entries from 2026 are acceptances for Georgetown/MIT/Stanford/CMU for PhD CS?
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

def q8():
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
    ))

# Do numbers for Q8 change if using LLM generated fields?
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

def q9():
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
    ))

# Additional 2 questions
EXTRA_1_QUESTION = "What are the top 3 most common universities/program names in the dataset?"
EXTRA_1_SQL = """
SELECT program, COUNT(*) AS n
FROM applicants
WHERE program IS NOT NULL AND program <> ''
GROUP BY program
ORDER BY n DESC
LIMIT 3;
"""

def extra_1():
    return fetch_all(EXTRA_1_SQL)


EXTRA_2_QUESTION = "How many people got rejected from JHU?"
EXTRA_2_SQL = """
SELECT COUNT(*)
FROM applicants
WHERE COALESCE(status,'') = %s
  AND (program ILIKE %s OR program ILIKE %s);
"""

def extra_2():
    return fetch_one(EXTRA_2_SQL, (
        "Rejected",
        "%johns hopkins%",
        "%jhu%",
    ))

def main():
    # Print answers
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
    print(f"Q9 (2026 PhD CS Acceptances at GU/MIT/Stanford/CMU using LLM fields): {q9()}")

    print(f"Extra Q1: {EXTRA_1_QUESTION}")
    for program, n in extra_1():
        print(f"  {n:>3}  {program}")

    print(f"Extra Q2: {EXTRA_2_QUESTION} {extra_2()}")


if __name__ == "__main__":
    main()
