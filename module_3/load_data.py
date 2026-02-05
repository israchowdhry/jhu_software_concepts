import json
import psycopg
from datetime import datetime

def load_data(jsonl_path):
    # Connect to the database created
    connection = psycopg.connect(
        dbname="gradcafe",
        user="postgres")

    # Open a cursor to perform database operations
    with connection.cursor() as cur:

            # Create table once
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

            # Read JSONL + insert rows
            with open(jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue # Ignores blank lines

                    row = json.loads(line)

                    # Convert to DATE
                    raw_date = row.get("date_added")

                    parsed_date = None
                    if raw_date:
                        try:
                            parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                        except ValueError:
                            parsed_date = None

                    # No two urls should be the same
                    cur.execute("""
                        INSERT INTO applicants (
                            program, comments, date_added, url, status,
                            term, us_or_international, gpa,
                            gre, gre_v, gre_aw, degree,
                            llm_generated_program, llm_generated_university
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (url) DO NOTHING;
                    """, (
                        f"{row.get('university')} - {row.get('program_name')}",
                        row.get("comments"),
                        parsed_date,  # <-- DATE object
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

    connection.close()

if __name__ == "__main__":
    load_data("llm_extend_applicant_data.jsonl")


