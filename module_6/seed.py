import os
import json
import psycopg

conn = psycopg.connect('postgresql://admin:password@db:5432/appdb')
cur = conn.cursor()

with open('/data/llm_extend_applicant_data.jsonl') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        program = f"{row.get('university')} - {row.get('program_name')}"
        cur.execute(
            """INSERT INTO applicants (program, comments, date_added, url, status, term, us_or_international, gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university)
            VALUES (%s,%s,TO_DATE(NULLIF(%s,''),'Month DD, YYYY'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (url) DO NOTHING""",
            (program, row.get('comments'), row.get('date_added'), row.get('entry_url'),
             row.get('applicant_status'), row.get('start_term'), row.get('international_american'),
             row.get('gpa'), row.get('gre_score'), row.get('gre_v_score'), row.get('gre_aw'),
             row.get('degree'), row.get('llm_generated_program'), row.get('llm_generated_university')))

conn.commit()
cur.execute('SELECT COUNT(*) FROM applicants')
print('Total rows:', cur.fetchone()[0])
conn.close()