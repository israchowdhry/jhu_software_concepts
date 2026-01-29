from urllib import parse, robotparser
import urllib3 # HTTP requests
from bs4 import BeautifulSoup #HTML parsing
import re
import time
import random
import json

base_url = "https://www.thegradcafe.com"
survey_path = "/survey/index.php"
target = 45000

headers = {
    "User-Agent": "Isra"
}

# robots.txt check
def _check_robots():
    parser = robotparser.RobotFileParser()
    parser.set_url(parse.urljoin(base_url, "robots.txt"))
    parser.read()

    allowed = parser.can_fetch(headers["User-Agent"], survey_path)
    if not allowed:
        print("robots.txt does not allow scraping")
        exit()

# scraping loop
def scrape_data(target=target):
    _check_robots()

    raw_rows = []
    page = 1

    while len(raw_rows) < target:
        survey_url = base_url + survey_path + "?page=" + str(page) + "&pp=250"

        # request pages with urllib3
        response = urllib3.request("GET", survey_url, headers=headers)
        html = response.data.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table")
        if not table:
            break

        rows = table.find_all("tr")
        if len(rows) <= 1:
            break

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            # store raw text
            raw_rows.append(row)

            if len(raw_rows) >= target:
                break

        page += 1
        time.sleep(random.uniform(0.6, 1.4))

    return raw_rows

def clean_data(raw_rows):
    cleaned = []

    for row in raw_rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        # column mapping
        university = cols[0].get_text().strip()

        program_cell = cols[1]
        program_info = program_cell.get_text(" ").strip()

        # comments (if available)
        comment_tag = program_cell.find("p")
        if comment_tag:
            comments = comment_tag.get_text().strip()
        else:
            comments = None

        date_added = cols[2].get_text().strip()
        decision_text = cols[3].get_text().strip()

        # program and degree
        program_name = program_info
        degree = None

        if "·" in program_info:
            parts = program_info.split("·")
            program_name = parts[0].strip()
            degree_part = parts[1].lower()

            if "phd" in degree_part:
                degree = "PhD"
            elif "psy.d" in degree_part or "psyd" in degree_part:
                degree = "PsyD"
            elif "master" in degree_part:
                degree = "Masters"

        # applicant status and dates
        status = None
        acceptance_date = None
        rejection_date = None

        decision_lower = decision_text.lower()

        if "accept" in decision_lower:
            status = "Accepted"
        elif "reject" in decision_lower:
            status = "Rejected"
        elif "wait" in decision_lower:
            status = "Waitlisted"

        m = re.search(r"\b\d{1,2}\s+\w+\b", decision_text)
        if m:
            if status == "Accepted":
                acceptance_date = m.group(0)
            elif status == "Rejected":
                rejection_date = m.group(0)

        # tags from program column
        start_term = None
        intl = None
        gpa = None
        gre_score = None
        gre_v = None
        gre_aw = None

        term_match = re.search(r"(Fall|Spring|Summer|Winter)\s+20\d{2}", program_info, re.I)
        if term_match:
            start_term = term_match.group(0)

        if "american" in program_info.lower():
            intl = "American"
        elif "international" in program_info.lower():
            intl = "International"

        m = re.search(r"GPA\s*([0-4]\.\d+)", program_info)
        if m:
            gpa = m.group(1)

        m = re.search(r"GRE\s*[:=]?\s*(\d{3})", program_info)
        if m:
            gre_score = m.group(1)

        m = re.search(r"\bV\s*(\d{2,3})", program_info)
        if m:
            gre_v = m.group(1)

        m = re.search(r"AW\s*([0-6]\.\d)", program_info)
        if m:
            gre_aw = m.group(1)

        # entry URL
        a = row.find("a", href=True)
        if a:
            entry_url = a["href"]
        else:
            entry_url = None

        # save record
        cleaned.append({
            "program_name": program_name,
            "university": university,
            "comments": comments,
            "date_added": date_added,
            "entry_url": entry_url,
            "applicant_status": status,
            "acceptance_date": acceptance_date,
            "rejection_date": rejection_date,
            "start_term": start_term,
            "international_american": intl,
            "gre_score": gre_score,
            "gre_v_score": gre_v,
            "gre_aw": gre_aw,
            "degree": degree,
            "gpa": gpa
        })

    return cleaned

# save JSON
def save_data(data, filename="applicant_data.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_data(filename="applicant_data.json"):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

# run
if __name__ == "__main__":
    raw = scrape_data()
    cleaned = clean_data(raw)
    save_data(cleaned, "applicant_data.json")
