"""
scrape.py

This module scrapes GradCafe survey listing pages and returns raw rows
containing combined HTML segments and an optional entry URL.

The workflow is:
1) Check robots.txt to ensure scraping is allowed for the survey path.
2) Iterate survey pages until the target number of rows is collected.
3) Return raw rows for later cleaning and saving.

Notes:
- This scraper uses a custom User-Agent header.
- If robots.txt disallows scraping, the script exits immediately.
"""

from urllib import parse, robotparser
import urllib3
from bs4 import BeautifulSoup

base_url = "https://www.thegradcafe.com"
survey_path = "/survey/index.php"
headers = {"User-Agent": "Isra"}
TARGET = 30000


# Checks robots.txt and exits if scraping is disallowed
def _check_robots():
    """
    Check the site's robots.txt rules for the survey path.

    This function downloads and parses robots.txt, then checks whether the
    configured User-Agent is allowed to fetch the survey listing path.

    :raises SystemExit: If robots.txt disallows scraping the survey path.
    :return: None
    :rtype: None
    """
    parser = robotparser.RobotFileParser()
    parser.set_url(parse.urljoin(base_url, "robots.txt"))
    parser.read()

    allowed = parser.can_fetch(headers["User-Agent"], survey_path)
    if not allowed:
        print("robots.txt does not allow scraping")
        exit()

# Pulls raw rows from GradCafe listing pages
def scrape_data(target=TARGET):
    """
    Scrape raw GradCafe listing rows from the survey pages.

    This function loops over survey listing pages and extracts each valid
    table row. For each record, it stores:
    - The current <tr> row HTML plus up to two subsequent <tr> rows
      (often tag/comment rows) combined into a single string.
    - The entry URL (if present).

    The loop stops when:
    - The requested number of rows is collected, or
    - The expected table cannot be found on a page.

    :param int target: The desired number of raw row records to collect.
    :return: A list of dictionaries with keys "combined_html" and "entry_url".
    :rtype: list[dict]
    """
    _check_robots()

    raw_rows = []
    page = 1

    while len(raw_rows) < target:
        survey_url = f"{base_url}{survey_path}?page={page}"
        print(f"Scraping page {page}, rows so far: {len(raw_rows)}")

        resp = urllib3.request("GET", survey_url, headers=headers)
        soup = BeautifulSoup(resp.data.decode("utf-8"), "html.parser")

        table = soup.find("table")
        if not table:
            break

        rows = table.find_all("tr")

        # Index loop so we can grab the next <tr> rows
        for i in range(len(rows)):
            row = rows[i]
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            a = row.find("a", href=True)
            entry_url = parse.urljoin(base_url, a["href"]) if a else None

            combined_parts = [str(row)]

            # Tag row (Fall 2026 / International / GPA)
            if i + 1 < len(rows):
                combined_parts.append(str(rows[i + 1]))

            # Optional comment row
            if i + 2 < len(rows):
                combined_parts.append(str(rows[i + 2]))

            raw_rows.append(
                {
                    "combined_html": "\n".join(combined_parts),
                    "entry_url": entry_url,
                }
            )

            if len(raw_rows) >= target:
                break

        page += 1

    return raw_rows

# Run
if __name__ == "__main__":
    """
    Run the scraper as a script.

    This block scrapes raw rows, cleans them, then saves the cleaned output
    to a JSON file named "applicant_data.json".

    :return: None
    :rtype: None
    """
    from clean import clean_data, save_data

    raw = scrape_data()
    cleaned = clean_data(raw)
    save_data(cleaned, "applicant_data.json")
    print("Saved records:", len(cleaned))
