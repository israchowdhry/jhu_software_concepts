from urllib import parse, robotparser
import urllib3 # HTTP requests
from bs4 import BeautifulSoup #HTML parsing

base_url = "https://www.thegradcafe.com"
survey_path = "/survey/index.php"
target = 45000

headers = {
    "User-Agent": "Mozilla/5.0 (Module2-GradCafe-Scraper)"
}

# robots.txt check
parser = robotparser.RobotFileParser()
parser.set_url(parse.urljoin(base_url, "robots.txt"))
parser.read()

allowed = parser.can_fetch(headers["User-Agent"], survey_path)
if not allowed:
    print("robots.txt does not allow scraping")
    exit()

# scraping loop
results = []
page = 1

while len(results) < target:
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

        # column mapping
        university = cols[0].get_text().strip()
        program_info = cols[1].get_text(" ").strip()
        date_added = cols[2].get_text().strip()
        decision_text = cols[3].get_text().strip()

    page += 1

