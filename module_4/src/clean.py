"""
Data cleaning utilities for the Grad Cafe scraper.

This module transforms raw scraped HTML entries into structured
Python dictionaries suitable for storage in JSONL format
and insertion into a PostgreSQL database.

It also provides helper utilities for saving and loading
cleaned applicant data.
"""

import urllib3
from bs4 import BeautifulSoup
import re
import json

HEADERS = {"User-Agent": "Isra"}


# Normalizes whitespace and returns None for empty values
def _norm(text):
    """
    Normalize whitespace in a string.

    This function:
    - Converts input to string
    - Collapses multiple spaces into one
    - Returns None if the result is empty

    :param text: Input text to normalize.
    :type text: str or None
    :return: Normalized string or None if empty.
    :rtype: str or None
    """
    if text is None:
        return None
    text = " ".join(str(text).split())
    return text if text else None


# On a detail page, extracts the <dd> value for a <dt> label
def _get_value(soup, label):
    """
    Extract a <dd> value corresponding to a <dt> label from a detail page.

    :param soup: BeautifulSoup-parsed HTML document.
    :type soup: bs4.BeautifulSoup
    :param label: Label text to search for inside <dt> tags.
    :type label: str
    :return: Extracted and normalized value if found.
    :rtype: str or None
    """
    dt = soup.find("dt", string=re.compile(label, re.I))
    if not dt:
        return None
    dd = dt.find_next_sibling("dd")
    if not dd:
        return None
    return _norm(dd.get_text(" ", strip=True))


# Converts raw listing HTML into structured applicant records
def clean_data(raw_entries):
    """
    Convert raw scraped HTML entries into structured applicant dictionaries.

    This function:
    - Parses listing HTML
    - Extracts program, university, GPA, GRE, and decision data
    - Optionally fetches detail pages for missing GRE values
    - Returns structured records ready for database loading

    :param raw_entries: List of dictionaries containing raw HTML and URLs.
    :type raw_entries: list[dict]
    :return: List of cleaned applicant dictionaries.
    :rtype: list[dict]
    """
    cleaned = []

    for item in raw_entries:
        combined_html = item.get("combined_html")
        entry_url = item.get("entry_url")

        if not combined_html:
            continue

        soup = BeautifulSoup(combined_html, "html.parser")
        trs = soup.find_all("tr")
        if not trs:
            continue

        main_tr = trs[0]
        cols = main_tr.find_all("td")
        if len(cols) < 4:
            continue

        # University
        university = _norm(cols[0].get_text(" ", strip=True))

        # Program + Degree
        program_cell = cols[1]
        program_text = _norm(program_cell.get_text(" ", strip=True))

        program_name = program_text
        degree = None

        if program_text and "·" in program_text:
            left, right = program_text.split("·", 1)
            program_name = left.strip()
            degree_part = right.lower()

            if "phd" in degree_part:
                degree = "PhD"
            elif "master" in degree_part:
                degree = "Masters"
            elif "psy" in degree_part:
                degree = "PsyD"

        # Final cleanup
        program_name = re.sub(
            r"\b(ph\.?d|phd|psy\.?d|psyd|masters?)\b",
            "",
            program_name,
            flags=re.I
        )

        program_name = " ".join(program_name.split()) or None

        # Date Added
        date_added = _norm(cols[2].get_text(" ", strip=True))

        # Decision badge text
        decision_text = _norm(cols[3].get_text(" ", strip=True))

        applicant_status = None
        acceptance_date = None
        rejection_date = None

        if decision_text:
            dlow = decision_text.lower()
            if "accept" in dlow:
                applicant_status = "Accepted"
            elif "reject" in dlow:
                applicant_status = "Rejected"
            elif "wait" in dlow:
                applicant_status = "Waitlisted"
            else:
                applicant_status = decision_text

            md = re.search(r"\b\d{1,2}\s+[A-Za-z]{3,}\b", decision_text)
            if md:
                if applicant_status == "Accepted":
                    acceptance_date = md.group(0)
                elif applicant_status == "Rejected":
                    rejection_date = md.group(0)

        # Pull tags from follow-up rows text
        all_text = _norm(soup.get_text(" ", strip=True)) or ""

        start_term = None
        international_american = None
        gpa = None
        gre_score = None
        gre_v_score = None
        gre_aw = None
        comments = None

        tm = re.search(r"\b(Fall|Spring|Summer|Winter)\s+20\d{2}\b", all_text, flags=re.I)
        if tm:
            start_term = tm.group(0)

        low = all_text.lower()
        if "international" in low:
            international_american = "International"
        elif "american" in low:
            international_american = "American"

        gm = re.search(r"\bGPA\s*([0-4]\.\d+)\b", all_text, flags=re.I)
        if gm:
            gpa = gm.group(1)

        # Comment row
        p = soup.find("p")
        if p:
            comments = _norm(p.get_text(" ", strip=True))

        # Only fetch detail page if GRE fields are missing
        needs_detail = (gre_score is None) or (gre_v_score is None) or (gre_aw is None)

        if needs_detail and entry_url:
            resp = urllib3.request("GET", entry_url, headers=HEADERS)
            if resp.status == 200:
                detail_html = resp.data.decode("utf-8")
                detail_soup = BeautifulSoup(detail_html, "html.parser")

                if degree is None:
                    deg_type = _get_value(detail_soup, "Degree Type")
                    if deg_type:
                        dlow = deg_type.lower()
                        if "phd" in dlow:
                            degree = "PhD"
                        elif "master" in dlow:
                            degree = "Masters"

                if gre_score is None:
                    sp = detail_soup.find("span", string=re.compile("GRE General", re.I))
                    if sp:
                        nxt = sp.find_next("span")
                        gre_score = _norm(nxt.get_text(strip=True) if nxt else None)

                if gre_v_score is None:
                    sp = detail_soup.find("span", string=re.compile("GRE Verbal", re.I))
                    if sp:
                        nxt = sp.find_next("span")
                        gre_v_score = _norm(nxt.get_text(strip=True) if nxt else None)

                if gre_aw is None:
                    sp = detail_soup.find("span", string=re.compile("Analytical Writing", re.I))
                    if sp:
                        nxt = sp.find_next("span")
                        gre_aw = _norm(nxt.get_text(strip=True) if nxt else None)

        cleaned.append(
            {
                "program_name": program_name,
                "university": university,
                "comments": comments,
                "date_added": date_added,
                "entry_url": entry_url,
                "applicant_status": applicant_status,
                "acceptance_date": acceptance_date,
                "rejection_date": rejection_date,
                "start_term": start_term,
                "international_american": international_american,
                "gre_score": gre_score,
                "gre_v_score": gre_v_score,
                "gre_aw": gre_aw,
                "degree": degree,
                "gpa": gpa,
            }
        )

    return cleaned


def save_data(data, filename="applicant_data.json"):
    """
    Save cleaned applicant data to a JSON file.

    :param data: List of cleaned applicant dictionaries.
    :type data: list[dict]
    :param filename: Output filename.
    :type filename: str
    :return: None
    :rtype: None
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_data(filename="applicant_data.json"):
    """
    Load cleaned applicant data from a JSON file.

    :param filename: Path to JSON file.
    :type filename: str
    :return: List of applicant dictionaries.
    :rtype: list[dict]
    """
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)
