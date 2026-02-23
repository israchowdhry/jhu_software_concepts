"""
Data cleaning utilities for the Grad Cafe scraper.

This module transforms raw scraped HTML entries into structured
Python dictionaries suitable for storage in JSONL format
and insertion into a PostgreSQL database.

It also provides helper utilities for saving and loading
cleaned applicant data.
"""

import json
import re

import urllib3
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Isra"}


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


def _parse_program_and_degree(program_text):
    """
    Parse program text into (program_name, degree).

    Handles cases like: "Computer Science · Masters".

    :param program_text: Raw program text.
    :type program_text: str
    :return: (program_name, degree)
    :rtype: tuple[str | None, str | None]
    """
    if not program_text:
        return None, None

    program_name = program_text
    degree = None

    if "·" in program_text:
        left, right = program_text.split("·", 1)
        program_name = left.strip() or None
        degree_part = right.lower()

        if "phd" in degree_part:
            degree = "PhD"
        elif "master" in degree_part:
            degree = "Masters"
        elif "psy" in degree_part:
            degree = "PsyD"

    if program_name:
        program_name = re.sub(
            r"\b(ph\.?d|phd|psy\.?d|psyd|masters?)\b",
            "",
            program_name,
            flags=re.I,
        )
        program_name = " ".join(program_name.split()) or None

    return program_name, degree


def _parse_decision(decision_text):
    """
    Parse decision text into (status, acceptance_date, rejection_date).

    :param decision_text: Decision badge text (e.g., "Accepted 12 Feb").
    :type decision_text: str | None
    :return: (status, acceptance_date, rejection_date)
    :rtype: tuple[str | None, str | None, str | None]
    """
    if not decision_text:
        return None, None, None

    dlow = decision_text.lower()
    if "accept" in dlow:
        status = "Accepted"
    elif "reject" in dlow:
        status = "Rejected"
    elif "wait" in dlow:
        status = "Waitlisted"
    else:
        status = decision_text

    acceptance_date = None
    rejection_date = None

    md = re.search(r"\b\d{1,2}\s+[A-Za-z]{3,}\b", decision_text)
    if md:
        if status == "Accepted":
            acceptance_date = md.group(0)
        elif status == "Rejected":
            rejection_date = md.group(0)

    return status, acceptance_date, rejection_date


def _extract_start_term(all_text):
    """
    Extract start term like 'Fall 2026' from free-text content.

    :param all_text: Full text extracted from listing HTML.
    :type all_text: str
    :return: Start term or None.
    :rtype: str | None
    """
    tm = re.search(
        r"\b(Fall|Spring|Summer|Winter)\s+20\d{2}\b",
        all_text,
        flags=re.I,
    )
    return tm.group(0) if tm else None


def _extract_us_or_international(all_text):
    """
    Extract whether entry indicates international or American.

    :param all_text: Full text extracted from listing HTML.
    :type all_text: str
    :return: "International", "American", or None.
    :rtype: str | None
    """
    low = all_text.lower()
    if "international" in low:
        return "International"
    if "american" in low:
        return "American"
    return None


def _extract_gpa(all_text):
    """
    Extract GPA like 'GPA 3.7' from free-text content.

    :param all_text: Full text extracted from listing HTML.
    :type all_text: str
    :return: GPA string or None.
    :rtype: str | None
    """
    gm = re.search(r"\bGPA\s*([0-4]\.\d+)\b", all_text, flags=re.I)
    return gm.group(1) if gm else None


def _extract_span_value(detail_soup, label):
    """
    Find a span containing label text, then return next span value.

    :param detail_soup: BeautifulSoup parsed detail HTML.
    :type detail_soup: bs4.BeautifulSoup
    :param label: Label text to search in spans.
    :type label: str
    :return: Extracted value or None.
    :rtype: str | None
    """
    sp = detail_soup.find("span", string=re.compile(label, re.I))
    if not sp:
        return None
    nxt = sp.find_next("span")
    return _norm(nxt.get_text(strip=True) if nxt else None)


def _fetch_detail_fields(entry_url):
    """
    Fetch detail page and extract (degree, gre_total, gre_v, gre_aw).

    Returns (None, None, None, None) if fetch fails.

    :param entry_url: Detail page URL.
    :type entry_url: str
    :return: (degree, gre_total, gre_v, gre_aw)
    :rtype: tuple[str | None, str | None, str | None, str | None]
    """
    if not entry_url:
        return None, None, None, None

    try:
        resp = urllib3.request("GET", entry_url, headers=HEADERS)
    except (urllib3.exceptions.HTTPError, urllib3.exceptions.MaxRetryError, OSError):
        return None, None, None, None

    if resp.status != 200:
        return None, None, None, None

    detail_html = resp.data.decode("utf-8", errors="replace")
    detail_soup = BeautifulSoup(detail_html, "html.parser")

    degree = None
    deg_type = _get_value(detail_soup, "Degree Type")
    if deg_type:
        dlow = deg_type.lower()
        if "phd" in dlow:
            degree = "PhD"
        elif "master" in dlow:
            degree = "Masters"

    gre_total = _extract_span_value(detail_soup, "GRE General")
    gre_v = _extract_span_value(detail_soup, "GRE Verbal")
    gre_aw = _extract_span_value(detail_soup, "Analytical Writing")

    return degree, gre_total, gre_v, gre_aw


def _extract_row_cells(soup):
    """
    Extract and validate the first table row and its cells.

    :param soup: Parsed listing HTML soup.
    :type soup: bs4.BeautifulSoup
    :return: List of <td> cells or None if invalid.
    :rtype: list | None
    """
    trs = soup.find_all("tr")
    if not trs:
        return None

    cols = trs[0].find_all("td")
    if len(cols) < 4:
        return None

    return cols


def _extract_comments(soup):
    """
    Extract optional comment text from the listing soup.

    :param soup: Parsed listing HTML soup.
    :type soup: bs4.BeautifulSoup
    :return: Comment string or None.
    :rtype: str | None
    """
    p_tag = soup.find("p")
    if not p_tag:
        return None
    return _norm(p_tag.get_text(" ", strip=True))


def _extract_summary_fields(soup):
    """
    Extract fields from the combined listing HTML (no detail page).

    :param soup: Parsed BeautifulSoup document for listing HTML.
    :type soup: bs4.BeautifulSoup
    :return: Dict of extracted fields or None if listing is malformed.
    :rtype: dict | None
    """
    first_row = soup.find("tr")
    if not first_row:
        return None

    cells = first_row.find_all("td")
    if len(cells) < 4:
        return None

    program_name, degree = _parse_program_and_degree(
        _norm(cells[1].get_text(" ", strip=True)) or ""
    )

    status, acceptance_date, rejection_date = _parse_decision(
        _norm(cells[3].get_text(" ", strip=True))
    )

    comments = None
    p_tag = soup.find("p")
    if p_tag:
        comments = _norm(p_tag.get_text(" ", strip=True))

    full_text = _norm(soup.get_text(" ", strip=True)) or ""

    return {
        "university": _norm(cells[0].get_text(" ", strip=True)),
        "program_name": program_name,
        "degree": degree,
        "date_added": _norm(cells[2].get_text(" ", strip=True)),
        "applicant_status": status,
        "acceptance_date": acceptance_date,
        "rejection_date": rejection_date,
        "comments": comments,
        "start_term": _extract_start_term(full_text),
        "international_american": _extract_us_or_international(full_text),
        "gpa": _extract_gpa(full_text),
    }

def clean_data(raw_entries):
    """
    Convert raw scraped HTML entries into structured applicant dictionaries.

    This function:
    - Parses listing HTML
    - Extracts program, university, GPA, and decision data
    - Optionally fetches detail pages for GRE values
    - Returns structured records ready for database loading

    :param raw_entries: List of dictionaries containing raw HTML and URLs.
    :type raw_entries: list[dict]
    :return: List of cleaned applicant dictionaries.
    :rtype: list[dict]
    """
    cleaned = []

    for item in raw_entries:
        combined_html = item.get("combined_html")
        if not combined_html:
            continue

        entry_url = item.get("entry_url")
        base = _extract_summary_fields(BeautifulSoup(combined_html, "html.parser"))
        if base is None:
            continue

        det = (None, None, None, None)
        if entry_url:
            det = _fetch_detail_fields(entry_url)

        if base["degree"] is None:
            base["degree"] = det[0]

        gre_vals = det[1:]

        cleaned.append(
            {
                "program_name": base["program_name"],
                "university": base["university"],
                "comments": base["comments"],
                "date_added": base["date_added"],
                "entry_url": entry_url,
                "applicant_status": base["applicant_status"],
                "acceptance_date": base["acceptance_date"],
                "rejection_date": base["rejection_date"],
                "start_term": base["start_term"],
                "international_american": base["international_american"],
                "gre_score": gre_vals[0],
                "gre_v_score": gre_vals[1],
                "gre_aw": gre_vals[2],
                "degree": base["degree"],
                "gpa": base["gpa"],
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
