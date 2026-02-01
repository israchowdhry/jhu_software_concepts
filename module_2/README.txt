SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 2 Assignment: Web Scraping Due on 02/01/2026 at 11:59 EST

Approach:
This assignment was completed in three major phases:
1. Web scraping of Grad Cafe applicant data
2. Data cleaning and normalization
3. Post-processing and standardization using a local LLM

1. Web Scraping (scrape.py)
The goal of scrape.py is to programmatically pull at least 30,000 applicant entries from Grad Cafe while respecting the site’s scraping rules.

Robots.txt Compliance:
Before any scraping begins, the script explicitly checks robots.txt using Python’s built-in robotparser. If the /survey/index.php path is disallowed, the script exits immediately. This ensures compliance with the site’s policies. (Screenshot of robots.txt compliance under module_2)

Pagination Strategy:
Grad Cafe listings are paginated. The script iterates page-by-page until the target number of rows is collected. For each page: 1. The HTML table containing applicant results is located. 2. Each <tr> row is examined. 3.Rows with fewer than 4 <td> cells are skipped (these are header or spacer rows).

Capturing Multi-Row Records:
Each applicant record can span multiple table rows:
1. A main row (program, university, decision, date)
2. A follow-up row containing tags (e.g., Fall 2026, International, GPA)
3. An optional comment row

To preserve all context, I concatenate up to three adjacent <tr> rows into a single combined_html field. This ensures the cleaner has access to all text related to the applicant, even when information is split across rows.
Each raw record is stored as:
{
  "combined_html": "<html snippet>",
  "entry_url": "https://www.thegradcafe.com/result/XXXXX"
}


2. Data Cleaning (clean.py)
The clean.py module converts raw HTML snippets into structured records that are ready for analysis.

HTML Parsing:
Each combined_html block is parsed with BeautifulSoup. From the primary <tr>:
1. University name is extracted. 2. Program name and degree are parsed. 3. Date added and decision text are captured

Whitespace is normalized using a helper _norm() function to ensure consistent formatting.

Program Name and Degree Separation:
Program names often include degree information (e.g., Biomedical Engineering · PhD).
I split on the · character when present:
Left side: program name
Right side: degree (PhD, Masters, PsyD)

A regex cleanup step removes any lingering degree words embedded in program names to avoid duplication.

Applicant Status and Decision Dates
The decision badge text is analyzed to determine:
1. Applicant status (Accepted, Rejected, Waitlisted)
2. Acceptance or rejection dates (when present)

Regex is used to extract day/month information from decision text. 

Extracting Supplemental Attributes
From the combined text of all related rows: Start term (e.g., Fall 2026), International vs. American, GPA, and Comments (from <p> tags).


If GRE scores or degree data are missing, the script conditionally fetches the detail page using def _get_value (entry_url) and attempts to extract these values using labeled HTML elements.
All missing or unavailable fields are consistently stored as null.
Each cleaned record is written as a dictionary and ultimately saved to applicant_data.json.

NOTES: As encouraged through lecture, ChatGPT was used to help with regex syntax. 

3. Program & University Standardization with a Local LLM (llm_hosting/app.py)
After producing a clean dataset, I performed a second pass to standardize program and university names, which often appear in inconsistent or abbreviated forms (e.g., JHU, Johns Hopkins, John Hopkins).

Implementation Details:

I used a TinyLlama GGUF model via llama-cpp-python

The model is prompted with few-shot examples demonstrating how to split and standardize program/university strings

The model returns JSON-only output with: (Note: mine returned a jsonl but was confirmed valid by professor.)

standardized_program

standardized_university


Post-Processing Safeguards:
To ensure consistency:

Canonical program and university lists are applied

Known abbreviations (e.g., UBC, McG) are expanded

Common misspellings are corrected

Lightweight fuzzy matching (difflib) maps near-matches to canonical names

If no university can be inferred, "Unknown" is used

Adjustments made to app.py:
To meet the assignment’s data consistency requirements, I added a post-processing step that converts placeholder GRE values such as "0" or "0.00" into null. This ensures that missing data is represented uniformly and does not appear as misleading numeric values.
The LLM output is written incrementally as JSON Lines (.jsonl), allowing the process to be safely interrupted and resumed. I also edited the the program_text in def standardize and def _cli_process_file to look for 'program_name' or 'university' to match my python code from scrape.py and clean.py.

Known Bugs / Limitations:

No known bugs that prevent correct data extraction or standardization.


