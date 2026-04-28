Module 1 – Personal Website

Grader Comment:
"One minor nitpick: Your GitHub link sends me to your jhu_software_concepts repo but it should send me directly to the module_1 folder since the link is under your Module 1 project section (-1 pt.)"

Revision Made:
Updated the GitHub link in the Projects page to point directly to the module_1 directory within the repository.

Why This Improves the Solution:
This allows users to access the specific project more efficiently without navigating through the full repository.

Module 2 – Web Scraping

Grader Comment:
urllib should be used for URL management. Unavailable data should be stored in a consistent format instead of mixing null, 0, and 0.00 values. Functions should also include docstrings explaining inputs, return values, and possible errors.

Revision Made:
Updated URL construction in scrape.py to use urllib.parse.urlencode and urllib.parse.urljoin. Added missing-value normalization in clean.py so unavailable values are stored consistently as None. Added docstrings to scraping, cleaning, saving, and loading functions.

Why This Improves the Solution:
These changes make the scraper more reliable, the cleaned dataset more consistent, and the code easier for another developer to understand and maintain.