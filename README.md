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

Module 3 – Database Queries Assignment

Grader Comment:
Files submitted to Canvas did not match the contents pushed to the GitHub repository.

Revision Made:
This issue was identified and corrected earlier in the semester. The GitHub repository was updated to match the submitted files, so no additional revision was needed at this time.

Why This Improves the Solution:
Ensuring consistency between submission materials and the GitHub repository allows for accurate grading and better version control practices.

Module 4 – Testing and Documentation

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The original implementation already met all assignment requirements and quality expectations.

Module 5 - Software Assurance + Secure SQL (SQLi Defense)

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

Module 6 - Deploy Anywhere

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

Module 7 - Cloud Computing

Grader Comment:
EC2_DEPLOYMENT.md was difficult to read because one line was over 700 columns wide, which forced horizontal scrolling. The grad-cafe-pipeline.ipynb notebook also did not lint to a score of 10.00/10.

Revision Made:
Reformatted EC2_DEPLOYMENT.md into a more readable structure with headings, bullet points, code blocks, and shorter lines. Updated the notebook formatting and linting issues so the notebook meets pylint expectations. 

Why This Improves the Solution:
These changes make the deployment documentation easier to read and improve the notebook’s code quality so the project looks more professional and maintainable.