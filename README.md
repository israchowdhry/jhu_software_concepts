## Course Title - Modern Software Concepts in Python

## Isra Chowdhry

 This repository contains all coursework completed for the Modern Software Concepts in Python course. It showcases a progression of projects covering web development, data collection, database integration, cloud deployment, data analysis, machine learning, and full-stack application development. Each module builds on previous work to create a comprehensive and practical software engineering portfolio.

## Project Portfolio
The final website serves as a portfolio that presents all completed course modules in a clean and organized format. The Projects page dynamically loads project data from a JSON file and displays each module as a structured content block, including a project overview, GitHub link, and key learning outcomes. The website demonstrates the progression of skills developed throughout the semester and provides an accessible way to explore each project.

## Module 1 – Personal Website

Grader Comment:
"One minor nitpick: Your GitHub link sends me to your jhu_software_concepts repo but it should send me directly to the module_1 folder since the link is under your Module 1 project section (-1 pt.)"

Revision Made:
Updated the GitHub link in the Projects page to point directly to the module_1 directory within the repository.

Why This Improves the Solution:
This allows users to access the specific project more efficiently without navigating through the full repository.

## Module 2 – Web Scraping

Grader Comment:
urllib should be used for URL management. Unavailable data should be stored in a consistent format instead of mixing null, 0, and 0.00 values. Functions should also include docstrings explaining inputs, return values, and possible errors.

Revision Made:
Updated URL construction in scrape.py to use urllib.parse.urlencode and urllib.parse.urljoin. Added missing-value normalization in clean.py so unavailable values are stored consistently as None. Added docstrings to scraping, cleaning, saving, and loading functions.

Why This Improves the Solution:
These changes make the scraper more reliable, the cleaned dataset more consistent, and the code easier for another developer to understand and maintain.

## Module 3 – Database Queries Assignment

Grader Comment:
Files submitted to Canvas did not match the contents pushed to the GitHub repository.

Revision Made:
This issue was identified and corrected earlier in the semester. The GitHub repository was updated to match the submitted files, so no additional revision was needed at this time.

Why This Improves the Solution:
Ensuring consistency between submission materials and the GitHub repository allows for accurate grading and better version control practices.

## Module 4 – Testing and Documentation

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The original implementation already met all assignment requirements and quality expectations.

## Module 5 - Software Assurance + Secure SQL (SQLi Defense)

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

## Module 6 - Deploy Anywhere

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

## Module 7 - Cloud Computing

Grader Comment:
EC2_DEPLOYMENT.md was difficult to read because one line was over 700 columns wide, which forced horizontal scrolling. The grad-cafe-pipeline.ipynb notebook also did not lint to a score of 10.00/10.

Revision Made:
Reformatted EC2_DEPLOYMENT.md into a more readable structure with headings, bullet points, code blocks, and shorter lines. Updated the notebook formatting and linting issues so the notebook meets pylint expectations. 

Why This Improves the Solution:
These changes make the deployment documentation easier to read and improve the notebook’s code quality so the project looks more professional and maintainable.

## Module 8 – Data Preparation & Statistics

Grader Comment:
In Acceptances-over-Time.png, the date should be binned into 3-day intervals.

Revision Made:
Updated the histogram to use 3-day intervals by converting decision dates into numeric values and defining histogram bins in 3-day increments.

Why This Improves the Solution:
This ensures the visualization follows the assignment requirement and produces a clearer representation of acceptance trends over time.

## Module 9 – Data Preparation & Models

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

## Module 10 - Data Dashboard

Grader Comment:
dashboard.png showed the webpage truncated and did not display the full dashboard. Visualizations 1 and 3 were redundant, as both illustrated price versus carat. Additionally, dashboard.py could use more comments.

Revision Made:
Updated dashboard screenshots to display the full webpage, using multiple screenshots where necessary. Replaced the redundant first visualization with a new chart showing average diamond price by clarity, which introduces a different dimension to the analysis. Added additional comments throughout dashboard.py to explain data loading, visualization creation, and layout structure.

Why This Improves the Solution:
These changes ensure the dashboard is fully visible for evaluation, eliminate redundant analysis, strengthen the overall data narrative, and improve the readability and maintainability of the code.

## Module 11 - MLOps Pipeline

Grader Comment:
No issues identified.

Revision Made:
The assignment received full credit, so no revisions were necessary.

Why This Improves the Solution:
The solution already followed best practices for secure SQL handling and code quality.

## Module 12 - Two-Layer Neural Network

Grader Comment:
Missing requirements.txt.

Revision Made:
Added a module-level requirements.txt file listing the external packages needed to run neural_network.py, including matplotlib, numpy, pandas, and scikit-learn.

Why This Improves the Solution:
This makes the module easier to run in a clean environment and clearly documents its dependencies.

## Module 13 - Scale & LM Deployment

Grader Comment:
Step 1 did not explain which columns were used and why. The tokenizer choice was also not explicitly explained.

Revision Made:
Added a clear explanation of the text and structured columns used for model input, including why each group of fields was included. Also added an explicit explanation that AutoTokenizer was used with distilbert-base-uncased so the tokenizer matches the pretrained transformer model.

Why This Improves the Solution:
These additions make the modeling choices easier to understand and show why the selected fields and tokenizer were appropriate for the admissions prediction task.

## Organization of Repository 

The repository is organized into module-based folders (module_1 through module_13), where each folder contains the code and deliverables for that specific assignment.

A root-level requirements.txt file lists all dependencies used across modules. The root README.md provides an overview of the repository, a log of grader corrections, and a summary of the final portfolio.

Supporting files such as data, templates, static assets, and documentation are organized within their respective module directories to maintain clarity and modularity.

## Final Reflection 

Over the course of the semester, I found Module 2 (Web Scraping) to be the most challenging. It was something completely new to me, and coming from Intro to Python, which was my first coding class, it felt overwhelming at times. I had to adjust to a much deeper level of coding and problem-solving than I was used to, and it took time to really understand how to approach both the logic and the structure of the code. The constant troubleshooting was also challenging, as small errors could take a while to identify and fix, which sometimes made the process feel frustrating.

The module that best reflects my strongest work is Module 9 (Data Preparation & Models). I felt that I understood the concepts the most in this module and was able to apply them confidently. It was one of the first times I felt fully comfortable with both the implementation and the reasoning behind the solution.

Throughout the semester, I believe my problem-solving skills improved the most. Early on, I would often get stuck and not know how to move forward, but over time I became more confident in breaking problems down, testing different approaches, and debugging my code.

At the beginning of the course, I did not know what to expect from Python or how much could be done with it. By the end, I realized how powerful it is and how many different ways there are to accomplish the same goal. This course helped me see Python not just as a language, but as a tool that can be used to build complete systems and solve real-world problems.