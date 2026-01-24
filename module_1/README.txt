Module 1 – Personal Website

Code overview:
1) run.py - This code is to start the website. I began by importing the Flask application factory function (create_app) from the app package. I then created the Flask app instance by calling create_app(). I then ran the Flask development server on host 0.0.0.0 and port 8080 when the file is directly executed.

2) __init__.py - This is where I built and configured the Flask application. I created the Flask app object: Flask(__name__). I then imported and registered the pages blueprint (pages_bp) so the app knows about all page routes. I then returned the configured app back to run.py.

3) routes.py - This is where I defined the website's URL routes. I created the Flask blueprint named "pages" to organize page routes. I defined the routes for home page = "/", projects page = "/projects", and contact page = "/contact". Each route returns the HTML template using render_template().

4) Static and Templates: ChatGPT was used to assist with generating HTML templates and CSS styling (style.css, base.html, contact.html, home.html, and projects.html), as allowed by course guidelines. The resource given in module 1 (The Real Python article) was used as a basis to construct these templates alongside ChatGPT to help as recommended from the lecture. Style.css = how the website will look like. Base.html = the base of what will be the same throughout the pages. Contact.html = what will be shown on the contact page. Home.html = what will be shown on the home page. Projects.html = what will be shown on the projects page.

Project structure:
- run.py: Entry point for the Flask application
- requirements.txt: Python dependencies
- app/: Application source code
- static/: CSS and images
- templates/: HTML templates

SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

How to run the site:

1) Open a terminal and navigate to the repository root:
   cd jhu_software_concepts

2) Navigate into the solution folder:
   cd module_1

3) Install required dependencies:
   pip install -r requirements.txt

4) Run the Flask application:
   python run.py

5) Open a web browser and go to:
   http://localhost:8080

