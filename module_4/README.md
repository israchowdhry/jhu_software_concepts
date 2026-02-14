SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 4 Assignment: Testing and Documentation Due on 02/15/2026 at 11:59 EST

Approach:
For this module, I built a full pytest test suite for my Flask app and database. My goal was to test everything in my project and reach 100% coverage without breaking any of the assignment rules. I organized my tests into different files based on what they test, like routes, buttons, formatting, database logic, and full end-to-end flows.

I made sure that none of my tests depend on the live internet. Whenever something normally calls the internet (like scraping), I used monkeypatch to replace it with fake data. This way the tests always run quickly and reliably. I also did not use sleep() anywhere. Instead, I directly changed state variables (like PULL_STATE["running"]) to simulate busy behavior.

I used Flask’s test client to test all routes. I did not use a browser or click anything manually. Every route is tested using GET or POST requests inside pytest.

Below is a breakdown of what each test file does.

conftest.py:

The conftest.py file sets up everything the tests need before they run. It creates the Flask test client so I can send fake GET and POST requests without opening a browser. It also sets up the DATABASE_URL environment variable and includes a reset database fixture. The reset fixture makes sure the applicants table is either created fresh or cleared before database tests run. This way, every test starts with a clean database and does not depend on previous test results. Putting all of this setup in one file keeps the tests organized and avoids repeating database setup code in every test file. It also makes the tests portable, so they work both on my computer and in GitHub Actions without hard-coding credentials.

test_flask_page.py:

This file tests that the Flask app loads correctly and that the main pages work. It checks that the app factory creates a working Flask app and that important routes like /analysis and / return status code 200. It also checks that the page contains the required elements like the “Pull Data” button, the “Update Analysis” button, the word “Analysis,” and at least one “Answer:” label. I use BeautifulSoup to read the returned HTML and confirm those elements exist. These tests make sure the page renders properly and that the backend is sending the correct data to the template. Everything is tested using Flask’s test client, so there is no manual clicking or browser interaction.

test_buttons.py:

This file tests how the POST routes behave, especially when the app is busy. It checks both /pull-data and /update-analysis. When the app is not busy, the tests confirm that the routes return status 200 and a JSON response like {"ok": True}. When the app is busy, I manually set PULL_STATE["running"] = True to simulate a background operation. Then I verify that both routes return status 409 with {"busy": True}. I also monkeypatch threading so that no background thread actually starts during testing. I did not use sleep() anywhere. Instead, I directly control the busy state, which makes the tests fast and reliable. This ensures correct behavior without depending on timing.

test_analysis_format.py:

This file checks formatting rules for the analysis page. It makes sure every analysis result includes the “Answer:” label. It also checks that percentages always show exactly two decimal places. To do this, I search the HTML using regular expressions. If any percentage appears, the test confirms that it matches the strict format like “12.34%”. This protects against formatting mistakes like showing “12.3%” or “12%”. These tests make sure the output follows the assignment’s formatting requirements.

test_db_insert.py:

This file tests how the database behaves. First, it checks that before pulling data, the applicants table is empty. After sending a POST request to /pull-data, it verifies that new rows are inserted into the database. It also checks that important fields are not null. I then test idempotency by pulling the same data twice and confirming that duplicate rows are not created. This works because the URL column has a UNIQUE constraint and uses ON CONFLICT DO NOTHING. I also test some of the query functions to make sure they return correct types and values. To keep everything controlled, I monkeypatch the scraper and cleaner so they return fake rows instead of using the real internet scraper.

test_integration_end_to_end.py:

This file tests the full system working together. It simulates the complete workflow: scrape data, clean it, write it to JSONL, load it into the database, update analysis, and render the results. I monkeypatch the scraper so it returns fake records, which avoids using the live internet. The test checks that rows appear in the database, that analysis updates correctly, and that the rendered page contains properly formatted answers and percentages.

This file also includes tests that run the __main__ blocks in app.py, scrape.py, load_data.py, and query_data.py using runpy. To prevent side effects, I monkeypatch network calls and patch Flask.run so it does not start a real server. I also wrote extra tests to force edge cases in clean.py, such as missing table rows, missing columns, unusual decision statuses, missing GRE spans, non-200 detail page responses, and robots.txt disallowed situations. These special tests were designed to push the code through every branch so that I could reach 100% coverage without breaking any assignment rules.

Known bugs: No known bugs 

Link to sphinx read the docs documentation: https://jhu-software-concepts-isra.readthedocs.io/en/latest/index.html