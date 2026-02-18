Fresh Install Instructions

To set up the project from scratch using pip, first create a virtual environment using python -m venv .venv, then activate it (on Windows use .venv\Scripts\activate, and on Mac/Linux use source .venv/bin/activate). Once activated, upgrade pip and install all required dependencies using pip install --upgrade pip followed by pip install -r requirements.txt. After installing dependencies, install the project itself in editable mode using pip install -e .. Editable installation ensures imports behave consistently across local development, testing, and CI environments, preventing common path-related issues.

To install using uv, create a virtual environment with uv venv, then synchronize dependencies exactly as specified in requirements.txt using uv pip sync requirements.txt. This guarantees that the environment matches the requirements file precisely, improving reproducibility. After syncing, install the package in editable mode using uv pip install -e ..

The application requires database configuration via environment variables. You may either define DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD, or provide a full DATABASE_URL. These values can be placed in a .env file (which should not be committed to version control). An example configuration file is included to demonstrate required variable names.

To verify correctness, run pytest to execute the full test suite. The project enforces 100% coverage and includes tests for SQL injection safety, database behavior, Flask routes, and end-to-end flows. To verify code quality, run pylint src, which should complete without errors and achieve a full lint score.

To run the web application, execute python -m src.app and open http://localhost:8080 in your browser. The interface allows you to trigger the ETL pipeline, update analysis results, and view formatted analytics output.

This project includes a setup.py file so it can be installed as a proper Python package. Packaging ensures consistent import behavior across environments, supports editable installs, reduces “works on my machine” issues, improves CI reliability, and allows tools such as uv to synchronize dependencies accurately. If a fresh install works using either pip or uv, pytest passes with 100% coverage, pylint passes cleanly, and the Flask app runs successfully, then Step 5 requirements are fully satisfied.