"""
Setup configuration for the Grad Cafe Analytics application.

This file makes the project installable as a package,
ensuring consistent imports and reproducible environments
across local development, testing, and CI.
"""

from setuptools import setup, find_packages

setup(
    name="gradcafe-analytics",
    version="0.1.0",
    description="Grad Cafe Analytics Web Application (Module 5)",
    author="Isra",
    packages=find_packages(),
    install_requires=[
        "flask==3.0.2",
        "psycopg[binary]==3.2.13",
        "beautifulsoup4",
        "urllib3",
        "python-dotenv",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pylint",
            "pydeps",
        ]
    },
    python_requires=">=3.10",
)
