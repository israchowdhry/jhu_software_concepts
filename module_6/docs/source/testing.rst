Testing Guide
=============

Run all tests
-------------
.. code-block:: bash

   pytest -q

Run marked tests
----------------
If markers are defined in ``pytest.ini`` you can run subsets like:

.. code-block:: bash

   pytest -m etl
   pytest -m web
   pytest -m db

Expected selectors / parsing assumptions
---------------------------------------
The scraper/cleaner assumes GradCafe listing pages contain an HTML table where each entry is represented by
a ``<tr>`` with at least 4 ``<td>`` columns. The cleaner extracts:

- University from the first ``<td>``
- Program + degree text from the second ``<td>`` (splits on the middle dot ``·``)
- Date added from the third ``<td>``
- Decision/status from the fourth ``<td>``

Additional tags (term, GPA, international/american) are parsed from the combined text of the entry rows.

If a required structure is missing (no table, missing columns, missing HTML), the record is skipped.

Fixtures / test doubles
-----------------------
To keep tests deterministic and avoid dependence on live GradCafe pages, the test suite uses local files
in ``tests/`` as controlled inputs.

- ``tests/tmp_applicant_data.json``: temporary dataset used to simulate scraped output during ETL tests.
- ``tests/applicant_data.json`` and ``tests/llm_extend_applicant_data.jsonl``: sample datasets used to test
  cleaning and ingestion without requiring network calls.

Database isolation
------------------
Tests that require database access use ``DATABASE_URL`` pointing to a test database so production data is not modified.
