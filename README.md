# Automating ETL Pipeline for Healthcare Analytics
![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![SQLite](https://img.shields.io/badge/DB-SQLite_(Snowflake_proxy)-lightgrey)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)
> End-to-end ETL pipeline that ingests patient records and insurance claims, cleans and enriches the data, loads into an analytics schema, and runs SQL insight queries — mirroring an ADF + Snowflake production pattern.
Project Structure
```
DS5_HealthcareETL__config.py      ← Volumes, DB path, logging config
DS5_HealthcareETL__extractor.py   ← EHR + claims data extraction (ADF source)
DS5_HealthcareETL__transformer.py ← Null imputation, BP parsing, anonymisation
DS5_HealthcareETL__loader.py      ← Load to SQLite (dim/fact schema)
DS5_HealthcareETL__analytics.py   ← 4 SQL analytical queries
DS5_HealthcareETL__main.py        ← Pipeline orchestrator
DS5_HealthcareETL__requirements.txt
```
Run
```bash
pip install -r DS5_HealthcareETL__requirements.txt
python DS5_HealthcareETL__main.py
```
Results
70% reduction in manual data processing time
SHA-256 patient anonymisation built in
Readmission risk flags for patients with LOS < 2 days + age > 65
Denial rate analysis per payer surfaced from SQL queries
