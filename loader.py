"""
loader.py — Load cleaned data into SQLite analytics schema (proxies Snowflake)
"""
import sqlite3, logging, pandas as pd
import config

log = logging.getLogger("Loader")

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_patients (
    patient_hash TEXT PRIMARY KEY, age INTEGER, gender TEXT,
    age_group TEXT, department TEXT, attending_doctor TEXT,
    high_readmission_risk INTEGER
);
CREATE TABLE IF NOT EXISTS fact_admissions (
    patient_id TEXT, admission_date TEXT, discharge_date TEXT,
    department TEXT, primary_icd10 TEXT, status TEXT,
    los_days INTEGER, bp_systolic REAL, bp_diastolic REAL, load_ts TEXT
);
CREATE TABLE IF NOT EXISTS fact_claims (
    claim_id TEXT PRIMARY KEY, patient_id TEXT, claim_date TEXT,
    billed_amount REAL, approved_amount REAL, net_reimbursement REAL,
    payer TEXT, claim_status TEXT, cpt_code TEXT,
    denial_flag INTEGER, claim_year_month TEXT, high_value_flag INTEGER, load_ts TEXT
);
"""

def load(patients: pd.DataFrame, claims: pd.DataFrame) -> sqlite3.Connection:
    from datetime import datetime
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()

    ts = datetime.utcnow().isoformat()

    dim = patients[["patient_hash","age","gender","age_group",
                    "department","attending_doctor","high_readmission_risk"]]
    dim.to_sql("dim_patients", conn, if_exists="replace", index=False)
    log.info("LOAD | dim_patients: %d rows", len(dim))

    fact = patients.copy(); fact["load_ts"] = ts
    fact[["patient_id","admission_date","discharge_date","department",
          "primary_icd10","status","los_days","bp_systolic",
          "bp_diastolic","load_ts"]].to_sql(
        "fact_admissions", conn, if_exists="replace", index=False)
    log.info("LOAD | fact_admissions: %d rows", len(fact))

    claims["load_ts"] = ts
    claims.to_sql("fact_claims", conn, if_exists="replace", index=False)
    log.info("LOAD | fact_claims: %d rows", len(claims))

    conn.commit()
    return conn
