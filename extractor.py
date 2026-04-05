"""
extractor.py — Simulate extraction from EHR and insurance claim systems

Mirrors an Azure Data Factory (ADF) Extract step:
  Source 1: Electronic Health Record (EHR) system → patient records
  Source 2: Insurance claims processing system → claims data
"""

import numpy as np
import pandas as pd
import logging
import config

log = logging.getLogger("Extractor")


def extract_patients() -> pd.DataFrame:
    log.info("EXTRACT | Pulling %d patient records from EHR...", config.N_PATIENTS)
    np.random.seed(config.SEED)
    n = config.N_PATIENTS

    icd10  = ["I10", "E11.9", "J06.9", "M54.5", "K21.0", "Z12.31", "F41.1"]
    depts  = ["Cardiology", "Endocrinology", "General Medicine",
              "Orthopaedics", "Gastroenterology", "Oncology", "Psychiatry"]
    status = ["Admitted", "Discharged", "Outpatient", "Emergency"]

    dispatch = pd.to_datetime("2024-01-01") + pd.to_timedelta(
        np.random.randint(0, 365, n), unit="D")
    los = np.random.randint(0, 30, n)

    df = pd.DataFrame({
        "patient_id":       [f"P{str(i).zfill(5)}" for i in range(1, n + 1)],
        "age":              np.random.randint(18, 90, n).astype(float),
        "gender":           np.random.choice(["M", "F", "Other"], n, p=[0.49, 0.49, 0.02]),
        "admission_date":   dispatch,
        "discharge_date":   dispatch + pd.to_timedelta(los, unit="D"),
        "department":       np.random.choice(depts, n),
        "primary_icd10":    np.random.choice(icd10, n),
        "status":           np.random.choice(status, n, p=[0.30, 0.40, 0.20, 0.10]),
        "attending_doctor": np.random.choice([f"DR{str(i).zfill(3)}" for i in range(1, 51)], n),
        "los_days":         los,
        "blood_pressure":   np.where(np.random.rand(n) < 0.05, "N/A",
                                     [f"{np.random.randint(90,180)}/{np.random.randint(60,110)}"
                                      for _ in range(n)]),
    })

    # Inject intentional nulls for data quality testing
    df.loc[np.random.choice(df.index, config.NULL_INJECT_AGE, replace=False), "age"] = np.nan
    df.loc[np.random.choice(df.index, config.NULL_INJECT_DEPT, replace=False), "department"] = None

    log.info("EXTRACT | %d rows | %d nulls injected", len(df), df.isnull().sum().sum())
    return df


def extract_claims() -> pd.DataFrame:
    log.info("EXTRACT | Pulling %d insurance claims...", config.N_CLAIMS)
    np.random.seed(config.SEED + 1)
    n = config.N_CLAIMS

    payers  = ["BlueCross", "Aetna", "UnitedHealth", "Cigna", "Medicaid", "Self-Pay"]
    statuses= ["Approved", "Denied", "Pending", "Partial"]
    cpt     = ["99213", "99214", "93000", "71046", "80053", "99285"]

    billed  = np.round(np.random.uniform(200, 50_000, n), 2)
    c_status= np.random.choice(statuses, n, p=[0.60, 0.15, 0.15, 0.10])
    approved = np.where(c_status == "Approved", billed * np.random.uniform(0.7, 1.0, n),
               np.where(c_status == "Partial",  billed * 0.50, 0.0))

    df = pd.DataFrame({
        "claim_id":        [f"CLM{str(i).zfill(6)}" for i in range(1, n + 1)],
        "patient_id":      [f"P{str(np.random.randint(1, n+1)).zfill(5)}" for _ in range(n)],
        "claim_date":      pd.to_datetime("2024-01-01") + pd.to_timedelta(
                               np.random.randint(0, 365, n), unit="D"),
        "billed_amount":   billed,
        "approved_amount": np.round(approved, 2),
        "payer":           np.random.choice(payers, n, p=[0.20,0.15,0.20,0.15,0.20,0.10]),
        "claim_status":    c_status,
        "cpt_code":        np.random.choice(cpt, n),
    })

    log.info("EXTRACT | %d claims extracted", len(df))
    return df
