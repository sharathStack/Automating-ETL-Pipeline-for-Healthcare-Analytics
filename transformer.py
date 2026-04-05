"""
transformer.py — Clean, enrich, and standardise raw records

Mirrors an ADF Transform / Snowflake data-prep step:
  - Null imputation
  - Blood pressure parsing → systolic/diastolic columns
  - Age group binning
  - Readmission risk flag
  - SHA-256 patient anonymisation
  - Claim denial flag + net reimbursement
"""

import hashlib
import logging
import numpy as np
import pandas as pd

log = logging.getLogger("Transformer")


def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM | Cleaning %d patient records...", len(df))
    df = df.copy()

    # Impute nulls
    df["age"]        = df["age"].fillna(df["age"].median()).astype(int)
    df["department"] = df["department"].fillna("Unknown")

    # Parse blood pressure
    df["blood_pressure"] = df["blood_pressure"].replace("N/A", np.nan)
    df["bp_systolic"]  = df["blood_pressure"].str.split("/").str[0].astype(float)
    df["bp_diastolic"] = df["blood_pressure"].str.split("/").str[1].astype(float)
    df.drop(columns=["blood_pressure"], inplace=True)

    # Age group
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 18, 35, 50, 65, 120],
        labels=["<18", "18-35", "36-50", "51-65", "65+"],
    )

    # Readmission risk: short LOS + discharged + elderly
    df["high_readmission_risk"] = (
        (df["los_days"] < 2) &
        (df["status"] == "Discharged") &
        (df["age"] > 65)
    ).astype(int)

    # SHA-256 anonymisation of patient ID
    df["patient_hash"] = df["patient_id"].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:16]
    )

    df.drop_duplicates(subset="patient_id", inplace=True)
    log.info("TRANSFORM | Patients ready: %d rows | Nulls remaining: %d",
             len(df), df.isnull().sum().sum())
    return df


def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM | Enriching %d claims...", len(df))
    df = df.copy()

    df["denial_flag"]        = (df["claim_status"] == "Denied").astype(int)
    df["net_reimbursement"]  = (df["approved_amount"] - df["billed_amount"] * 0.05).clip(lower=0)
    df["claim_year_month"]   = df["claim_date"].dt.to_period("M").astype(str)
    df["high_value_flag"]    = (df["billed_amount"] > df["billed_amount"].quantile(0.90)).astype(int)

    log.info("TRANSFORM | Claims ready: %d rows | Denial rate: %.1f%%",
             len(df), df["denial_flag"].mean() * 100)
    return df
