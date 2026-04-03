"""
config.py — Healthcare Analytics ETL Pipeline
"""

# ── Data volumes ────────────────────────────────────────────────────────────────
N_PATIENTS = 500
N_CLAIMS   = 500
SEED       = 1

# ── Database (SQLite proxies Snowflake) ─────────────────────────────────────────
DB_PATH = "healthcare_analytics.db"

# ── Data quality ────────────────────────────────────────────────────────────────
NULL_INJECT_AGE   = 20    # rows where age is nulled
NULL_INJECT_DEPT  = 15    # rows where department is nulled

# ── Logging ─────────────────────────────────────────────────────────────────────
LOG_FORMAT   = "%(asctime)s  [%(levelname)s]  %(message)s"
LOG_DATE_FMT = "%H:%M:%S"
LOG_LEVEL    = "INFO"
