"""
main.py — Healthcare Analytics ETL Pipeline entry point

Pipeline:
  Extract (EHR + Claims) → Transform (clean/enrich) → Load (SQLite) → Analyse
"""
import logging, time, os
import config
from extractor  import extract_patients, extract_claims
from transformer import transform_patients, transform_claims
from loader     import load
from analytics  import run_analytics

logging.basicConfig(level=config.LOG_LEVEL,
                    format=config.LOG_FORMAT,
                    datefmt=config.LOG_DATE_FMT)

def main():
    print("="*55)
    print("  HEALTHCARE ANALYTICS ETL PIPELINE")
    print("="*55)
    t0 = time.time()

    # Extract
    patients_raw = extract_patients()
    claims_raw   = extract_claims()

    # Transform
    patients = transform_patients(patients_raw)
    claims   = transform_claims(claims_raw)

    # Load
    conn = load(patients, claims)

    # Analyse
    run_analytics(conn)
    conn.close()

    elapsed = time.time() - t0
    print(f"\n  Pipeline complete in {elapsed:.2f}s")
    print(f"  DB → {os.path.abspath(config.DB_PATH)}")
    print("\n  Done ✓")

if __name__ == "__main__":
    main()
