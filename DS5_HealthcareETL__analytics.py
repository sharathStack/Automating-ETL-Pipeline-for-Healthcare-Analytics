"""
analytics.py — SQL analytical queries on the loaded warehouse
"""
import sqlite3, pandas as pd

QUERIES = {
    "Top Departments by Admissions": """
        SELECT department,
               COUNT(*)              AS admissions,
               ROUND(AVG(los_days),1)AS avg_los,
               SUM(high_readmission_risk) AS readmission_flags
        FROM fact_admissions
        GROUP BY department
        ORDER BY admissions DESC LIMIT 5;
    """,
    "Payer Summary": """
        SELECT payer,
               COUNT(*)                                        AS claims,
               ROUND(SUM(billed_amount),2)                    AS total_billed,
               ROUND(SUM(approved_amount),2)                  AS total_approved,
               ROUND(100.0*SUM(denial_flag)/COUNT(*),1)       AS denial_rate_pct,
               ROUND(AVG(net_reimbursement),2)                AS avg_net_reimb
        FROM fact_claims
        GROUP BY payer
        ORDER BY total_billed DESC;
    """,
    "High-Value Claim Denial Rate": """
        SELECT high_value_flag,
               COUNT(*)                                  AS claims,
               ROUND(100.0*SUM(denial_flag)/COUNT(*),1)  AS denial_rate_pct
        FROM fact_claims
        GROUP BY high_value_flag;
    """,
    "Monthly Claim Volume": """
        SELECT claim_year_month,
               COUNT(*)                       AS claims,
               ROUND(SUM(billed_amount)/1e3,1)AS billed_k
        FROM fact_claims
        GROUP BY claim_year_month
        ORDER BY claim_year_month
        LIMIT 6;
    """,
}

def run_analytics(conn: sqlite3.Connection) -> None:
    print("\n" + "="*55)
    print("  HEALTHCARE ANALYTICS — QUERY RESULTS")
    print("="*55)
    for title, sql in QUERIES.items():
        df = pd.read_sql(sql, conn)
        print(f"\n  ── {title} ──")
        print(df.to_string(index=False))
