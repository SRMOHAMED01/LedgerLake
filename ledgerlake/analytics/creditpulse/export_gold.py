#!/usr/bin/env python3
"""
export_gold.py — Export LedgerLake Gold tables for CreditPulse training.

Usage:
    python export_gold.py --output s3://analytics/creditpulse/
    python export_gold.py --output ./creditpulse_data/ --format parquet
"""

import argparse
import os
import sys
from pathlib import Path

# Add project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "dbt_project"))

GOLD_TABLES = [
    "customer_360",
    "customer_monthly_spending",
    "fraud_detection_metrics",
    "loan_portfolio_analysis",
]

CREDITPULSE_FEATURE_TABLES = [
    "customer_360",
    "loan_portfolio_analysis",
]


def get_duckdb_path():
    """Get DuckDB path from environment or default."""
    return os.getenv("DUCKDB_PATH", "/tmp/duckdb/ledgerlake.duckdb")


def export_tables(output_path: str, format: str = "parquet", tables=None):
    """Export Gold tables to specified path."""
    import duckdb
    
    db_path = get_duckdb_path()
    print(f"Connecting to DuckDB: {db_path}")
    
    conn = duckdb.connect(db_path, read_only=True)
    
    # Install httpfs for S3 support
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT_HOST', 'http://localhost:9000')}';")
    conn.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ROOT_USER', 'ledgerlake')}';")
    conn.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_ROOT_PASSWORD', 'changeme')}';")
    conn.execute("SET s3_region='us-east-1';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    tables = tables or GOLD_TABLES
    
    for table in tables:
        print(f"Exporting {table}...")
        output_file = f"{output_path.rstrip('/')}/{table}.{format}"
        
        if format == "parquet":
            conn.execute(f"COPY {table} TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        elif format == "csv":
            conn.execute(f"COPY {table} TO '{output_file}' (FORMAT CSV, HEADER TRUE)")
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Verify
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  ✓ {table}: {count} rows → {output_file}")
    
    conn.close()
    print(f"\nExport complete: {len(tables)} tables to {output_path}")


def export_creditpulse_features(output_path: str):
    """Export feature tables specifically for CreditPulse training."""
    import duckdb
    
    db_path = get_duckdb_path()
    conn = duckdb.connect(db_path, read_only=True)
    
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT_HOST', 'http://localhost:9000')}';")
    conn.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ROOT_USER', 'ledgerlake')}';")
    conn.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_ROOT_PASSWORD', 'changeme')}';")
    conn.execute("SET s3_region='us-east-1';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # Create CreditPulse feature view
    print("Creating CreditPulse feature dataset...")
    conn.execute("""
        CREATE OR REPLACE VIEW creditpulse_features AS
        SELECT 
            c.customer_id,
            c.age,
            c.country,
            c.customer_segment,
            c.total_transactions,
            c.average_transaction,
            c.total_spent,
            c.credit_score,
            c.risk_level,
            c.active_loans_count,
            COALESCE(l.loan_amount, 0) AS loan_amount,
            COALESCE(l.duration_months, 0) AS duration_months,
            COALESCE(l.loan_status, 'NONE') AS loan_status,
            COALESCE(l.default_probability, 0) AS default_probability,
            COALESCE(l.risk_category, 'NONE') AS risk_category,
            CASE WHEN COALESCE(l.loan_status, '') = 'DEFAULTED' THEN 1 ELSE 0 END AS is_defaulted
        FROM customer_360 c
        LEFT JOIN loan_portfolio_analysis l ON c.customer_id = l.customer_id
    """)
    
    output_file = f"{output_path.rstrip('/')}/creditpulse_features.parquet"
    conn.execute(f"COPY creditpulse_features TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    
    count = conn.execute("SELECT COUNT(*) FROM creditpulse_features").fetchone()[0]
    print(f"  ✓ creditpulse_features: {count} rows → {output_file}")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Export LedgerLake Gold tables")
    parser.add_argument("--output", required=True, help="Output path (local or s3://)")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--tables", nargs="+", help="Specific tables to export")
    parser.add_argument("--creditpulse", action="store_true", help="Export CreditPulse feature dataset")
    
    args = parser.parse_args()
    
    if args.creditpulse:
        export_creditpulse_features(args.output)
    else:
        export_tables(args.output, args.format, args.tables)


if __name__ == "__main__":
    main()