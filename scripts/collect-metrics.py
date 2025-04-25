#!/usr/bin/env python3
import psycopg2
from psycopg2 import OperationalError
import time
import csv
import re
import sys
import os

# ────────── Configuration ──────────
DB_PARAMS = {
    'host':     os.getenv('PGHOST', 'localhost'),
    'port':     int(os.getenv('PGPORT', 5432)),
    'dbname':   os.getenv('PGDATABASE', 'mydb'),
    'user':     os.getenv('PGUSER', 'myuser'),
    'password': os.getenv('PGPASSWORD', 'mypassword'),
}
SQL_FILE_PATH   = '/app/refresh1.sql'
OUTPUT_CSV = 'write_exec_times_explain.csv'
MAX_RETRIES = 30
RETRY_INTERVAL = 1.0  # seconds
# ───────────────────────────────────

def wait_for_postgres():
    """Keep trying to connect until Postgres is ready or we hit max retries."""
    for i in range(1, MAX_RETRIES+1):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.close()
            print(f"Postgres is up (after {i} attempt{'s' if i>1 else ''})")
            return
        except OperationalError:
            print(f"Waiting for Postgres… ({i}/{MAX_RETRIES})", file=sys.stderr)
            time.sleep(RETRY_INTERVAL)
    print(f"ERROR: could not connect to Postgres after {MAX_RETRIES} attempts", file=sys.stderr)
    sys.exit(1)

def extract_execution_time(plan_text):
    """
    Given the text of an EXPLAIN ANALYZE plan, find the
    'Execution Time: X.XXX ms' line and return X.XXX as float.
    """
    for line in plan_text.splitlines():
        # Postgres 9.6+ uses "Execution Time: 0.123 ms"
        match = re.search(r'Execution Time:\s*([\d\.]+)\s*ms', line)
        if match:
            return float(match.group(1))
    # As fallback, try older "Total runtime" style
    for line in plan_text.splitlines():
        match = re.search(r'Total runtime:\s*([\d\.]+)\s*ms', line)
        if match:
            return float(match.group(1))
    return None

def main():
    # Connect to Postgres
    wait_for_postgres()
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"ERROR: could not connect to database: {e}", file=sys.stderr)
        sys.exit(1)
    conn.autocommit = True
    cur = conn.cursor()

    # Prepare output CSV
    with open(OUTPUT_CSV, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['statement', 'exec_time_ms', 'status'])

        buffer = []
        with open(SQL_FILE_PATH, 'r') as infile:
            for line in infile:
                # Skip empty lines and comments
                stripped = line.strip()
                if not stripped or stripped.startswith('--'):
                    continue

                buffer.append(line.rstrip())
                # If line ends with semicolon, we have a full statement
                if stripped.endswith(';'):
                    stmt = ' '.join(buffer)
                    buffer.clear()

                    # Wrap in EXPLAIN ANALYZE
                    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS) {stmt}"
                    try:
                        cur.execute(explain_sql)
                        # Collect plan rows
                        plan_rows = cur.fetchall()
                        plan_text = "\n".join(row[0] for row in plan_rows)
                        exec_time = extract_execution_time(plan_text)
                        status = 'OK'
                    except Exception as e:
                        conn.rollback()
                        exec_time = None
                        status = f"ERROR: {e}"

                    # Log to CSV
                    writer.writerow([stmt, exec_time if exec_time is not None else '', status])
                    print(f"[{status}] {exec_time if exec_time is not None else 'N/A'} ms\t{stmt[:60]}...")

    cur.close()
    conn.close()
    print(f"\nDone. Results written to `{OUTPUT_CSV}`")

if __name__ == '__main__':
    main()
