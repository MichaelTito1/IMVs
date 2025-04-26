#!/usr/bin/env python3
import os
import sys
import argparse
import psycopg2
from utils import wait_for_postgres

def get_db_params():
    return {
        'host':     os.getenv('PGHOST', 'postgres'),
        'port':     int(os.getenv('PGPORT', 5432)),
        'dbname':   os.getenv('PGDATABASE', 'mydb'),
        'user':     os.getenv('PGUSER', 'myuser'),
        'password': os.getenv('PGPASSWORD', 'mypassword'),
    }

def list_tables(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type   = 'BASE TABLE';
    """)
    return [row[0] for row in cur.fetchall()]

def create_imv(cur, table):
    viewname = f"imv_{table}"
    sql = f"SELECT pgivm.create_immv('{viewname}', 'SELECT * FROM {table}');"
    cur.execute(sql)

def drop_imv(cur, table):
    viewname = f"{table}"
    # if the view name starts with "imv_", we need to drop the IMV first
    if viewname.startswith("imv_"):
        sql = f"DROP TABLE {viewname};"
        cur.execute(sql)

def main():
    p = argparse.ArgumentParser(
        description="Create or drop pg_ivm IMVs for every public table"
    )
    p.add_argument('action', choices=['create','drop'],
                   help="Whether to create IMVs or drop them")
    args = p.parse_args()

    dbparams = get_db_params()
    # Optionally wait for DB readiness:
    wait_for_postgres(dbparams)

    try:
        conn = psycopg2.connect(**dbparams)
    except Exception as e:
        print(f"ERROR: could not connect to database: {e}", file=sys.stderr)
        sys.exit(1)
    conn.autocommit = True
    cur = conn.cursor()

    tables = list_tables(cur)
    if not tables:
        print("No tables found in public schema.", file=sys.stderr)
        sys.exit(1)

    for table in tables:
        try:
            if args.action == 'create':
                create_imv(cur, table)
                print(f"[OK] created IMV imv_{table}")
            else:
                drop_imv(cur, table)
                print(f"[OK] dropped IMV {table}")
        except Exception as e:
            print(f"[ERROR] table={table}: {e}", file=sys.stderr)

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
