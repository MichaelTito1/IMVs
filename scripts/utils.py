import sys
import psycopg2
from psycopg2 import OperationalError
import time

def wait_for_postgres(params, retries=30, interval=1.0):
    for i in range(retries):
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            print(f"Postgres is up (after {i} attempt{'s' if i>1 else ''})")
            return
        except OperationalError:
            print(f"Waiting for Postgres… ({i+1}/{retries})", file=sys.stderr)
            time.sleep(interval)
    print(f"ERROR: could not connect to Postgres after {retries} attempts", file=sys.stderr)
    sys.exit(1)