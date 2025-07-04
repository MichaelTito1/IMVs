1. clone pg_ivm extension into directory
1. run docker compose up
3. install tpc-h tools using [this guide](https://github.com/FlashSQL/mijin-til/blob/master/benchmark/how-to-install-tpch-for-pgsql.md)

To set scale factor use: `./dbgen -s 0.01`

Truncate extra pipe at the end of `.tbl` files generated from dbgen:
```
for file in *.tbl; do
    sed 's/|$//' "$file" > "${file%.tbl}.csv"
done
```

# Importing Data
1. create table schemas from [`tpch-schema.sql`](https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-schema.sql)
2. Verify that tables are now imported: `mydb-# \dt`
3. Import the data:
```
COPY customer FROM '/tpch/TPCH-V3.0.1/dbgen/customer.csv' WITH (FORMAT csv, DELIMITER '|');
COPY lineitem FROM '/tpch/TPCH-V3.0.1/dbgen/lineitem.csv' WITH (FORMAT csv, DELIMITER '|');
COPY nation FROM '/tpch/TPCH-V3.0.1/dbgen/nation.csv' WITH (FORMAT csv, DELIMITER '|');
COPY orders FROM '/tpch/TPCH-V3.0.1/dbgen/orders.csv' WITH (FORMAT csv, DELIMITER '|');
COPY part FROM '/tpch/TPCH-V3.0.1/dbgen/part.csv' WITH (FORMAT csv, DELIMITER '|');
COPY partsupp FROM '/tpch/TPCH-V3.0.1/dbgen/partsupp.csv' WITH (FORMAT csv, DELIMITER '|');
COPY region FROM '/tpch/TPCH-V3.0.1/dbgen/region.csv' WITH (FORMAT csv, DELIMITER '|');
COPY supplier FROM '/tpch/TPCH-V3.0.1/dbgen/supplier.csv' WITH (FORMAT csv, DELIMITER '|');
```

# Collecting data about effect of IMVs on write performance 
## Creating write SQL statements
1. Using the write workload generator, create write workload. 
    > maybe use TPC-H refresh functions? RF1 for INSERT and RF2 for DELETE? Run the following:
    In powershell: `./tpch/TPCH-V3.0.1/dbgen/dbgen -s 1 -U 1`
    In WSL: `python ./scripts/tpch-refresh-to-sql.py --update-dir /mnt/d/TU-Darmstadt/4/IMVs/tpch/TPCH-V3.0.1/dbgen --stream 1 --output refresh1.sql`

## Running Write SQL statements and collecting metrics:
1. go inside `bench` container, and do:
> `python3 /app/scripts/collect-metrics.py`

## Managing IMV for all tables in the database:
```
# To create one IMV per table:
python manage-imvs.py create

# To drop them again:
python manage-imvs.py drop
```

After creating IMVs, rerun the write SQL statements

# How to configure PostgreSQL to capture execution plans of IMV triggers?
[source](https://chatgpt.com/share/6860535e-88d8-800b-9a72-44f8c97207fa). Use the `auto_explain` extension with nested-statement logging:

1. **Load and configure** (in `postgresql.conf` or per-session)

   ```sql
   -- make sure auto_explain is in shared_preload_libraries
   shared_preload_libraries = 'auto_explain'
   ```
2. **Session settings** (or in `postgresql.conf`)

   ```sql
   -- log every plan
   SET auto_explain.log_min_duration = 0;                 
   -- include actual times
   SET auto_explain.log_analyze     = true;              
   -- show the full plan
   SET auto_explain.log_verbose     = true;              
   -- **key**: log nested statements (inside functions/triggers)
   SET auto_explain.log_nested_statements = true;        
   -- send log output to your client (psql) rather than to server log
   SET client_min_messages = log;                        
   ```
3. **Run your INSERT** as usual. 

# Next Steps [PENDING REVIEW]
4. Without IVM Measure execution time of read and write operations.
5. Create IVM on the tables. For example:
`SELECT pgivm.create_immv('immv_query1', '$$YOUR_SIMPLE_TPCH_QUERY_HERE$$');`
6. With IVM Measure execution time of read and write operations.
7. Write scripts to automate aggregation of execution times

# generating read workload:
- download needed dataset files from cluster
- generate workload 
> `python src/ldb_wlgenerator/generate_workload.py --data_dir /mnt/d/TU-Darmstadt/4/data/baseball --dataset_statistics_dir /mnt/d/TU-Darmstadt/4/data/baseball/database_statistics/ --workload_dir /mnt/d/TU-Darmstadt/4/data/baseball/workloads/`

# generating write workload:
- from queryPotter, run `python workload_gen/setup.py`

- From this repo, run `read_write_sql_matcher.py` as follows to get ~100k rows with 5 writes per select statement:
`python scripts/read_write_sql_matcher.py --write_file ../data/baseball/write_workload.sql --select_file ../data/baseball/workloads/baseball_scaled10/imv_test_workload.sql --max-writes-per-table 10 --max-matches-per-select 5 --max-total-matches 100000`
