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
COPY customer FROM '/tpch/TPC-H V3.0.1/dbgen/customer.csv' WITH (FORMAT csv, DELIMITER '|');
COPY lineitem FROM '/tpch/TPC-H V3.0.1/dbgen/lineitem.csv' WITH (FORMAT csv, DELIMITER '|');
COPY nation FROM '/tpch/TPC-H V3.0.1/dbgen/nation.csv' WITH (FORMAT csv, DELIMITER '|');
COPY orders FROM '/tpch/TPC-H V3.0.1/dbgen/orders.csv' WITH (FORMAT csv, DELIMITER '|');
COPY part FROM '/tpch/TPC-H V3.0.1/dbgen/part.csv' WITH (FORMAT csv, DELIMITER '|');
COPY partsupp FROM '/tpch/TPC-H V3.0.1/dbgen/partsupp.csv' WITH (FORMAT csv, DELIMITER '|');
COPY region FROM '/tpch/TPC-H V3.0.1/dbgen/region.csv' WITH (FORMAT csv, DELIMITER '|');
COPY supplier FROM '/tpch/TPC-H V3.0.1/dbgen/supplier.csv' WITH (FORMAT csv, DELIMITER '|');
```

# Next Steps
1. Using the write workload generator, create write workload. 
4. Without IVM Measure execution time of read and write operations.
5. Create IVM on the tables. For example:
`SELECT pgivm.create_immv('immv_query1', '$$YOUR_SIMPLE_TPCH_QUERY_HERE$$');`
6. With IVM Measure execution time of read and write operations.
7. Write scripts to automate aggregation of execution times