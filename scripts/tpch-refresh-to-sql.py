#!/usr/bin/env python3
"""
Script to convert TPC-H RF1/RF2 refresh data to SQL statements.
This script reads the TPC-H dbgen generated refresh files and
converts them into SQL INSERT and DELETE statements.
"""

import os
import sys
import argparse

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert TPC-H refresh data to SQL.')
    parser.add_argument('--update-dir', required=True, help='Directory containing update files')
    parser.add_argument('--stream', type=int, required=True, help='Update stream number')
    parser.add_argument('--output', required=True, help='Output SQL file')
    parser.add_argument('--delimiter', default='|', help='Delimiter used in the data files')
    return parser.parse_args()

def process_orders_insert(file_path, delimiter, output_file):
    """Process orders.tbl.uN file and create INSERT statements."""
    with open(file_path, 'r') as f:
        print(f"-- RF1: Processing inserts for ORDERS from {os.path.basename(file_path)}", file=output_file)
        for line in f:
            values = line.strip().split(delimiter)
            # Remove the last empty element due to trailing delimiter
            if values[-1] == '':
                values = values[:-1]
                
            # Format the SQL INSERT statement for ORDERS
            # o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
            sql = f"INSERT INTO ORDERS VALUES ({values[0]}, {values[1]}, '{values[2]}', {values[3]}, '{values[4]}', '{values[5]}', '{values[6]}', {values[7]}, '{values[8]}');"
            print(sql, file=output_file)
        print(file=output_file)

def process_lineitem_insert(file_path, delimiter, output_file):
    """Process lineitem.tbl.uN file and create INSERT statements."""
    with open(file_path, 'r') as f:
        print(f"-- RF1: Processing inserts for LINEITEM from {os.path.basename(file_path)}", file=output_file)
        for line in f:
            values = line.strip().split(delimiter)
            # Remove the last empty element due to trailing delimiter
            if values[-1] == '':
                values = values[:-1]
                
            # Format the SQL INSERT statement for LINEITEM
            # l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, 
            # l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            sql = f"INSERT INTO LINEITEM VALUES ({values[0]}, {values[1]}, {values[2]}, {values[3]}, {values[4]}, {values[5]}, {values[6]}, {values[7]}, '{values[8]}', '{values[9]}', '{values[10]}', '{values[11]}', '{values[12]}', '{values[13]}', '{values[14]}', '{values[15]}');"
            print(sql, file=output_file)
        print(file=output_file)

def process_delete(file_path, output_file):
    """Process delete.N file and create DELETE statements."""
    with open(file_path, 'r') as f:
        print(f"-- RF2: Processing deletes from {os.path.basename(file_path)}", file=output_file)
        for line in f:
            orderkey = line.strip()
            if orderkey:
                # Delete from LINEITEM first (foreign key constraint)
                sql_lineitem = f"DELETE FROM LINEITEM WHERE L_ORDERKEY = {orderkey};"
                print(sql_lineitem, file=output_file)
                
                # Then delete from ORDERS
                sql_orders = f"DELETE FROM ORDERS WHERE O_ORDERKEY = {orderkey};"
                print(sql_orders, file=output_file)
        print(file=output_file)

def main():
    args = parse_args()
    
    # Construct file paths
    orders_file = os.path.join(args.update_dir, f"orders.tbl.u{args.stream}")
    lineitem_file = os.path.join(args.update_dir, f"lineitem.tbl.u{args.stream}")
    delete_file = os.path.join(args.update_dir, f"delete.{args.stream}")
    
    # Check if files exist
    if not os.path.exists(orders_file):
        print(f"Error: Orders file {orders_file} not found", file=sys.stderr)
        return 1
    
    if not os.path.exists(lineitem_file):
        print(f"Error: Lineitem file {lineitem_file} not found", file=sys.stderr)
        return 1
    
    if not os.path.exists(delete_file):
        print(f"Error: Delete file {delete_file} not found", file=sys.stderr)
        return 1
    
    # Process files and generate SQL
    with open(args.output, 'w') as output_file:
        # Add transaction control
        print("BEGIN TRANSACTION;", file=output_file)
        print(file=output_file)
        
        # Process RF1 (inserts)
        process_orders_insert(orders_file, args.delimiter, output_file)
        process_lineitem_insert(lineitem_file, args.delimiter, output_file)
        
        # Process RF2 (deletes)
        process_delete(delete_file, output_file)
        
        # Commit transaction
        print("COMMIT;", file=output_file)
    
    print(f"SQL statements written to {args.output}")
    return 0

if __name__ == "__main__":
    sys.exit(main())