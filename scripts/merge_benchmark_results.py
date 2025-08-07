#!/usr/bin/env python3
"""
Script to merge benchmark_results.csv with write_workload.csv.
This creates a comprehensive dataset combining execution metrics with original statement metadata.
"""

import pandas as pd
import argparse
import os
from typing import Optional

def merge_benchmark_with_workload(
    benchmark_file: str,
    write_workload_file: str, 
    output_file: str,
    write_sql_file: Optional[str] = None
) -> None:
    """
    Merge benchmark results with write workload data.
    
    Args:
        benchmark_file: Path to benchmark_results.csv
        write_workload_file: Path to write_workload.csv
        output_file: Path for the merged output CSV
        write_sql_file: Optional path to write_workload.sql for additional validation
    """
    
    print(f"Reading benchmark results from: {benchmark_file}")
    benchmark_df = pd.read_csv(benchmark_file)
    
    print(f"Reading write workload from: {write_workload_file}")
    workload_df = pd.read_csv(write_workload_file)
    
    print(f"Benchmark results shape: {benchmark_df.shape}")
    print(f"Write workload shape: {workload_df.shape}")
    
    # Reset index of workload to get row numbers (matching write_index in benchmark)
    workload_df = workload_df.reset_index()
    workload_df.rename(columns={'index': 'workload_index'}, inplace=True)
    
    # Filter benchmark results to only include write operations
    write_results = benchmark_df[benchmark_df['operation_type'] == 'write'].copy()
    print(f"Write operations in benchmark: {len(write_results)}")
    
    # Merge on write_index from benchmark matching workload_index
    merged_df = write_results.merge(
        workload_df, 
        left_on='write_index', 
        right_on='write_idx', 
        how='left',
        suffixes=('_benchmark', '_workload')
    )
    
    print(f"Merged dataset shape: {merged_df.shape}")
    
    # Reorder columns for better readability
    column_order = [
        # Core identification columns
        'experiment_id', 'configuration', 'operation_type', 'write_index', 'workload_index',
        
        # Performance metrics (raw)
        'execution_time', 'plan_execution_time', 'rows_affected', 'rows_inserted',
        
        # Batch configuration
        'batch_mode', 'batch_size', 'error',
        
        # SQL workload information
        'query_type', 'sql', 'template_id', 'parameters',  # from workload
        'statement_benchmark', 'statement_workload',  # SQL statements from both sources
        
        # Query execution plan details
        'plan_total_cost', 'plan_actual_time', 'plan_node_type', 'plan', 'triggers',
        
        # Query complexity metrics
        'num_joins', 'num_scans', 'num_aggregations', 
        'start_table', 'join_tables', 'write_table',
        
        # Calculated performance metrics (will be added by analysis scripts)
        'total_mv_time', 'immv_time', 'write_time', 'refresh_time', 'speedup',
        
        # Additional analysis columns
        'cardinality_category', 'performance_category', 'complexity_category'
    ]
    
    # Only include columns that exist in the merged dataframe
    available_columns = [col for col in column_order if col in merged_df.columns]
    remaining_columns = [col for col in merged_df.columns if col not in available_columns]
    final_column_order = available_columns + remaining_columns
    
    # merged_df = merged_df[final_column_order]
    merged_df = merged_df[available_columns]
    merged_df.drop(columns=['join_tables_benchmark', 'write_table_benchmark', 'statement', 'workload_index', 'query_type_workload',
     'num_joins_benchmark', 'num_scans_benchmark', 'num_aggregations_benchmark'], inplace=True)

    # Validate the merge quality
    unmatched_writes = write_results[~write_results['write_index'].isin(workload_df['workload_index'])]
    if not unmatched_writes.empty:
        print(f"Warning: {len(unmatched_writes)} write operations couldn't be matched with workload data")
        print("Unmatched write indices:", unmatched_writes['write_index'].tolist()[:10])
    
    # Optional validation against SQL file
    if write_sql_file and os.path.exists(write_sql_file):
        print(f"Validating against SQL file: {write_sql_file}")
        with open(write_sql_file, 'r', encoding='utf-8') as f:
            sql_statements = [line.strip() for line in f if line.strip()]
        
        print(f"SQL file contains {len(sql_statements)} statements")
        
        # Check if counts match
        if len(sql_statements) != len(workload_df):
            print(f"Warning: SQL file has {len(sql_statements)} statements but workload CSV has {len(workload_df)} rows")
    
    # Add summary statistics
    print("\n=== MERGE SUMMARY ===")
    print(f"Total benchmark write operations: {len(write_results)}")
    print(f"Total workload entries: {len(workload_df)}")
    print(f"Successfully merged: {len(merged_df)}")
    print(f"Merge success rate: {len(merged_df)/len(write_results)*100:.1f}%")
    
    # Query type distribution
    if 'query_type' in merged_df.columns:
        print("\nQuery type distribution:")
        print(merged_df['query_type'].value_counts())
    
    # Execution time statistics
    if 'execution_time' in merged_df.columns:
        print(f"\nExecution time statistics:")
        print(f"Mean: {merged_df['execution_time'].mean():.3f}s")
        print(f"Median: {merged_df['execution_time'].median():.3f}s") 
        print(f"Min: {merged_df['execution_time'].min():.3f}s")
        print(f"Max: {merged_df['execution_time'].max():.3f}s")
    
    # Save merged data
    print(f"\nSaving merged data to: {output_file}")
    merged_df.to_csv(output_file, index=False)
    print("Merge completed successfully!")

def main():
    """Main function to handle command line arguments and execute merge."""
    parser = argparse.ArgumentParser(description='Merge benchmark results with write workload data')
    parser.add_argument('--benchmark_file', default='/app/data/benchmark_results.csv',
                       help='Path to benchmark_results.csv file')
    parser.add_argument('--write_workload_file', default='/app/data/write_workload.csv',
                       help='Path to write_workload.csv file')
    parser.add_argument('--write_sql_file', default='/app/data/write_workload.sql',
                       help='Optional path to write_workload.sql file for validation')
    parser.add_argument('--output', default='/app/data/merged_benchmark_results.csv',
                       help='Output file for merged data')
    
    args = parser.parse_args()
    
    # Validate input files exist
    if not os.path.exists(args.benchmark_file):
        print(f"Error: Benchmark file not found: {args.benchmark_file}")
        return 1
        
    if not os.path.exists(args.write_workload_file):
        print(f"Error: Write workload file not found: {args.write_workload_file}")
        return 1
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        merge_benchmark_with_workload(
            args.benchmark_file,
            args.write_workload_file,
            args.output,
            args.write_sql_file if os.path.exists(args.write_sql_file) else None
        )
        return 0
    except Exception as e:
        print(f"Error during merge: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
