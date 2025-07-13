# execute_queries.py
import argparse
import os
from typing import List, Dict
import pandas as pd
import logging
from collections import defaultdict
from classes.postgresql_benchmark import PostgreSQLBenchmark

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_sql_file(filename: str) -> List[str]:
    """Read SQL statements from file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        statements = []
        for line in lines:
            line = line.strip()
            if line and line.endswith(';'):
                # Remove the trailing semicolon and add the statement
                statements.append(line[:-1].strip())
        
        return statements
    except Exception as e:
        logger.error(f"Error reading file '{filename}': {e}")
        return []

def load_pairs_from_csv(pairs_file: str) -> pd.DataFrame:
    """Load select-write pairs from CSV file"""
    try:
        return pd.read_csv(pairs_file)
    except Exception as e:
        logger.error(f"Error reading pairs file '{pairs_file}': {e}")
        return pd.DataFrame()

def group_pairs_by_select(pairs_df: pd.DataFrame) -> Dict[int, List[int]]:
    """Group write statements by their corresponding select statement"""
    groups = defaultdict(list)
    
    for _, pair in pairs_df.iterrows():
        select_id = pair['select_id']
        write_id = pair['write_id']
        groups[select_id].append(write_id)
    
    return dict(groups)

def validate_statements(select_statements: List[str], write_statements: List[str], groups: Dict[int, List[int]]) -> bool:
    """Validate that all referenced statement IDs exist"""
    max_select_id = len(select_statements) - 1
    max_write_id = len(write_statements) - 1
    
    for select_id, write_ids in groups.items():
        if select_id > max_select_id:
            logger.error(f"Invalid select_id {select_id}, max available: {max_select_id}")
            return False
        
        for write_id in write_ids:
            if write_id > max_write_id:
                logger.error(f"Invalid write_id {write_id}, max available: {max_write_id}")
                return False
    
    return True

def is_valid_sql_statement(statement: str) -> bool:
    """Basic validation to check if statement looks like valid SQL"""
    statement = statement.strip().upper()
    
    # Check for SELECT statements
    if statement.startswith('SELECT'):
        return True
    
    # Check for write statements
    write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'MERGE']
    for keyword in write_keywords:
        if statement.startswith(keyword):
            return True
    
    return False

def filter_valid_statements(statements: List[str]) -> List[str]:
    """Filter out invalid SQL statements"""
    valid_statements = []
    for i, stmt in enumerate(statements):
        if is_valid_sql_statement(stmt):
            valid_statements.append(stmt)
        else:
            logger.warning(f"Skipping invalid statement at index {i}: {stmt[:100]}...")
    
    return valid_statements

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Benchmarking Tool')
    parser.add_argument('--select_file', default='/app/data/workload_200k_s1.sql', 
                       help='File containing SELECT statements')
    parser.add_argument('--write_file', default='/app/data/write_workload.sql', 
                       help='File containing WRITE statements')
    parser.add_argument('--pairs_file', default='/app/data/matches.csv', 
                       help='CSV file containing pairs of SELECT and WRITE statements')
    parser.add_argument('--output', default='/app/data/benchmark_results.csv', 
                       help='Output CSV file name')
    parser.add_argument('--warmup_rounds', type=int, default=3, 
                       help='Number of warmup rounds')
    parser.add_argument('--limit_experiments', type=int, 
                       help='Limit number of experiments (for testing)')
    parser.add_argument('--limit_writes_per_select', type=int, default=5,
                       help='Limit number of write statements per select (for testing)')
    parser.add_argument('--start_from_experiment', type=int, default=0,
                       help='Start from a specific experiment number (for resuming)')
    
    args = parser.parse_args()
    
    # Database configuration from environment variables
    config = {
        'host': os.getenv('PGHOST', 'localhost'),
        'port': int(os.getenv('PGPORT', 5432)),
        'db_name': os.getenv('PGDATABASE', 'baseball'),
        'user': os.getenv('PGUSER', 'myuser'),
        'password': os.getenv('PGPASSWORD', 'mypassword'),
    }
    
    # Initialize benchmark
    benchmark = PostgreSQLBenchmark(**config)
    
    if not benchmark.connect():
        logger.error("Failed to connect to database. Exiting.")
        return 1
    
    try:
        # Load SQL statements
        logger.info("Loading SQL statements...")
        select_statements = read_sql_file(args.select_file)
        write_statements = read_sql_file(args.write_file)
        pairs_df = load_pairs_from_csv(args.pairs_file)
        
        if not select_statements or not write_statements or pairs_df.empty:
            logger.error("Failed to load required data files")
            return 1
        
        # Filter valid statements
        logger.info("Filtering valid SQL statements...")
        select_statements = filter_valid_statements(select_statements)
        write_statements = filter_valid_statements(write_statements)
        
        logger.info(f"Loaded {len(select_statements)} valid SELECT statements")
        logger.info(f"Loaded {len(write_statements)} valid WRITE statements")
        logger.info(f"Loaded {len(pairs_df)} statement pairs")
        
        # Group pairs by select statement
        groups = group_pairs_by_select(pairs_df)
        logger.info(f"Grouped into {len(groups)} select statement groups")
        
        # Warmup database
        logger.info("Starting database warmup...")
        benchmark.warmup_database(select_statements, args.warmup_rounds)
        
        # Run experiments grouped by select statement
        experiment_count = 0
        total_experiments = len(groups)
        
        if args.limit_experiments:
            total_experiments = min(total_experiments, args.limit_experiments)
        
        logger.info(f"Starting {total_experiments} experiments...")
        
        for select_id in sorted(groups.keys()):
            if args.limit_experiments and experiment_count >= args.limit_experiments:
                break
            
            if experiment_count < args.start_from_experiment:
                experiment_count += 1
                continue
            
            write_ids = groups[select_id]
            
            # Limit write statements if specified
            if args.limit_writes_per_select:
                write_ids = write_ids[:args.limit_writes_per_select]
            
            if select_id < len(select_statements):
                select_stmt = select_statements[select_id]
                
                # Get corresponding write statements
                write_stmts = []
                for write_id in write_ids:
                    if write_id < len(write_statements):
                        write_stmts.append(write_statements[write_id])
                    else:
                        logger.warning(f"Invalid write_id {write_id} for select_id {select_id}")
                
                if write_stmts:
                    experiment_id = f"exp_{select_id}"
                    
                    logger.info(f"Running experiment {experiment_count + 1}/{total_experiments}: "
                              f"select_id={select_id}, {len(write_stmts)} write statements")
                    
                    # Run experiment with current select and its write statements
                    results = benchmark.run_experiment(select_stmt, write_stmts, experiment_id)
                    benchmark.results.extend(results)
                    
                    experiment_count += 1
                    
                    # Save intermediate results every 5 experiments
                    if experiment_count % 5 == 0:
                        intermediate_file = f"{args.output}.tmp"
                        benchmark.save_results_to_csv(intermediate_file)
                        logger.info(f"Saved intermediate results to {intermediate_file}")
                else:
                    logger.warning(f"No valid write statements found for select_id {select_id}")
            else:
                logger.warning(f"Invalid select_id: {select_id}")
        
        # Save final results
        benchmark.save_results_to_csv(args.output)
        logger.info(f"Benchmark completed. {experiment_count} experiments processed.")
        
        # Print summary statistics
        logger.info("=== EXPERIMENT SUMMARY ===")
        if benchmark.results:
            configs = set(r.get('configuration', '') for r in benchmark.results)
            operations = set(r.get('operation_type', '') for r in benchmark.results)
            
            logger.info(f"Total results collected: {len(benchmark.results)}")
            logger.info(f"Configurations tested: {', '.join(configs)}")
            logger.info(f"Operation types: {', '.join(operations)}")
            
            # Calculate average execution times by configuration
            config_times = defaultdict(list)
            
            for result in benchmark.results:
                if result.get('execution_time') and not result.get('error'):
                    config = result.get('configuration', 'unknown')
                    config_times[config].append(result['execution_time'])
            
            for config, times in config_times.items():
                avg_time = sum(times) / len(times)
                logger.info(f"Average execution time for {config}: {avg_time:.4f}s ({len(times)} operations)")
        
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user. Saving partial results...")
        benchmark.save_results_to_csv(f"{args.output}.partial")
        return 130
    except Exception as e:
        logger.error(f"Error during benchmark execution: {e}", exc_info=True)
        return 1
    finally:
        benchmark.close_connection()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code != 0:
        logger.error(f"Benchmark failed with exit code {exit_code}")
    else:
        logger.info("Benchmark completed successfully")
    exit(exit_code)