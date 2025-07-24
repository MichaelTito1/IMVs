# postgresql_benchmark.py
import csv
import time
import logging
from typing import Any, Dict, List
import json

from classes.baseball_db import BaseballDB

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLBenchmark(BaseballDB):
    def __init__(self, host='localhost', port=5432, user='postgres', password='', db_name='baseball'):
        # Initialize parent class
        super().__init__(host=host, port=port, user=user, password=password, db_name=db_name)
        self.results = []
        
    def connect(self):
        """Connect to PostgreSQL database and enable pg_ivm extension"""
        # Use parent's connection method
        if not self.connect_to_target_db():
            return False
            
        # Enable pg_ivm extension
        try:
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_ivm;")
            self.conn.commit()
            logger.info("pg_ivm extension enabled")
        except Exception as e:
            logger.warning(f"Could not enable pg_ivm extension: {e}", exc_info=True)
            
        return True
    
    def warmup_database(self, select_statements: List[str], warmup_rounds: int = 3):
        """Warmup the database with simple SELECT queries"""
        logger.info(f"Starting database warmup with {warmup_rounds} rounds...")
        
        for round_num in range(warmup_rounds):
            logger.info(f"Warmup round {round_num + 1}/{warmup_rounds}")
            for idx, statement in enumerate(select_statements[:10]):  # Limit to first 10 for warmup
                try:
                    start_time = time.time()
                    self.cursor.execute(statement)
                    self.cursor.fetchall()  # Ensure all results are fetched
                    execution_time = (time.time() - start_time) * 1000
                    
                    if round_num == 0:  # Only log first round to avoid spam
                        logger.debug(f"Warmup query {idx}: {execution_time:.4f}s")
                        
                except Exception as e:
                    logger.warning(f"Warmup query {idx} failed: {e}", exc_info=True)
        
        logger.info("Database warmup completed")
    
    def execute_with_timing_and_plan(self, statement: str, statement_type: str = "SELECT", fetch_results: bool = True) -> Dict[str, Any]:
        """Execute a statement and collect timing and execution plan"""
        result = {
            'statement': statement,
            'statement_type': statement_type,
            'execution_time': 0,
            'plan': None,
            'rows_affected': 0,
            'error': None
        }
        
        try:
            # Get execution plan for SELECT statements
            if statement_type == "SELECT":
                plan_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"
                start_time = time.time()
                self.cursor.execute(plan_query)
                plan_result = self.cursor.fetchone()[0]
                execution_time = (time.time() - start_time) * 1000
                
                result['execution_time'] = execution_time
                result['plan_execution_time'] = plan_result[0]['Execution Time'] if plan_result else None
                result['plan'] = plan_result[0]['Plan'] if plan_result else None
                
                # Get actual row count
                if result['plan'] and isinstance(result['plan'], list):
                    try:
                        result['rows_affected'] = result['plan'][0]['Plan']['Actual Rows']
                    except (KeyError, IndexError):
                        pass
            else:
                # For write operations, just execute and time
                plan_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"
                start_time = time.time()
                self.cursor.execute(plan_query)
                plan_result = self.cursor.fetchone()[0]
                execution_time = (time.time() - start_time) * 1000
                
                result['execution_time'] = execution_time
                result['plan_execution_time'] = plan_result[0]['Execution Time'] if plan_result else None
                result['plan'] = plan_result[0]['Plan'] if plan_result else None
                result['triggers'] = plan_result[0]['Triggers'] if 'Triggers' in plan_result[0] else None
                result['rows_affected'] = self.cursor.rowcount
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error executing statement: {e}", exc_info=True)
            
        return result
    
    def create_materialized_view(self, select_statement: str, view_name: str) -> bool:
        """Create a materialized view from a SELECT statement"""
        try:
            # Clean the view name
            clean_view_name = f"mv_{view_name}".replace(' ', '_').replace('-', '_')
            
            mv_sql = f"CREATE MATERIALIZED VIEW {clean_view_name} AS {select_statement}"
            self.cursor.execute(mv_sql)
            self.conn.commit()
            logger.info(f"Created materialized view: {clean_view_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating materialized view {clean_view_name}: {e}", exc_info=True)
            self.conn.rollback()
            return False
    
    def create_incremental_view(self, select_statement: str, view_name: str) -> bool:
        """Create an incremental materialized view using pg_ivm"""
        try:
            # Clean the view name
            clean_view_name = f"imv_{view_name}".replace(' ', '_').replace('-', '_')
            select_statement = select_statement.replace(';', '').replace('\'', '\'\'')
            imv_sql = f"SELECT pgivm.create_immv('{clean_view_name}', '{select_statement}')"
            self.cursor.execute(imv_sql)
            self.conn.commit()
            logger.info(f"Created incremental materialized view: {clean_view_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating incremental materialized view {clean_view_name}: {e}", exc_info=True)
            logger.error(f"SQL: {imv_sql}")
            self.conn.rollback()
            return False
    
    def refresh_materialized_view(self, view_name: str) -> Dict[str, Any]:
        """Refresh a materialized view and return timing info"""
        result = {
            'operation': 'refresh_mv',
            'view_name': view_name,
            'execution_time': 0,
            'error': None
        }
        
        try:
            start_time = time.time()
            self.cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            self.conn.commit()
            result['execution_time'] = (time.time() - start_time) * 1000
            logger.debug(f"Refreshed materialized view {view_name} in {result['execution_time']:.4f}s")
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error refreshing materialized view {view_name}: {e}", exc_info=True)
            
        return result
    
    def cleanup_views(self):
        """Drop all created materialized and incremental views, and tables matching 'imv_exp_%'."""
        try:
            # Drop materialized views
            self.cursor.execute("""
                SELECT schemaname, matviewname
                FROM pg_matviews
                WHERE matviewname LIKE 'mv_%' OR matviewname LIKE 'imv_%'
            """)
            views = self.cursor.fetchall()

            for schema, view_name in views:
                try:
                    self.cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{view_name} CASCADE")
                    logger.debug(f"Dropped materialized view: {schema}.{view_name}")
                except Exception as e:
                    logger.warning(f"Error dropping materialized view {schema}.{view_name}: {e}", exc_info=True)

            # Drop tables matching 'imv_exp_%'
            self.cursor.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE tablename LIKE 'imv_exp_%'
            """)
            tables = self.cursor.fetchall()

            for schema, table_name in tables:
                try:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE")
                    logger.debug(f"Dropped table: {schema}.{table_name}")
                except Exception as e:
                    logger.warning(f"Error dropping table {schema}.{table_name}: {e}", exc_info=True)

            self.conn.commit()
            logger.info("Cleanup of views and tables completed")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error during cleanup: {e}", exc_info=True)

    
    def run_experiment(self, select_statement: str, write_statements: List[(int, str)], experiment_id: str, use_batch: bool = False) -> List[Dict[str, Any]]:
        """
        Run a complete experiment with all three configurations.
        Modified to support batch execution of write statements.
        """
        experiment_results = []
        
        logger.info(f"Starting experiment {experiment_id}")
        
        # Configuration 1: Basic setup (no views)
        logger.info(f"Experiment {experiment_id} - Basic setup")
        try:
            # self.conn.begin()
            
            # Execute SELECT statement
            select_result = self.execute_with_timing_and_plan(select_statement, "SELECT")
            select_result.update({
                'experiment_id': experiment_id,
                'configuration': 'basic',
                'operation_type': 'select'
            })
            experiment_results.append(select_result)
            
            # Execute WRITE statements
            for write_id, write_statement in write_ops:
                logger.info(f"Executing write statement {write_id + 1}/{len(write_ops)}")
                write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                write_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'basic',
                    'operation_type': 'write',
                    'write_index': write_id,
                    'batch_mode': use_batch,
                    'batch_size': batch_size
                })
                experiment_results.append(write_result)
            
            self.conn.commit()  # Commit changes
            
        except Exception as e:
            logger.error(f"Error in basic setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        # Configuration 2: Materialized View setup
        logger.info(f"Experiment {experiment_id} - Materialized View setup")
        try:
            view_name = f"mv_{experiment_id}"
            
            if self.create_materialized_view(select_statement, experiment_id):
                # Execute SELECT on materialized view
                mv_select = f"SELECT * FROM {view_name}"
                select_result = self.execute_with_timing_and_plan(mv_select, "SELECT")
                select_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'materialized_view',
                    'operation_type': 'select'
                })
                experiment_results.append(select_result)
                
                # Execute WRITE statements with MV refresh
                for write_id, write_statement in write_ops:
                    # Execute write
                    write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                    write_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'materialized_view',
                        'operation_type': 'write',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(write_result)
                    
                    # Refresh materialized view
                    refresh_result = self.refresh_materialized_view(view_name)
                    refresh_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'materialized_view',
                        'operation_type': 'refresh',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(refresh_result)
                
                # Cleanup MV
                self.cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error in materialized view setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        # Configuration 3: Incremental Materialized View setup
        logger.info(f"Experiment {experiment_id} - Incremental Materialized View setup")
        try:
            # self.conn.begin()
            imv_name = f"imv_{experiment_id}"
            
            if self.create_incremental_view(select_statement, experiment_id):
                # Execute SELECT on incremental view
                imv_select = f"SELECT * FROM {imv_name}"
                select_result = self.execute_with_timing_and_plan(imv_select, "SELECT")
                select_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'incremental_view',
                    'operation_type': 'select'
                })
                experiment_results.append(select_result)
                
                # Execute WRITE statements (IMV updates automatically)
                for write_id, write_statement in write_ops:
                    write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                    write_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'incremental_view',
                        'operation_type': 'write',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(write_result)
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error in incremental view setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        logger.info(f"Completed experiment {experiment_id}")
        return experiment_results
    
    def save_results_to_csv(self, filename: str = "benchmark_results.csv"):
        """Save all results to a CSV file"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        # Flatten the results for CSV export
        flattened_results = []
        
        for result in self.results:
            logger.info(f"Flattening result: {list(result.keys())}")
            flat_result = {
                'experiment_id': result.get('experiment_id', ''),
                'configuration': result.get('configuration', ''),
                'operation_type': result.get('operation_type', ''),
                'write_index': result.get('write_index', ''),
                'execution_time': result.get('execution_time', 0),
                'plan_execution_time': result.get('plan_execution_time', 0),
                'rows_affected': result.get('rows_affected', 0),
                'error': result.get('error', ''),
                'statement': result.get('statement', '')[:200],  # Truncate for CSV
            }
            
            # Add plan information if available
            if result.get('plan'):
                try:
                    plan = result['plan']
                    
                    if isinstance(plan, dict):
                        flat_result['plan_total_cost'] = plan['Total Cost'] if 'Total Cost' in plan else 0
                        flat_result['plan_actual_time'] = plan['Actual Total Time'] if 'Actual Total Time' in plan else 0
                        flat_result['plan_node_type'] = plan['Node Type'] if 'Node Type' in plan else ''
                        flat_result['plan'] = json.dumps(plan, ensure_ascii=False)  # Store full plan as JSON
                except Exception as e:
                    logger.warning(f"Error processing plan data: {e}", exc_info=True)
            
            flat_result['triggers'] = json.dumps(result.get('triggers', []), ensure_ascii=False) if result.get('triggers') else ''
            flattened_results.append(flat_result)
        
        # Write to CSV
        if flattened_results:
            fieldnames = flattened_results[0].keys()
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_results)
            
            logger.info(f"Results saved to {filename} ({len(flattened_results)} rows)")
        else:
            logger.warning("No flattened results to save")
    
    def close_connection(self):
        """Close database connection"""
        self.cleanup_views()
        
        # Use parent's close method
        super().close_connection()