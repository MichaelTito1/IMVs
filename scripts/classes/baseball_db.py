import csv
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseballDB:
    def __init__(self, host='localhost', port=5432, user='postgres', password='', db_name='baseball_db'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.conn = None
        self.cursor = None
    
    def connect_to_postgres(self):
        """Connect to PostgreSQL server (not specific database)"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database='postgres'  # Connect to default postgres database
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL server")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def database_exists(self):
        """Check if the target database exists"""
        try:
            self.cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,))
            return self.cursor.fetchone() is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking database existence: {e}")
            return False
    
    def create_database(self):
        """Create the database if it doesn't exist"""
        if self.database_exists():
            logger.info(f"Database '{self.db_name}' already exists")
            return True
        
        try:
            self.cursor.execute(f'CREATE DATABASE "{self.db_name}"')
            logger.info(f"Database '{self.db_name}' created successfully")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def connect_to_target_db(self):
        """Connect to the target database"""
        try:
            if self.conn:
                self.conn.close()
            
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database '{self.db_name}'")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database '{self.db_name}': {e}")
            return False
    
    def execute_schema_file(self, schema_file_path):
        """Execute SQL commands from schema file"""
        if not os.path.exists(schema_file_path):
            logger.error(f"Schema file not found: {schema_file_path}")
            return False
        
        try:
            with open(schema_file_path, 'r', encoding='utf-8') as file:
                schema_sql = file.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    self.cursor.execute(statement)
            
            self.conn.commit()
            logger.info(f"Schema from '{schema_file_path}' executed successfully")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Error executing schema: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Error reading schema file: {e}")
            return False
    
    def get_table_names(self):
        """Get list of table names in the database"""
        try:
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in self.cursor.fetchall()]
            return tables
        except psycopg2.Error as e:
            logger.error(f"Error getting table names: {e}")
            return []

    def import_csv_to_table(self, csv_path, table_name: str):
        """Import CSV data into specified table"""
        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found: {csv_path}")
            return False
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(
                csv_path,
                sep='\t',  # Assuming tab-separated values
                encoding='utf-8',
                dtype=str,  # Read all columns as strings to avoid type issues
                na_values=['<!NULL-?>'],
                keep_default_na=False,      # do _not_ treat "NA", "", etc. as missing
                quoting=csv.QUOTE_NONE,
                comment=None
            ).head(500)  # Limit to 500 rows
            
            if df.empty:
                logger.warning(f"CSV file is empty: {csv_path}")
                return True
            
            # Get table columns
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            
            db_columns = [row[0] for row in self.cursor.fetchall()]
            
            if not db_columns:
                logger.error(f"Table '{table_name}' not found or has no columns")
                return False
            
            # Match CSV columns to database columns (case-insensitive)
            csv_columns = df.columns.tolist()
            column_mapping = {}
            
            for db_col in db_columns:
                for csv_col in csv_columns:
                    if db_col.lower() == csv_col.lower():
                        column_mapping[db_col] = csv_col
                        break
            
            if not column_mapping:
                logger.warning(f"No matching columns found between CSV and table '{table_name}'")
                return False
            
            # Prepare data for insertion
            matched_columns = list(column_mapping.keys())
            df_subset = df[list(column_mapping.values())]
            df_subset.columns = matched_columns
            
            # Handle NaN values
            df_subset = df_subset.where(pd.notnull(df_subset), None)
            
            # Create INSERT statement
            placeholders = ', '.join(['%s'] * len(matched_columns))
            columns_str = ', '.join([f'"{col}"' for col in matched_columns])
            insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
            
            # Insert data in batches
            batch_size = 1000
            total_rows = len(df_subset)
            
            for i in range(0, total_rows, batch_size):
                batch = df_subset.iloc[i:i+batch_size]
                values = [tuple(row) for row in batch.values]
                self.cursor.executemany(insert_sql, values)
                
                if i + batch_size < total_rows:
                    logger.info(f"Inserted {i + batch_size}/{total_rows} rows into '{table_name}'")
            
            self.conn.commit()
            logger.info(f"Successfully imported {total_rows} rows from '{csv_path}' to '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error importing CSV '{csv_path}' to table '{table_name}': {e}", exc_info=True)
            self.conn.rollback()
            return False
    
    def import_all_csvs(self, csv_directory):
        """Import all CSV files from directory to matching tables"""
        csv_dir = Path(csv_directory)
        
        if not csv_dir.exists():
            logger.error(f"CSV directory not found: {csv_directory}")
            return False
        
        # Get all CSV files
        csv_files = list(csv_dir.glob('*.csv'))
        
        if not csv_files:
            logger.warning(f"No CSV files found in directory: {csv_directory}")
            return True
        
        # Get database tables
        tables = self.get_table_names()
        
        imported_count = 0
        
        for csv_file in csv_files:
            # Try to match CSV filename to table name
            filename = csv_file.stem.lower()  # filename without extension
            
            # Find matching table
            matching_table = None
            for table in tables:
                if table.lower() == filename or table.lower().replace('_', '') == filename.replace('_', ''):
                    matching_table = table
                    break
            
            if matching_table:
                logger.info(f"Importing '{csv_file.name}' to table '{matching_table}'")
                if self.import_csv_to_table(str(csv_file), matching_table):
                    imported_count += 1
            else:
                logger.warning(f"No matching table found for CSV file: {csv_file.name}")
        
        logger.info(f"Import completed. {imported_count}/{len(csv_files)} files imported successfully")
        return True
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

