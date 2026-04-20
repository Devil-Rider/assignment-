import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# 1. Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_redshift_engine():
    """Reads credentials from .env and creates a SQLAlchemy engine safely."""
    load_dotenv()
    
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT", "5439")
    dbname = os.getenv("REDSHIFT_DB")
    
    # Safely encode password in case of special characters
    encoded_password = quote_plus(password)
    conn_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(conn_string)
        logger.info("Successfully connected to Redshift.")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {e}")
        raise

def execute_sql_files(engine, folder_path):
    """
    Finds all .sql files in the target directory, reads them, 
    and executes them sequentially in Redshift.
    """
    # Use pathlib to easily handle paths across Mac/Windows/Linux
    sql_dir = Path("/Users/kirankumarr/Desktop/Angara - assingment/Part 2 /sql/")
    
    if not sql_dir.exists() or not sql_dir.is_dir():
        logger.error(f"Directory not found: {folder_path}")
        return

    # Grab all .sql files in the folder and sort them alphabetically
    # Sorting ensures they run in a predictable order (e.g., 1_dim_store.sql, 2_fct_orders.sql)
    sql_files = sorted(sql_dir.glob('*.sql'))
    
    if not sql_files:
        logger.warning(f"No .sql files found in {folder_path}")
        return

    logger.info(f"Found {len(sql_files)} SQL files to execute in '{folder_path}'.")

    # Connect to the database and execute
    with engine.begin() as conn:
        for file_path in sql_files:
            logger.info(f"Reading and executing: {file_path.name}...")
            
            # Read the SQL file content
            with open(file_path, 'r') as file:
                sql_query = file.read()
                
            # Execute the query
            try:
                conn.execute(text(sql_query))
                logger.info(f"Successfully executed {file_path.name}.")
            except Exception as e:
                logger.error(f"Error executing {file_path.name}: {e}")
                raise # Stop the pipeline if a query fails

def main():
    logger.info("Starting SQL execution pipeline...")
    
    # 1. Establish the connection
    engine = get_redshift_engine()
    
    # 2. Define the folder where your SQL files live
    # Change "models" to whatever your folder is named (e.g., "queries" or "sql")
    sql_folder = "sql" 
    
    # 3. Execute the files
    execute_sql_files(engine, sql_folder)
    
    logger.info("All SQL files executed successfully!")

if __name__ == "__main__":
    main()