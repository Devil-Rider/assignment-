import os
import logging
import pandas as pd
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus 

# 1. Set up logging to track what the pipeline is doing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_schema(engine, schema_name):
    """Ensures the target schema exists in the database."""
    logger.info(f"Ensuring schema '{schema_name}' exists...")
    with engine.connect() as conn:
        # Create the schema if it hasn't been created yet
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
        conn.commit() # Commit the transaction
        logger.info(f"Schema '{schema_name}' is ready.")


def get_redshift_engine():
    """Reads credentials and creates a SQLAlchemy engine for Pandas."""
    load_dotenv()
    
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT")
    dbname = os.getenv("REDSHIFT_DB")
    
    # 2. URL-encode the password to handle special characters (like @, $, etc.) safely
    encoded_password = quote_plus(password)
    
    # 3. Use the encoded_password in your connection string instead
    conn_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(conn_string)
        logger.info("Successfully created Redshift SQLAlchemy engine.")
        return engine
    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        raise



def clean_column_names(df):
    """
    Cleans column names using fast, vectorized Pandas string operations.
    """
    # 1. Convert to string, lowercase, and strip whitespace
    df.columns = df.columns.astype(str).str.lower().str.strip()
    
    # 2. Replace anything that is NOT a-z or 0-9 with an underscore
    df.columns = df.columns.str.replace(r'[^a-z0-9]+', '_', regex=True)
    
    # 3. Strip any accidental underscores at the beginning or end
    df.columns = df.columns.str.strip('_')
    
    return df



def align_order_columns(df_week1, df_week2):
    """Aligns schemas of the two order dataframes before concatenating."""
    logger.info("Aligning schemas for orders_week1 and orders_week2...")
    
    # Check for 'promo' in both, just to be completely safe
    if 'promo' not in df_week1.columns:
        df_week1['promo'] = None
    if 'promo' not in df_week2.columns:
        df_week2['promo'] = None
        
    # Ensure column order matches exactly before concatenating
    df_week1 = df_week1.reindex(columns=df_week2.columns)
    
    # Combine into a single orders DataFrame
    df_orders = pd.concat([df_week1, df_week2], ignore_index=True)
    
    return df_orders

def extract_and_transform():
    """Reads the raw CSVs, standardizes column names, and aligns schemas."""
    logger.info("Extracting raw CSV files...")
    
    # Read the data files
    df_week1 = pd.read_csv("data_files/orders_week1.csv")
    df_week2 = pd.read_csv("data_files/orders_week2.csv")
    df_store = pd.read_csv("data_files/store_master.csv")
    df_sku = pd.read_csv("data_files/sku_master.csv")
    df_inventory = pd.read_csv("data_files/inventory_snapshots.csv")
    
    # Clean column names (using the function we built earlier)
    logger.info("Standardizing column names across all tables...")
    df_week1 = clean_column_names(df_week1)
    df_week2 = clean_column_names(df_week2)
    df_store = clean_column_names(df_store)
    df_sku = clean_column_names(df_sku)
    df_inventory = clean_column_names(df_inventory)
    
    # Use our new dedicated function to align and combine the orders
    df_orders = align_order_columns(df_week1, df_week2)
    logger.info(f"Combined orders into a single dataset with {len(df_orders)} rows.")
    
    return df_orders, df_store, df_sku, df_inventory

def load_to_staging(engine, schema_name, df_orders, df_store, df_sku, df_inventory):
    """Loads DataFrames into Redshift as idempotent staging tables."""
    logger.info(f"Loading data into Redshift schema: {schema_name}...")
    
    # Dictionary mapping DataFrame to its target table name
    # Prefixing all tables with stg_ as requested 
    tables_to_load = {
        "stg_orders": df_orders,
        "stg_store_master": df_store,
        "stg_sku_master": df_sku,
        "stg_inventory_snapshots": df_inventory
    }
    
    for table_name, df in tables_to_load.items():
        logger.info(f"Writing to {table_name}...")
        # if_exists='replace' ensures idempotency (drops and recreates table) 
        # index=False prevents pandas from writing the row numbers as a column
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='replace',
            index=False,
            method='multi', # Speeds up inserts
            chunksize=1000  # Prevents memory overload on larger files
        )
        logger.info(f"Successfully loaded {len(df)} rows into {schema_name}.{table_name}.")

def main():
    logger.info("Starting ingestion pipeline...")
    
    # 1. Establish connection via SQLAlchemy
    engine = get_redshift_engine()
    schema_name = os.getenv("REDSHIFT_SCHEMA")
    setup_schema(engine,schema_name)
    
    # 2. Extract and Transform
    df_orders, df_store, df_sku, df_inventory = extract_and_transform()
    
    # 3. Load
    load_to_staging(engine, schema_name, df_orders, df_store, df_sku, df_inventory)
    
    logger.info("Pipeline execution completed successfully.")

if __name__ == "__main__":
    main()