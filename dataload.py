"""
-----------
dataload.py
-----------

Bulk loads CSV files into Oracle database tables.

Requirements:
    pip install oracledb python-dotenv

Setup:
    Create a .env file in the same directory:
        DB_USER=your_username
        DB_PASS=your_password
        DB_HOST=your_host
        DB_PORT=1521
        DB_SERVICE=your_service_name
        DB_DSN=your_host:1521/your_service_name
    Or just fill in the DB credentials directly in the script 

CSV files expected in the same directory as this script:
    occupation.csv, states.csv, cardholder.csv, city_details.csv,
    cardholder_location.csv, merchant_category.csv, merchant.csv, transactions.csv
"""

import oracledb
import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

#--------------------------------------------------------------------------------
# Just fill in your DB credentials here or use a .env file with python-dotenv
from dotenv import load_dotenv
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
DB_DSN = os.getenv("DB_DSN") or f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE')}"

if not DB_USER or not DB_PASS or not DB_DSN:
    raise SystemExit(
        "Missing DB_USER, DB_PASS/DB_PASSWORD, or DB_DSN. Check your .env file."
    )
# Meow
#--------------------------------------------------------------------------------

# Replace with your Instant Client Path
LIB_DIR = r"C:\Users\zachd\OneDrive\Documents\oracle_database\instantclient_11_2"  
# Initialize Thick Mode (Required for FreeSQL/Cloud)
oracledb.init_oracle_client(lib_dir=LIB_DIR)

DELETE_ORDER = [
    'transactions',
    'cardholder_location',
    'cardholder',
    'merchant',
    'category',
    'cities',
    'states',
    'occupation'
]


def get_existing_tables(cursor):
    cursor.execute("SELECT table_name FROM user_tables")
    return {row[0] for row in cursor}


def clear_existing_rows(cursor):
    existing_tables = get_existing_tables(cursor)
    for table in DELETE_ORDER:
        if table.upper() in existing_tables:
            print(f"Clearing existing rows from {table}...")
            cursor.execute(f"TRUNCATE TABLE {table}")
        else:
            print(f"Skipping missing table {table}.")


def clear_all_data():
    conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
    cursor = conn.cursor()
    try:
        clear_existing_rows(cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def table_exists(cursor, table_name):
    existing_tables = get_existing_tables(cursor)
    return table_name.upper() in existing_tables


def bulk_load(table_name, file_path, sql):
    full_path = os.path.join(SCRIPT_DIR, file_path)
    try:
        conn   = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()

        if not table_exists(cursor, table_name):
            print(f"Skipping load for {file_path}: target table {table_name} does not exist")
            return

        with open(full_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header row
            data = [[cell.strip() for cell in row] for row in reader]

        print(f"Starting bulk load of {len(data)} rows from {os.path.basename(file_path)}...")
        cursor.executemany(sql, data)
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows.\n")

    except Exception as e:
        print(f"Error loading {file_path}: {e}\n")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn'   in locals(): conn.close()


# ----------------- Bulk load each CSV file with appropriate SQL -----------------

clear_all_data()

bulk_load(
    'OCCUPATION',
    'occupation.csv',
    "INSERT INTO occupation (occupation_id, job) VALUES (:1, :2)"
)

bulk_load(
    'STATES',
    'states.csv',
    "INSERT INTO states (state_code, state_name) VALUES (:1, :2)"
)

bulk_load(
    'CARDHOLDER',
    'cardholder.csv',
    """INSERT INTO cardholder (cardholder_id, first_name, last_name, dob, occupation_id, gender)
       VALUES (:1, :2, :3, TO_DATE(:4, 'MM/DD/YYYY'), :5, :6)"""
)

bulk_load(
    'CITIES',
    'cities.csv',
    "INSERT INTO cities (city_id, city, state_code, city_pop) VALUES (:1, :2, :3, :4)"
)

bulk_load(
    'CARDHOLDER_LOCATION',
    'cardholder_location.csv',
    """INSERT INTO cardholder_location (location_id, cardholder_id, street, city_id, zip_code, latitude, longitude)
       VALUES (:1, :2, :3, :4, :5, :6, :7)"""
)

bulk_load(
    'CATEGORY',
    'category.csv',
    "INSERT INTO category (category_id, category_name) VALUES (:1, :2)"
)

bulk_load(
    'MERCHANT',
    'merchant.csv',
    "INSERT INTO merchant (merchant_id, merchant_name) VALUES (:1, :2)"
)

bulk_load(
    'TRANSACTIONS',
    'transactions.csv',
    """INSERT INTO transactions
           (transaction_id, cardholder_id, merchant_id, category_id, transaction_lat, 
            transaction_long, amount, unix_time, is_fraud)
       VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)"""
)
