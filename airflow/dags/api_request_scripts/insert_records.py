import psycopg2
import sys
from psycopg2.extras import execute_batch

# Adjust import path for Airflow
sys.path.append('/opt/airflow/api_request')

from api_request_scripts.eia_monthly import fetch_data
# from api_request import fetch_data

import os
from dotenv import load_dotenv

# -----------------------
#  Database Connection
# -----------------------

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("DBT_HOST"),  # From .env
            port=5432,
            dbname=os.getenv("DBT_DATABASE", "neondb"),
            user=os.getenv("DBT_USER", "neondb_owner"),
            password=os.getenv("DBT_PASS"),
            sslmode="require"  # REQUIRED for Neon
        )
        print(f"Connected to {os.getenv('DBT_HOST')} successfully!")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


# -----------------------
#  Create Table
# -----------------------

def create_table(conn):
    print("Creating table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;

            CREATE TABLE IF NOT EXISTS raw.raw_eia_fuel_data (
                id SERIAL PRIMARY KEY,
                period TIMESTAMP,
                respondent TEXT,
                respondent_name TEXT,
                fueltype TEXT,
                type_name TEXT,
                timezone TEXT,
                timezone_description TEXT,
                value INTEGER,
                value_units TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Table was created.")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


# -----------------------
#  Insert Records
# -----------------------
def insert_records(conn, data):
    print("Inserting EIA fuel-type records into database...")

    rows = data["response"]["data"]

    insert_query = """
        INSERT INTO raw.raw_eia_fuel_data (
            period,
            respondent,
            respondent_name,
            fueltype,
            type_name,
            timezone,
            timezone_description,
            value,
            value_units
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (period, respondent, fueltype) DO NOTHING
    """

    values = [
        (
            row.get("period"),
            row.get("respondent"),
            row.get("respondent-name"),
            row.get("fueltype"),
            row.get("type-name"),
            row.get("timezone"),
            row.get("timezone-description"),
            int(row.get("value")) if row.get("value") not in [None, ""] else None,
            row.get("value-units"),
        )
        for row in rows
    ]

    cursor = conn.cursor()

    execute_batch(
        cursor,
        insert_query,
        values,
        page_size=500  # 👈 sweet spot for Neon
    )

    conn.commit()
    cursor.close()

    print(f"Inserted {len(values)} rows successfully.")


# -----------------------
#  Main Execution
# -----------------------

def main():
    try:
        data = fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, data)

    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")


