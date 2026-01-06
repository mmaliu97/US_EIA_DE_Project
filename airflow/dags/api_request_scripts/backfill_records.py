import psycopg2
import sys
from psycopg2.extras import execute_batch

# Adjust import path for Airflow
sys.path.append('/opt/airflow/api_request')

from api_request_scripts.backfill_eia_monthly import fetch_month_data

import os
from dotenv import load_dotenv
from datetime import date
from dateutil.relativedelta import relativedelta

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
#  Defining monthly range of data
# -----------------------

def month_range(start: date, end: date):
    current = start.replace(day=1)
    while current <= end:
        yield current
        current += relativedelta(months=1)

# -----------------------
#  Main Execution
# -----------------------

def main():
    conn = None
    try:
        conn = connect_to_db()
        create_table(conn)

        start_month = date(2020, 1, 1)
        today = date.today()

        for month_start in month_range(start_month, today):
            month_end = (month_start + relativedelta(months=1)) - relativedelta(days=1)
            if month_end > today:
                month_end = today

            print(f"\n=== Processing {month_start:%Y-%m} ===")

            data = fetch_month_data(month_start, month_end)

            rows = data.get("response", {}).get("data", [])
            if not rows:
                print("No data returned for this month.")
                continue

            insert_records(conn, data)

    except Exception as e:
        print(f"An error occurred during execution: {e}")
        raise

    finally:
        if conn:
            conn.close()
            print("Database connection closed")


