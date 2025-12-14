import psycopg2
import sys

# Adjust import path for Airflow
sys.path.append('/opt/airflow/api_request')

from api_request_scripts.api_request import fetch_data
# from api_request import fetch_data


# -----------------------
#  Database Connection
# -----------------------

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        )
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
            CREATE SCHEMA IF NOT EXISTS dev;

            CREATE TABLE IF NOT EXISTS dev.raw_eia_fuel_data (
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

    try:
        rows = data["response"]["data"]  # list of dicts from EIA API

        cursor = conn.cursor()

        insert_query = """
            INSERT INTO dev.raw_eia_fuel_data (
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
            );
        """

        for row in rows:
            cursor.execute(
                insert_query,
                (
                    row.get("period"),
                    row.get("respondent"),
                    row.get("respondent-name"),
                    row.get("fueltype"),
                    row.get("type-name"),
                    row.get("timezone"),
                    row.get("timezone-description"),
                    int(row.get("value")) if row.get("value") not in [None, ""] else None,
                    row.get("value-units")
                )
            )

        conn.commit()
        print(f"Inserted {len(rows)} rows successfully.")

    except psycopg2.Error as e:
        print(f"Error inserting data into the database: {e}")
        conn.rollback()
        raise


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


main()
