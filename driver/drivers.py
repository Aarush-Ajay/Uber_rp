import psycopg2
import os
import random
import uuid
import sys

# --- Database Setup (Must match main.py) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ------------------------------------------

# Define the status options (must match the Enum in main.py)
DRIVER_STATUSES = ["off", "in a drive", "accepting"]

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"[DRIVER SIMULATOR] Database connection failed: {e}")
        return None

def create_new_driver():
    """Generates a unique driver and inserts them into the drivers table."""
    conn = get_db_connection()
    if not conn:
        sys.exit(1)
    
    cursor = conn.cursor()
    
    # Generate unique ID and random status
    driver_id = f"DRV-{uuid.uuid4().hex[:8].upper()}"
    driver_name = f"Driver {random.randint(1, 999)}"
    initial_status = random.choice(DRIVER_STATUSES)
    
    try:
        cursor.execute(
            """
            INSERT INTO drivers (driver_id, name, status)
            VALUES (%s, %s, %s);
            """,
            (driver_id, driver_name, initial_status)
        )
        conn.commit()
        
        print(f"\n--- DRIVER SIMULATOR STARTED ---")
        print(f"Driver ID: {driver_id}")
        print(f"Name: {driver_name}")
        print(f"Initial Status: {initial_status.upper()}")
        print(f"Status will be periodically updated by a real-world system.")
        print(f"--------------------------------")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[DRIVER SIMULATOR] Error inserting driver: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_new_driver()
