import psycopg2
import os
import uuid
import random

# --- Database Configuration (Must match main.py) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ---------------------------------------------

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
        print(f"[DRIVER SIMULATOR] ERROR: Database connection failed: {e}")
        return None

def generate_random_location():
    """Generates a random location string that matches the Location Enum keys in match_worker.py."""
    # These strings must match the keys in the Location Enum, including spaces.
    locations = ["Downtown Core", "Central Station", "University Area", "The Suburbs", "Airport Terminal"]
    return random.choice(locations)

def simulate_driver():
    """Creates a new driver with a random initial status and inserts into the database."""
    conn = get_db_connection()
    if not conn:
        print("[DRIVER SIMULATOR] Failed to run due to DB connection error.")
        return

    driver_id = "DRV-" + uuid.uuid4().hex[:6].upper()
    # Use an underscore for simple logging and consistency
    first_name = random.choice(["Alex", "Ben", "Charlie", "Dana", "Emily", "Frank"])
    last_name = random.choice(["Smith", "Jones", "Chen", "Lee", "Singh"])
    driver_name = f"{first_name}_{last_name}" 
    
    # Status randomization: Now ONLY chooses between 'accepting' and 'off'.
    status = random.choice(["accepting", "off"]) 
    location = generate_random_location()

    try:
        cursor = conn.cursor()
        
        # Insert the new driver into the drivers table
        cursor.execute(
            """
            INSERT INTO drivers (driver_id, name, status, current_location)
            VALUES (%s, %s, %s, %s);
            """,
            (driver_id, driver_name, status, location)
        )
        conn.commit()
        
        print(f"\n[DRIVER SIMULATOR] NEW DRIVER ADDED: {driver_name}")
        print(f"  ID: {driver_id}, Location: {location}")
        print(f"  Initial Status: {status.upper()}")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[DRIVER SIMULATOR] ERROR: Database insertion failed. Driver was not added: {e}")
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    simulate_driver()
