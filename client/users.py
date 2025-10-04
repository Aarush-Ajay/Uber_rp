import psycopg2
import os
import time
import random

# --- Database Setup (Must match main.py) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ------------------------------------------

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
        print(f"Database connection failed: {e}")
        return None

def insert_new_request(user_id, source, destination):
    """Inserts a new ride request into the queue table."""
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO request_queue (user_id, source_location, destination_location)
            VALUES (%s, %s, %s);
            """,
            (user_id, source, destination)
        )
        conn.commit()
        print(f"[PRODUCER] User {user_id} added request to queue. Source: {source}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[PRODUCER] Error inserting request: {e}")
    finally:
        cursor.close()
        conn.close()

def simulate_single_user_request():
    """Simulates a single user sending one random ride request to the queue."""
    locations = ["Downtown", "Airport", "Suburbs", "University", "City Center", "Mall"]
    
    # Generate a random user ID and select unique locations
    user_id = f"user_{random.randint(10000, 99999)}"
    source_location, dest_location = random.sample(locations, 2)
    
    print(f"\n--- Starting Request Generation for User {user_id} ---")
    
    insert_new_request(user_id, source_location, dest_location)
    
    print(f"--- Request successfully generated and sent to queue for User {user_id} ---")

if __name__ == "__main__":
    simulate_single_user_request()
