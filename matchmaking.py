import psycopg2
import os
import time
from enum import Enum
import random

# --- Database Configuration (Must match main.py) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "chiragb07")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ---------------------------------------------

# --- Location Enum for Proximity Matching ---
# The value is used for distance calculation (e.g., Downtown=10, Airport=50)
class Location(Enum):
    # Note: Keys must match the string stored in the database exactly
    Downtown_Core = 10
    Central_Station = 20
    University_Area = 30
    The_Suburbs = 40
    Airport_Terminal = 50

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
        print(f"[MATCH WORKER] ERROR: Database connection failed: {e}")
        return None

def calculate_proximity_score(location_a, location_b):
    """Calculates the absolute difference between two location Enum values."""
    # Location strings use spaces, but Enum keys must be standard Python names (Downtown_Core)
    try:
        val_a = Location[location_a.replace(" ", "_")].value
        val_b = Location[location_b.replace(" ", "_")].value
        return abs(val_a - val_b)
    except KeyError:
        # Should not happen if driver_simulator and user_producer use the correct strings
        print(f"[MATCH WORKER] ERROR: Unknown location string provided: {location_a} or {location_b}")
        return float('inf') # Treat unknown locations as infinitely far

def find_best_driver(cursor, user_source_location):
    """
    Finds the nearest available driver by calculating the minimum proximity score
    between the user's source location and all available drivers.
    The selected driver is locked (FOR UPDATE) to prevent other workers from selecting them.
    """
    
    # 1. Fetch all available drivers and lock them for the duration of the transaction
    # We must fetch the driver's primary key (d.id) to update their status later
    cursor.execute(
        """
        SELECT d.id, d.driver_id, d.name, d.current_location
        FROM drivers d
        WHERE d.status = 'accepting'
        FOR UPDATE SKIP LOCKED;
        """
    )
    available_drivers = cursor.fetchall()
    
    if not available_drivers:
        return None, None, None, None

    best_match = {
        'driver_pk_id': None,
        'driver_id': None,
        'driver_name': None,
        'min_distance': float('inf')
    }

    # 2. Iterate through drivers to find the one with the lowest proximity score
    for driver in available_drivers:
        # Unpack the fetched driver data using explicit indices for safety
        driver_pk_id, driver_id, driver_name, driver_location = driver
        
        # Calculate distance
        distance = calculate_proximity_score(user_source_location, driver_location)
        
        if distance < best_match['min_distance']:
            best_match['min_distance'] = distance
            best_match['driver_pk_id'] = driver_pk_id
            best_match['driver_id'] = driver_id
            best_match['driver_name'] = driver_name

    if best_match['driver_pk_id'] is not None:
        print(f"[MATCH WORKER] Found best driver {best_match['driver_name']} at {driver_location}. Distance score: {best_match['min_distance']}")
        return best_match['driver_pk_id'], best_match['driver_id'], best_match['driver_name'], best_match['min_distance']

    return None, None, None, None


def process_matching_queue():
    """Continuously polls the 'users' table for 'pending' requests and attempts to match a driver."""
    conn = get_db_connection()
    if not conn:
        print("[MATCH WORKER] Cannot start: Failed to connect to database.")
        return

    print("[MATCH WORKER] Starting continuous polling for pending requests...")
    
    try:
        while True:
            # We use a transaction block for atomicity: everything inside either succeeds or rolls back.
            with conn:
                with conn.cursor() as cursor:
                    # 1. FETCH the oldest PENDING request (FIFO based on request_time) and lock it for processing
                    cursor.execute(
                        """
                        SELECT id, user_id, source_location
                        FROM users
                        WHERE request_status = 'pending'
                        ORDER BY request_time ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED;
                        """
                    )
                    request = cursor.fetchone()

                    if not request:
                        # No pending rides, pause polling
                        time.sleep(1)
                        continue

                    request_id, user_id, source_location = request
                    
                    print(f"\n[MATCH WORKER] Processing Request ID {request_id} for user {user_id}...")
                    
                    # 2. ATTEMPT DRIVER MATCH
                    # Returns (driver_pk_id, driver_id, driver_name, distance_score)
                    driver_pk_id, driver_id, driver_name, distance_score = find_best_driver(cursor, source_location)

                    if driver_id:
                        # --- SUCCESSFUL MATCH ---
                        
                        # A. Update driver status to 'in a drive' (note the space, matching the Enum)
                        cursor.execute(
                            """
                            UPDATE drivers
                            SET status = 'in a drive'
                            WHERE id = %s;
                            """,
                            (driver_pk_id,)
                        )
                        
                        # B. Update the user request status to 'matched' and set foreign key and time
                        cursor.execute(
                            """
                            UPDATE users
                            SET request_status = 'matched', driver_fk_id = %s, match_time = NOW()
                            WHERE id = %s;
                            """,
                            (driver_pk_id, request_id)
                        )
                        
                        print(f"[MATCH WORKER] SUCCESS: Request {request_id} MATCHED to {driver_name} ({driver_id}). Distance: {distance_score}.")
                        conn.commit() # Commit success
                        
                    else:
                        # --- NO DRIVER AVAILABLE ---
                        
                        # The request remains 'pending'. We explicitly rollback to release the lock on the request.
                        print(f"[MATCH WORKER] WARNING: Request {request_id} UNSERVICED. No driver available. Leaving pending.")
                        conn.rollback() # Rollback (releases lock on the request row)
                        time.sleep(2) # Short delay before checking again for a driver

    except Exception as e:
        print(f"[MATCH WORKER] CRITICAL ERROR IN PROCESSING LOOP: {e}")
        if conn:
            conn.rollback() 
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_matching_queue()