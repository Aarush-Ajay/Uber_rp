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

# --- Location Enum for Duration Calculation (Must match match_worker.py) ---
class Location(Enum):
    # The value is used for distance calculation (e.g., Downtown=10, Airport=50)
    # The larger the difference, the longer the simulated ride time.
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
        print(f"[STATUS CHECKER] ERROR: Database connection failed: {e}")
        return None

def calculate_ride_duration(source, destination):
    """
    Calculates the simulated ride time in seconds based on the absolute
    difference between the Enum values of the source and destination.
    """
    MIN_DURATION = 2 # Minimum time to ensure a brief delay
    
    try:
        # Convert location strings (with spaces) to Enum keys (with underscores)
        val_source = Location[source.replace(" ", "_")].value
        val_destination = Location[destination.replace(" ", "_")].value
        
        # Duration = Absolute Distance + Minimum delay
        duration = abs(val_source - val_destination) + MIN_DURATION
        
        return duration
    except KeyError:
        # Fallback duration if a location string is invalid
        print(f"[STATUS CHECKER] WARNING: Unknown location in duration calculation. Using fallback time.")
        return 10 # Default to 10 seconds for unknown locations

def simulate_ride_completion():
    """Continuously polls the 'users' table for 'matched' rides and marks them complete."""
    conn = get_db_connection()
    if not conn:
        print("[STATUS CHECKER] Cannot start: Failed to run due to DB connection error.")
        return

    print("[STATUS CHECKER] Starting continuous polling for matched rides...")
    
    try:
        while True:
            # We use a transaction block for atomicity.
            with conn:
                with conn.cursor() as cursor:
                    # 1. FETCH the oldest MATCHED request (FIFO) and lock it for processing
                    # We join with drivers to get the driver_id for better logging.
                    cursor.execute(
                        """
                        SELECT 
                            u.id, u.user_id, u.driver_fk_id, u.source_location, u.destination_location,
                            d.driver_id, u.match_time
                        FROM users u
                        JOIN drivers d ON u.driver_fk_id = d.id
                        WHERE u.request_status = 'matched'
                        ORDER BY u.match_time ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED;
                        """
                    )
                    ride_data = cursor.fetchone()

                    if not ride_data:
                        # No matched rides to complete, pause polling
                        time.sleep(5)
                        continue

                    # Unpack the fetched data
                    (request_id, user_id, driver_pk_id, source, destination, 
                     driver_id, match_time) = ride_data
                    
                    # -----------------------------------------------------------
                    # UPDATED LOGGING LINE: Now includes source and destination
                    # -----------------------------------------------------------
                    print(f"\n[STATUS CHECKER] PROCESSING: Ride ID {request_id} ({user_id}) from {source} to {destination} (Driver {driver_id}) is in progress...")

                    # 2. SIMULATE RIDE TIME based on calculated duration
                    ride_duration = calculate_ride_duration(source, destination)
                    
                    print(f"[STATUS CHECKER]   Simulating ride duration: {ride_duration} seconds ({source} to {destination}).")
                    time.sleep(ride_duration)
                    
                    # 3. MARK RIDE AS COMPLETED (Final Status Update)
                    cursor.execute(
                        """
                        UPDATE users
                        SET request_status = 'completed', completion_time = NOW()
                        WHERE id = %s;
                        """,
                        (request_id,)
                    )
                    
                    # 4. FREE UP THE DRIVER (Crucial step for system flow)
                    # The driver is now 'accepting' again and available for the Match Worker.
                    cursor.execute(
                        """
                        UPDATE drivers
                        SET status = 'accepting'
                        WHERE id = %s;
                        """,
                        (driver_pk_id,)
                    )
                    
                    conn.commit() # Commit success
                    print(f"[STATUS CHECKER] SUCCESS: Ride {request_id} COMPLETED. Driver {driver_id} is now ACCEPTING.")
                    
    except Exception as e:
        print(f"[STATUS CHECKER] CRITICAL ERROR IN COMPLETION LOOP: {e}")
        if conn:
            conn.rollback() 
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    simulate_ride_completion()