import psycopg2
import requests
import os
import time
import sys
import json

# --- Configuration (Must match main.py) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "chiragb07")
DB_PORT = os.environ.get("DB_PORT", "5432")

SERVER_API_URL = "http://127.0.0.1:8000/api/request-ride"
# ------------------------------------------

def get_db_connection():
    """Establishes and returns a single connection to the database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        # Set isolation level to allow SELECT FOR UPDATE to work correctly
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED) 
        return conn
    except psycopg2.OperationalError as e:
        print(f"[QUEUE PROCESSOR] Database connection failed: {e}")
        return None

def process_queue():
    """
    Polls the request_queue, processes items, and STOPS when the queue is empty.
    Requests remain in the queue if no driver is available.
    """
    # --- DEBUG START ---
    print(f"[QUEUE PROCESSOR] Attempting to connect to DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    # --- DEBUG END ---
    
    conn = get_db_connection()
    if not conn:
        print("[QUEUE PROCESSOR] Cannot start: Failed to connect to database.")
        return

    print("[QUEUE PROCESSOR] Starting queue processing...")
    
    while True:
        cursor = conn.cursor()
        queue_id = None
        
        try:
            # 1. Fetch the oldest request, ordered by arrival timestamp
            cursor.execute("""
                SELECT id, user_id, source_location, destination_location
                FROM request_queue
                ORDER BY arrival_timestamp ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
            """)
            request_to_process = cursor.fetchone()

            if not request_to_process:
                # --- STOP POLLING LOGIC ---
                print(f"[QUEUE PROCESSOR] Queue empty. Stopping gracefully.")
                break # Exit the while True loop
            
            # Unpack data
            queue_id, user_id, source, destination = request_to_process
            request_data = {
                "user_id": user_id,
                "source_location": source,
                "destination_location": destination
            }

            print(f"[QUEUE PROCESSOR] Fetched request ID {queue_id} for user {user_id}. Sending to API...")

            # 2. Send request to the main server API
            response = requests.post(SERVER_API_URL, json=request_data)
            response.raise_for_status() # Check for HTTP errors (4xx, 5xx)
            
            response_json = response.json()
            driver_status = response_json.get("driver_match_status", "")

            # 3. Check Server's Response for Match Status
            if "No driver available" in driver_status:
                # Request UNSERVICED: Keep in queue by rolling back
                conn.rollback() 
                print(f"[QUEUE PROCESSOR] WARNING: Request ID {queue_id} UNSERVICED. No driver found. Request remains in queue.")
                # Wait briefly before immediately re-checking the queue
                time.sleep(1) 
            else:
                # SUCCESS: Remove from queue
                cursor.execute("DELETE FROM request_queue WHERE id = %s;", (queue_id,))
                conn.commit()
                print(f"[QUEUE PROCESSOR] Success: Request ID {queue_id} processed, matched, and removed from queue.")
                # We do NOT sleep here, we immediately check for the next request

        except requests.exceptions.RequestException as e:
            # Handle API network/connection errors
            conn.rollback()
            print(f"[QUEUE PROCESSOR] ERROR: API call failed. Request ID {queue_id} remains in queue. Error: {e}")
        except psycopg2.Error as e:
            # Handle database errors
            conn.rollback()
            print(f"[QUEUE PROCESSOR] DATABASE ERROR: Transaction rolled back. Error: {e}")
        except Exception as e:
            # Handle all other unexpected errors
            conn.rollback()
            print(f"[QUEUE PROCESSOR] UNEXPECTED ERROR: {e}")
        finally:
            if cursor:
                cursor.close()

# --- Script Entry Point ---
if __name__ == "__main__":
    process_queue()
