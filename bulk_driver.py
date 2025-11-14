import requests
import uuid
import random
import time
import sys

# --- Configuration ---
# NOTE: This MUST be the correct URL for your running FastAPI server.
ORCHESTRATOR_API_URL = "http://127.0.0.1:8000/api/register-driver"
TOTAL_DRIVERS = 50 

# These locations must match the Enum keys in your worker scripts
LOCATIONS = ["Downtown Core", "Central Station", "University Area", "The Suburbs", "Airport Terminal"]
DRIVER_STATUSES = ["accepting", "off"]
# ---------------------

def generate_driver_payload():
    """Generates a single driver registration payload."""
    driver_id = "DRV-" + uuid.uuid4().hex[:6].upper()
    first_name = random.choice(["Alex", "Ben", "Charlie", "Dana", "Emily", "Frank", "Grace", "Henry", "Ivy", "Jack"])
    last_name = random.choice(["Smith", "Jones", "Chen", "Lee", "Singh", "Garcia", "Brown", "Miller"])
    driver_name = f"{first_name} {last_name}"
    
    # We will primarily set drivers to 'accepting' for the test, but randomize slightly
    status = random.choice(DRIVER_STATUSES)
    location = random.choice(LOCATIONS)
    
    return {
        "driver_id": driver_id,
        "name": driver_name,
        "status": status,
        "current_location": location
    }

def register_driver(payload):
    """Sends a single POST request to register a driver."""
    try:
        # Note: The server only requires driver_id, name, and current_location, 
        # but the full payload ensures all data points are covered.
        response = requests.post(ORCHESTRATOR_API_URL, json=payload, timeout=5)
        response.raise_for_status() 
        return f"[SUCCESS] Registered {payload['name']} ({payload['driver_id']}) at {payload['current_location']}."
        
    except requests.exceptions.RequestException as e:
        return f"[FAILURE] Driver {payload['driver_id']}, Error: {e}"

def run_bulk_registration():
    """Registers 50 drivers sequentially with a brief pause."""
    start_time = time.time()
    
    print(f"\n--- BULK DRIVER REGISTRATION STARTING ({TOTAL_DRIVERS} drivers) ---")

    success_count = 0
    failure_count = 0
    
    for i in range(1, TOTAL_DRIVERS + 1):
        payload = generate_driver_payload()
        result = register_driver(payload)
        
        if "[SUCCESS]" in result:
            success_count += 1
        else:
            failure_count += 1
            print(result) # Print errors immediately
            
        if i % 10 == 0:
            print(f"  {i} / {TOTAL_DRIVERS} drivers registered...")
        
        # Add a very small pause to avoid flooding the connection pool too aggressively
        time.sleep(0.05) 

    end_time = time.time()
    duration = end_time - start_time

    print("\n--- REGISTRATION COMPLETE ---")
    print(f"Time Taken: {duration:.2f} seconds")
    print(f"Successful Registrations: {success_count}")
    print(f"Failed Registrations: {failure_count}")

if __name__ == "__main__":
    # Ensure the main server (main.py) is running before executing!
    run_bulk_registration()