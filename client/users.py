import requests
import uuid
import json
import random
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION FOR SCALING TEST ---
TOTAL_REQUESTS = 1000
TOTAL_DRIVERS = 50
MAX_CONCURRENT_THREADS = 50 # Threads for sending user requests simultaneously

# Port range expanded to simulate a larger capacity (20 instances: 8000 through 8019)
START_PORT = 8000
END_PORT = 8019 

ORCHESTRATOR_HOST = "http://127.0.0.1"
DRIVER_API_URL = f"{ORCHESTRATOR_HOST}:{START_PORT}/api/register-driver" # Assume T1 (8000) is always available for setup

# Global list to store ports confirmed to be active
ACTIVE_PORTS = []
# --------------------------------------

# --- UTILITY FUNCTIONS ---

def generate_random_location():
    """Generates a random location string, matching the Enum keys in the workers."""
    locations = ["Downtown Core", "Central Station", "University Area", "The Suburbs", "Airport Terminal"]
    return random.choice(locations)

def check_active_ports():
    """Performs a quick GET request on all potential ports to check for a 200 OK status."""
    global ACTIVE_PORTS
    potential_ports = list(range(START_PORT, END_PORT + 1))
    
    print(f"\n[PORT SCAN] Scanning for active server instances (Ports {START_PORT}-{END_PORT})...")
    
    active_found = []
    
    for port in potential_ports:
        check_url = f"{ORCHESTRATOR_HOST}:{port}/" 
        try:
            # Check using a low timeout
            response = requests.get(check_url, timeout=0.5) 
            if response.status_code == 200:
                active_found.append(port)
        except requests.exceptions.RequestException:
            pass
            
    ACTIVE_PORTS = active_found
    
    if ACTIVE_PORTS:
        print(f"[PORT SCAN] Found {len(ACTIVE_PORTS)} active servers: {ACTIVE_PORTS}")
    else:
        print("[PORT SCAN] CRITICAL: No active servers found. ABORTING TEST.")
    
    return bool(ACTIVE_PORTS)

# --- DRIVER REGISTRATION PHASE ---

def generate_driver_payload():
    """Generates a single driver registration payload."""
    driver_id = "DRV-" + uuid.uuid4().hex[:6].upper()
    first_name = random.choice(["Alex", "Ben", "Charlie", "Dana", "Emily", "Frank", "Grace", "Henry", "Ivy", "Jack"])
    last_name = random.choice(["Smith", "Jones", "Chen", "Lee", "Singh", "Garcia", "Brown", "Miller"])
    driver_name = f"{first_name} {last_name}"
    
    status = random.choice(["accepting", "off"]) # Status is set by the API logic, but we include it
    location = generate_random_location()
    
    return {
        "driver_id": driver_id,
        "name": driver_name,
        "status": status,
        "current_location": location
    }

def register_driver_bulk():
    """Registers 50 drivers sequentially using the driver API endpoint."""
    print(f"\n--- PHASE 1: BULK DRIVER REGISTRATION ({TOTAL_DRIVERS} Drivers) ---")

    success_count = 0
    
    for i in range(TOTAL_DRIVERS):
        payload = generate_driver_payload()
        try:
            # Note: We rely on the server running on port 8000 for the setup phase
            response = requests.post(DRIVER_API_URL, json=payload, timeout=2) 
            response.raise_for_status() 
            success_count += 1
        except requests.exceptions.RequestException as e:
            print(f"[DRIVER SETUP] FAILED to register driver {payload['driver_id']}. Error: {e}")
            
        if (i + 1) % 10 == 0:
            print(f"  {i + 1} / {TOTAL_DRIVERS} drivers registered...")
        
        # Small pause to prevent overwhelming the single DB connection during setup
        time.sleep(0.01) 

    print(f"--- PHASE 1 COMPLETE: {success_count} drivers successfully registered. ---")

# --- CONCURRENT REQUEST SENDING PHASE ---

def send_request_concurrent(payload):
    """Sends a single POST request to a random active port."""
    
    # Selects a port only from the globally active list
    target_port = random.choice(ACTIVE_PORTS)
    API_URL = f"{ORCHESTRATOR_HOST}:{target_port}/api/request-ride"
    
    try:
        response = requests.post(API_URL, json=payload, timeout=5)
        response.raise_for_status() 
        data = response.json()
        request_id = data.get('request_id', 'N/A')
        
        # Log minimal success data for the thread pool
        return f"[SUCCESS] ID:{request_id} @ Port:{target_port}"
        
    except requests.exceptions.RequestException as e:
        return f"[FAILURE] User:{payload['user_id']} @ Port:{target_port}, Error:{e}"


def run_concurrent_requests():
    """Runs the stress test across multiple threads and active ports."""
    print(f"\n--- PHASE 2: CONCURRENT REQUESTS ({TOTAL_REQUESTS} Users) ---")

    # 1. Create all 1000 payloads
    # IMPORTANT: We generate user payloads here, not driver payloads
    all_payloads = [
        {"user_id": f"USER-{i}", "source_location": generate_random_location(), "destination_location": generate_random_location()} 
        for i in range(TOTAL_REQUESTS)
    ]
    
    start_time = time.time()
    
    success_count = 0
    failure_count = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_THREADS) as executor:
        # Submit all tasks to the thread pool
        future_to_payload = {executor.submit(send_request_concurrent, payload): payload for payload in all_payloads}
        
        # Process results as they complete
        for future in as_completed(future_to_payload):
            result = future.result()
            
            if "[SUCCESS]" in result:
                success_count += 1
            else:
                failure_count += 1
            
            # Print status periodically
            if (success_count + failure_count) % 100 == 0:
                print(f"  {success_count + failure_count} / {TOTAL_REQUESTS} requests completed. Successes: {success_count}")

    end_time = time.time()
    duration = end_time - start_time

    print("\n--- STRESS TEST COMPLETE SUMMARY ---")
    print(f"Time Taken: {duration:.2f} seconds")
    print(f"Requests Per Second (RPS): {TOTAL_REQUESTS / duration:.2f}")
    print(f"Successful Requests: {success_count}")
    print(f"Failed Requests: {failure_count}")


if __name__ == "__main__":
    # 1. Check which servers are actually running (Ports 8000-8019)
    if not check_active_ports():
        sys.exit(1) # Exit if no servers are running
    
    # 2. Add 50 drivers to ensure high capacity
    register_driver_bulk()
    
    # 3. Wait briefly for DB to settle and workers to clear any old state
    print("\n[INFO] Waiting 2 seconds for workers to stabilize...")
    time.sleep(2)
    
    # 4. Launch the 1000 concurrent user requests
    run_concurrent_requests()
