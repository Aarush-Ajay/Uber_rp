import requests
import uuid
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
# NOTE: This MUST be the correct URL for your running FastAPI server.
ORCHESTRATOR_API_URL = "http://127.0.0.1:8000/api/request-ride"
TOTAL_REQUESTS = 1000
MAX_WORKERS = 50 # Number of threads to run concurrently (adjust based on your CPU/network)

# These locations must match the Enum keys in your worker scripts
LOCATIONS = ["Downtown Core", "Central Station", "University Area", "The Suburbs", "Airport Terminal"]

# ---------------------

def generate_payload():
    """Generates a random user request payload."""
    user_id = "STRESS-" + uuid.uuid4().hex[:8].upper()
    source = random.choice(LOCATIONS)
    destination = random.choice(LOCATIONS)
    
    return {
        "user_id": user_id,
        "source_location": source,
        "destination_location": destination
    }

def send_request(payload):
    """Sends a single POST request and handles the response."""
    try:
        response = requests.post(ORCHESTRATOR_API_URL, json=payload, timeout=5)
        response.raise_for_status() 
        data = response.json()
        
        # Log success details
        status = data.get('status', 'N/A')
        request_id = data.get('request_id', 'N/A')
        return f"[SUCCESS] ID: {request_id}, Status: {status}"
        
    except requests.exceptions.RequestException as e:
        # Log failure details (e.g., server down or timeout)
        return f"[FAILURE] User: {payload['user_id']}, Error: {e}"

def run_stress_test():
    """Manages the pool of threads to send all requests concurrently."""
    start_time = time.time()
    
    print(f"\n--- STRESS TEST STARTING ---")
    print(f"Target URL: {ORCHESTRATOR_API_URL}")
    print(f"Total Requests: {TOTAL_REQUESTS}")
    print(f"Concurrency Level (Threads): {MAX_WORKERS}")

    # Create all 1000 payloads before starting the execution
    all_payloads = [generate_payload() for _ in range(TOTAL_REQUESTS)]
    
    success_count = 0
    failure_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks to the thread pool
        future_to_payload = {executor.submit(send_request, payload): payload for payload in all_payloads}
        
        # Process results as they complete
        for future in as_completed(future_to_payload):
            result = future.result()
            
            if "[SUCCESS]" in result:
                success_count += 1
            else:
                failure_count += 1
            
            # Print status periodically (e.g., every 100 requests)
            if (success_count + failure_count) % 100 == 0:
                print(f"  {success_count + failure_count} / {TOTAL_REQUESTS} requests completed...")

    end_time = time.time()
    duration = end_time - start_time

    print("\n--- STRESS TEST COMPLETE ---")
    print(f"Time Taken: {duration:.2f} seconds")
    print(f"Requests Per Second (RPS): {TOTAL_REQUESTS / duration:.2f}")
    print(f"Successful Requests: {success_count}")
    print(f"Failed Requests: {failure_count}")

if __name__ == "__main__":
    # Ensure all worker components are running before running the stress test!
    run_stress_test()