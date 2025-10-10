import requests
import uuid
import json
import random

# The producer talks directly to the Orchestrator's API
ORCHESTRATOR_API_URL = "http://127.0.0.1:8000/api/request-ride"

def generate_random_location():
    """Generates a random location string, matching the Enum keys in the workers."""
    locations = ["Downtown Core", "Central Station", "University Area", "The Suburbs", "Airport Terminal"]
    return random.choice(locations)

def send_request():
    """Generates a random user request and sends it to the Orchestrator API."""
    user_id = "USER-" + uuid.uuid4().hex[:8].upper()
    source = generate_random_location()
    destination = generate_random_location()
    
    payload = {
        "user_id": user_id,
        "source_location": source,
        "destination_location": destination
    }

    print(f"\n[USER PRODUCER] Simulating user {user_id}...")
    
    try:
        response = requests.post(ORCHESTRATOR_API_URL, json=payload)
        response.raise_for_status() 
        
        data = response.json()
        print(f"[USER PRODUCER] SUCCESS: Request ID {data['request_id']} logged.")
        # Display the source and destination details
        print(f"[USER PRODUCER]   Trip: {source} -> {destination}. Status: {data['status']}")
        
    except requests.exceptions.RequestException as e:
        print(f"[USER PRODUCER] ERROR: Failed to connect to orchestrator: {e}")
        print("Please ensure the Orchestrator (main.py) is running on port 8000.")

if __name__ == "__main__":
    send_request()
