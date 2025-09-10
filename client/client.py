import requests
import json
import uuid
import datetime

# Define the server's URL and the specific endpoints
RIDE_REQUEST_URL = "http://127.0.0.1:8000/api/request-ride"
AVAILABLE_DRIVERS_URL = "http://127.0.0.1:8000/api/available-drivers"

def send_ride_request(user_id, source_location, destination_location, request_id):
    """
    Sends a POST request with ride details to the server's API endpoint.
    """
    payload = {
        "id": request_id,
        "user_id": user_id,
        "source_location": source_location,
        "destination_location": destination_location,
        "request_time": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    
    print(f"Sending request to {RIDE_REQUEST_URL} with data:\n{json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(RIDE_REQUEST_URL, json=payload)
        response.raise_for_status() 
        print("\nServer Response:")
        print(response.json())
        print("Success! Your ride request has been received.")

    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred: {e}")
        print("Please check if the server is running and the URL is correct.")

def get_available_drivers():
    """
    Sends a GET request to retrieve a list of available drivers.
    """
    print(f"\nRequesting list of available drivers from {AVAILABLE_DRIVERS_URL}")
    try:
        response = requests.get(AVAILABLE_DRIVERS_URL)
        response.raise_for_status()
        
        drivers = response.json().get("drivers", [])
        print("\nAvailable Drivers:")
        if drivers:
            for driver in drivers:
                print(f"  - ID: {driver['driver_id']}, Name: {driver['name']}")
        else:
            print("  - No drivers are currently available.")

    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred: {e}")
        print("Please check if the server is running and the URL is correct.")

if __name__ == "__main__":
    # Example 1: Send a ride request
    send_ride_request(
        user_id="user_12345",
        source_location="123 Main St, Anytown",
        destination_location="456 Oak Ave, Somewhere",
        request_id=uuid.uuid4().int
    )

    # Example 2: Get a list of available drivers
    get_available_drivers()
