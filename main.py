from fastapi import FastAPI
from pydantic import BaseModel
from enum import Enum
import psycopg2
import os

app = FastAPI()

# Database connection details (replace with your own)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Define the data structure for the ride request
class RideRequest(BaseModel):
    user_id: str
    source_location: str
    destination_location: str

# Define an Enum for driver availability states
class DriverStatus(str, Enum):
    off = "off"
    in_a_drive = "in a drive"
    accepting = "accepting"

# Define the data structure for a driver
class Driver(BaseModel):
    driver_id: str
    name: str
    status: DriverStatus

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

def create_initial_tables():
    """Creates all necessary tables if they don't exist."""
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        
        # Create ride_requests table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ride_requests (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                source_location VARCHAR(255) NOT NULL,
                destination_location VARCHAR(255) NOT NULL,
                request_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create drivers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id SERIAL PRIMARY KEY,
                driver_id VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL
            );
        """)

        # Create clients table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                client_id VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL
            );
        """)
        
        conn.commit()
        cursor.close()
        conn.close()

# Call this function to ensure tables exist
create_initial_tables()

# Insert some sample driver data for testing
def insert_sample_drivers():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM drivers;")
            count = cursor.fetchone()[0]
            if count == 0:
                print("Inserting sample driver data...")
                drivers_data = [
                    ('driver_001', 'John Doe', DriverStatus.accepting.value),
                    ('driver_002', 'Jane Smith', DriverStatus.in_a_drive.value),
                    ('driver_003', 'Alex Chen', DriverStatus.off.value),
                    ('driver_004', 'Emily Davis', DriverStatus.accepting.value)
                ]
                for driver in drivers_data:
                    cursor.execute(
                        """
                        INSERT INTO drivers (driver_id, name, status)
                        VALUES (%s, %s, %s);
                        """, driver
                    )
                conn.commit()
                print("Sample drivers inserted.")
            else:
                print("Sample drivers already exist.")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting sample drivers: {e}")
        finally:
            cursor.close()
            conn.close()

# Insert some sample client data for testing
def insert_sample_clients():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM clients;")
            count = cursor.fetchone()[0]
            if count == 0:
                print("Inserting sample client data...")
                clients_data = [
                    ('client_001', 'Raj Verma'),
                    ('client_002', 'Priya Singh'),
                    ('client_003', 'Rohan Sharma')
                ]
                for client in clients_data:
                    cursor.execute(
                        """
                        INSERT INTO clients (client_id, name)
                        VALUES (%s, %s);
                        """, client
                    )
                conn.commit()
                print("Sample clients inserted.")
            else:
                print("Sample clients already exist.")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting sample clients: {e}")
        finally:
            cursor.close()
            conn.close()

insert_sample_drivers()
insert_sample_clients()

@app.post("/api/request-ride")
async def handle_ride_request(request: RideRequest):
    """
    Receives ride request data and stores it in the PostgreSQL database.
    """
    conn = get_db_connection()
    if not conn:
        return {"error": "Could not connect to the database."}
        
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO ride_requests (user_id, source_location, destination_location)
            VALUES (%s, %s, %s);
            """,
            (request.user_id, request.source_location, request.destination_location)
        )
        conn.commit()
        
        print("\n----------------------------------------------------")
        print("Ride request successfully stored in PostgreSQL.")
        print("Received Ride Request:")
        print(f"User ID: {request.user_id}")
        print(f"Source: {request.source_location}")
        print(f"Destination: {request.destination_location}")
        print("----------------------------------------------------\n")

        return {
            "message": "Ride request successfully received and stored",
            "details": request
        }
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database insertion failed: {e}")
        return {"error": "Failed to store the ride request."}
        
    finally:
        cursor.close()
        conn.close()

@app.get("/api/available-drivers")
async def get_available_drivers():
    """
    Returns a list of drivers who are available (status = 'accepting').
    """
    conn = get_db_connection()
    if not conn:
        return {"error": "Could not connect to the database."}

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT driver_id, name, status
            FROM drivers
            WHERE status = %s;
            """,
            (DriverStatus.accepting.value,)
        )
        
        available_drivers = [
            {"driver_id": row[0], "name": row[1], "status": row[2]} 
            for row in cursor.fetchall()
        ]
        
        return {"drivers": available_drivers}

    except psycopg2.Error as e:
        print(f"Database query failed: {e}")
        return {"error": "Failed to retrieve drivers."}

    finally:
        cursor.close()
        conn.close()

@app.get("/")
async def root():
    return {"message": "Mini Uber API is running!"}
