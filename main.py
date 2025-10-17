import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import os
import random
import time

# --- Database Configuration ---
# NOTE: These variables should be set in the environment for production use.
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "chiragb07")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ------------------------------

app = FastAPI()

# Global variable for the connection pool
db_pool = None

# --- Models and Enums ---
class RideStatus(str, Enum):
    pending = "pending"
    matched = "matched"
    completed = "completed"
    cancelled = "cancelled"

class DriverStatus(str, Enum):
    accepting = "accepting" # Available to take a new ride
    in_a_drive = "in a drive" # Currently transporting a rider
    off = "off" # Offline/Unavailable

class UserRequest(BaseModel):
    user_id: str
    source_location: str
    destination_location: str

class DriverRegistration(BaseModel):
    driver_id: str
    name: str
    current_location: str
    # status will default to 'accepting' upon registration

# --- Database Pool and Connection Functions ---

def get_db_connection():
    """Retrieves a connection from the pool."""
    global db_pool
    if db_pool:
        # Blocks until a connection is available
        return db_pool.getconn()
    return None

def put_db_connection(conn):
    """Returns a connection to the pool."""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def initialize_db_pool():
    """Initializes the connection pool and creates necessary tables."""
    global db_pool
    print("Initializing Database Connection Pool...")
    try:
        # Initialize pool
        db_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        # Create tables
        create_initial_tables()
        print("Database Connection Pool Initialized successfully.")

    except psycopg2.OperationalError as e:
        print(f"FATAL ERROR: Database connection failed. Check credentials and server status: {e}")
        # Setting db_pool to None ensures endpoints know the database is down
        db_pool = None 

def create_initial_tables():
    """Creates the 'drivers' and 'users' tables if they don't exist."""
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # 1. Create drivers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id SERIAL PRIMARY KEY,
                driver_id VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'accepting',
                current_location VARCHAR(255)
            );
        """)
        
        # 2. Create users table (which stores the ride requests/status)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                source_location VARCHAR(255) NOT NULL,
                destination_location VARCHAR(255) NOT NULL,
                request_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                request_status VARCHAR(50) DEFAULT 'pending',
                match_time TIMESTAMP WITH TIME ZONE NULL,          -- Added for tracking match event
                completion_time TIMESTAMP WITH TIME ZONE NULL,     -- Added for tracking trip completion
                
                -- Foreign Key to link to driver once matched
                driver_fk_id INTEGER REFERENCES drivers(id) NULL
            );
        """)
        
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error creating initial tables: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)

# Initialize the database pool when the application starts
initialize_db_pool()

# --- API Endpoints ---

@app.get("/")
async def root():
    return {"message": "Uber Ride Orchestrator is running! Use /api/request-ride to start a ride request."}


@app.post("/api/register-driver")
async def register_driver(driver_data: DriverRegistration):
    """Adds a new driver to the system with 'accepting' status."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        # Insert driver with default 'accepting' status, or update if ID exists
        cursor.execute(
            """
            INSERT INTO drivers (driver_id, name, status, current_location)
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (driver_id) DO UPDATE 
            SET name = EXCLUDED.name, status = EXCLUDED.status, current_location = EXCLUDED.current_location
            RETURNING id;
            """,
            (driver_data.driver_id, driver_data.name, DriverStatus.accepting.value, driver_data.current_location)
        )
        driver_pk_id = cursor.fetchone()[0]
        conn.commit()
        
        print(f"[DRIVER] Driver {driver_data.driver_id} registered/updated with status: accepting.")
        
        return {
            "message": "Driver registered/updated successfully.",
            "driver_pk_id": driver_pk_id,
            "driver_id": driver_data.driver_id
        }
        
    except psycopg2.IntegrityError as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Registration error: {e}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[DRIVER] Database operation failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to process driver registration.")
        
    finally:
        put_db_connection(conn)


@app.post("/api/request-ride")
async def handle_ride_request(request: UserRequest):
    """
    User API entry point. Logs the request into the 'users' table with a 'pending' status.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        # Insert the request directly into the users table with pending status
        cursor.execute(
            """
            INSERT INTO users (user_id, source_location, destination_location, request_status)
            VALUES (%s, %s, %s, %s) RETURNING id;
            """,
            (request.user_id, request.source_location, request.destination_location, RideStatus.pending.value)
        )
        new_request_id = cursor.fetchone()[0]
        conn.commit()
        
        print(f"\n[ORCHESTRATOR] NEW REQUEST ID {new_request_id} logged as PENDING.")
        
        return {
            "message": "Ride request received and queued for matching.",
            "request_id": new_request_id,
            "status": RideStatus.pending.value
        }
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[ORCHESTRATOR] Database insertion failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue request.")
        
    finally:
        put_db_connection(conn)


@app.get("/api/ride-status/{request_id}")
async def get_ride_status(request_id: int):
    """Allows the client to poll for the status of their ride."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                u.request_status, 
                u.source_location, 
                u.destination_location,
                d.name AS driver_name, 
                d.driver_id,
                d.current_location AS driver_location
            FROM users u
            LEFT JOIN drivers d ON u.driver_fk_id = d.id
            WHERE u.id = %s;
            """,
            (request_id,)
        )
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Request ID not found.")
            
        status, source, destination, driver_name, driver_id, driver_location = result
        
        response = {
            "request_id": request_id,
            "status": status,
            "source": source,
            "destination": destination,
        }
        
        if driver_name and status == RideStatus.matched.value:
             response["driver_info"] = {
                "name": driver_name, 
                "driver_id": driver_id,
                "current_location": driver_location,
                "eta": "5 minutes (simulated)" # Add a simulated ETA for a real-world feel
            }
        
        return response
        
    except Exception as e:
        print(f"[ORCHESTRATOR] Error fetching status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error.")
        
    finally:
        put_db_connection(conn)