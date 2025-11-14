import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import os
import random
import time

app = FastAPI()

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "chiragb07")
DB_PORT = os.environ.get("DB_PORT", "5432")
# ------------------------------

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
    status: str = DriverStatus.accepting.value # Included for full payload validation

# --- Database Pool and Connection Functions ---

def get_db_connection():
    """Retrieves a connection from the pool."""
    global db_pool
    if db_pool:
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
        db_pool = None 

def create_initial_tables():
    """Creates all necessary tables for the system if they don't exist."""
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # 1. Create drivers table (Updated with current_location for matching)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id SERIAL PRIMARY KEY,
                driver_id VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'accepting',
                current_location VARCHAR(255) NOT NULL
            );
        """)
        
        # 2. Create users/ride_requests table (Stores ride requests/status)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                source_location VARCHAR(255) NOT NULL,
                destination_location VARCHAR(255) NOT NULL,
                request_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                request_status VARCHAR(50) DEFAULT 'pending',
                match_time TIMESTAMP WITH TIME ZONE NULL,
                completion_time TIMESTAMP WITH TIME ZONE NULL,
                driver_fk_id INTEGER REFERENCES drivers(id) NULL
            );
        """)
        
        # 3. Create events table (NEW: For Organizer listings)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                organizer_id VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                venue_location VARCHAR(255) NOT NULL,
                event_time TIMESTAMP WITH TIME ZONE NOT NULL,
                promo_code VARCHAR(50) NULL,
                discount_rate NUMERIC(3, 2) DEFAULT 0.00,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)
        
        # 4. Create event_bookings table (NEW: Tracks rides booked via events)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_bookings (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                event_fk_id INTEGER REFERENCES events(id) NOT NULL,
                to_event_ride_fk_id INTEGER REFERENCES users(id) NULL,
                from_event_ride_fk_id INTEGER REFERENCES users(id) NULL,
                booking_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                trip_type VARCHAR(50) NOT NULL -- 'one-way' or 'round-trip'
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
    """Adds a new driver to the system with 'accepting' status (used by driver simulator)."""
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
    This is hit by the User Producer/Stress Test and eventually the Event Frontend.
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
                "eta": "5 minutes (simulated)"
            }
        
        return response
        
    except Exception as e:
        print(f"[ORCHESTRATOR] Error fetching status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error.")
        
    finally:
        put_db_connection(conn)

@app.post("/api/events/book-ride")
async def book_event_ride(user_id: str, event_id: int, user_source: str, trip_type: str = "round-trip"):
    """
    NEW ENDPOINT: Books a ride associated with an event. This handles the 'back-to-home' trip logic.
    This endpoint is designed to be hit by the event_server.py or the frontend.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")

    try:
        cursor = conn.cursor()

        # 1. Get Event Details (specifically the venue location)
        cursor.execute(
            "SELECT venue_location FROM events WHERE id = %s AND is_active = TRUE", 
            (event_id,)
        )
        event_venue = cursor.fetchone()
        if not event_venue:
            raise HTTPException(status_code=404, detail="Event not found or is inactive.")
        
        venue_location = event_venue[0]
        to_event_ride_id = None
        from_event_ride_id = None
        
        # 2. Book the RIDE TO the event (User Source -> Venue)
        cursor.execute(
            """
            INSERT INTO users (user_id, source_location, destination_location, request_status)
            VALUES (%s, %s, %s, %s) RETURNING id;
            """,
            (user_id, user_source, venue_location, RideStatus.pending.value)
        )
        to_event_ride_id = cursor.fetchone()[0]

        # 3. Book the RIDE FROM the event (Venue -> User Source) - ONLY if round-trip
        if trip_type == "round-trip":
            cursor.execute(
                """
                INSERT INTO users (user_id, source_location, destination_location, request_status)
                VALUES (%s, %s, %s, %s) RETURNING id;
                """,
                (user_id, venue_location, user_source, RideStatus.pending.value)
            )
            from_event_ride_id = cursor.fetchone()[0]

        # 4. Log the event booking
        cursor.execute(
            """
            INSERT INTO event_bookings (user_id, event_fk_id, to_event_ride_fk_id, from_event_ride_fk_id, trip_type)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (user_id, event_id, to_event_ride_id, from_event_ride_id, trip_type)
        )
        
        conn.commit()

        return {
            "message": "Round-trip ride booked successfully.",
            "event_id": event_id,
            "ride_to_id": to_event_ride_id,
            "ride_from_id": from_event_ride_id,
            "trip_type": trip_type
        }
        
    except HTTPException:
        conn.rollback()
        raise # Re-raise 404
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[ORCHESTRATOR] Event Booking failed: {e}")
        raise HTTPException(status_code=500, detail="Database failure during booking.")
    finally:
        put_db_connection(conn)