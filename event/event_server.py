import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from datetime import datetime
import os
import requests
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8088", # Explicitly allow your frontend server port
    "http://127.0.0.1:8088",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --- Configuration ---
# DB settings for direct connection (must be the same as main.py)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "chiragb07")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Main Orchestrator API URL (Port 8000) for booking rides
ORCHESTRATOR_URL = "http://127.0.0.1:8000"
BOOK_RIDE_ENDPOINT = f"{ORCHESTRATOR_URL}/api/events/book-ride"

# Global variable for the connection pool
db_pool = None

# --- Models ---
class EventBase(BaseModel):
    name: str
    venue_location: str
    event_time: datetime
    promo_code: Optional[str] = None
    discount_rate: float = 0.00
    is_active: bool = True

class EventCreate(EventBase):
    organizer_id: str

class EventUpdate(EventBase):
    pass

class EventBookingRequest(BaseModel):
    user_id: str
    event_id: int
    user_source: str
    trip_type: str = "round-trip" # can be "one-way" or "round-trip"

# --- Database Pool and Connection Functions ---

def get_db_connection():
    global db_pool
    if db_pool:
        return db_pool.getconn()
    return None

def put_db_connection(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def initialize_db_pool():
    global db_pool
    print("[EVENT SERVER] Initializing Database Connection Pool...")
    try:
        db_pool = SimpleConnectionPool(
            minconn=1, maxconn=5, host=DB_HOST, database=DB_NAME,
            user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        print("[EVENT SERVER] Database Pool Initialized successfully.")

    except psycopg2.OperationalError as e:
        print(f"[EVENT SERVER] FATAL ERROR: Database connection failed: {e}")
        db_pool = None 

# Initialize the database pool when the application starts
initialize_db_pool()


# --- Event Organizer API Endpoints (PORT 8080) ---

@app.get("/")
async def root():
    return {"message": "Event Management Service is running on Port 8080."}

@app.post("/api/organizer/events", status_code=status.HTTP_201_CREATED)
async def create_event(event_data: EventCreate):
    """Event Organizer: Creates a new event listing."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO events (organizer_id, name, venue_location, event_time, promo_code, discount_rate, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """,
            (
                event_data.organizer_id, event_data.name, event_data.venue_location, 
                event_data.event_time, event_data.promo_code, event_data.discount_rate, event_data.is_active
            )
        )
        new_event_id = cursor.fetchone()[0]
        conn.commit()
        
        print(f"[EVENT SERVER] New event '{event_data.name}' created by {event_data.organizer_id}. ID: {new_event_id}")
        
        return {
            "message": "Event created successfully.",
            "event_id": new_event_id
        }
        
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {e}")
        
    finally:
        put_db_connection(conn)

@app.get("/api/events")
async def get_active_events():
    """Customer: Retrieves all active events for browsing."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT id, name, venue_location, event_time, promo_code, discount_rate 
            FROM events 
            WHERE is_active = TRUE AND event_time > NOW() 
            ORDER BY event_time ASC;
            """
        )
        events = [
            {
                "id": row[0],
                "name": row[1],
                "venue": row[2],
                "time": row[3],
                "promo_code": row[4],
                "discount": float(row[5])
            } 
            for row in cursor.fetchall()
        ]
        
        return {"events": events}
        
    except psycopg2.Error as e:
        print(f"[EVENT SERVER] Database query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve events.")
        
    finally:
        put_db_connection(conn)

@app.post("/api/events/book")
async def book_event_ride_proxy(booking_data: EventBookingRequest):
    """
    Customer: Initiates a ride booking (one-way or round-trip) for a specific event.
    This acts as a proxy, calling the main Orchestrator server's booking logic.
    """
    
    # 1. Call the Main Orchestrator's dedicated booking endpoint
    try:
        response = requests.post(
            BOOK_RIDE_ENDPOINT,
            params={
                "user_id": booking_data.user_id,
                "event_id": booking_data.event_id,
                "user_source": booking_data.user_source,
                "trip_type": booking_data.trip_type
            },
            timeout=5
        )
        response.raise_for_status()
        
        return response.json()
        
    except requests.exceptions.HTTPError as e:
        # Pass the Orchestrator's error status code and message back
        if e.response is not None:
             raise HTTPException(status_code=e.response.status_code, detail=e.response.json().get("detail", "Error processing ride booking."))
        raise HTTPException(status_code=500, detail="Error communicating with Orchestrator service.")
    
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail="Orchestrator Service is unavailable (Port 8000).")

# --- Example Update Endpoint for Organizer Dashboard ---
@app.put("/api/organizer/events/{event_id}")
async def update_event(event_id: int, update_data: EventUpdate):
    """Allows an organizer to update event details (time, discounts, etc.)."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Service Unavailable.")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Failed to get DB connection.")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            UPDATE events SET 
                name = %s, 
                venue_location = %s, 
                event_time = %s, 
                promo_code = %s, 
                discount_rate = %s, 
                is_active = %s
            WHERE id = %s;
            """,
            (
                update_data.name, update_data.venue_location, update_data.event_time, 
                update_data.promo_code, update_data.discount_rate, update_data.is_active, event_id
            )
        )
        conn.commit()
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Event not found.")
            
        print(f"[EVENT SERVER] Event ID {event_id} updated successfully.")
        return {"message": "Event updated successfully."}
        
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database update failed: {e}")
        
    finally:
        put_db_connection(conn)