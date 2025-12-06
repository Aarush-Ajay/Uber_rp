import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush") 
DB_PORT = os.environ.get("DB_PORT", "5432")

ORCHESTRATOR_URL = "http://127.0.0.1:8000"
db_pool = None

class EventCreate(BaseModel):
    organizer_id: str
    name: str
    venue_name: str
    venue_lat: float
    venue_lng: float
    event_time: str
    ticket_price: float
    total_capacity: int
    bid_amount: float = 0.0
    event_type: str
    promo_code: Optional[str] = None
    discount_rate: float = 0.0
    is_active: bool = True

class BookingRequest(BaseModel):
    user_id: str
    event_id: int
    user_lat: float
    user_lng: float
    user_source_name: str
    trip_type: str
    ticket_qty: int = 1

def initialize_db_pool():
    global db_pool
    try:
        db_pool = SimpleConnectionPool(minconn=1, maxconn=5, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
    except Exception as e: print(f"DB Error: {e}")

initialize_db_pool()

@app.post("/api/organizer/events")
async def create_event(e: EventCreate):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO events (organizer_id, name, venue_name, venue_lat, venue_lng, event_time, ticket_price, total_capacity, bid_amount, event_type, promo_code, discount_rate, is_active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (e.organizer_id, e.name, e.venue_name, e.venue_lat, e.venue_lng, e.event_time, e.ticket_price, e.total_capacity, e.bid_amount, e.event_type, e.promo_code, e.discount_rate, e.is_active))
        eid = cursor.fetchone()[0]
        conn.commit()
        return {"message": "Event Created", "event_id": eid}
    except Exception as ex: conn.rollback(); raise HTTPException(500, str(ex))
    finally: db_pool.putconn(conn)

@app.get("/api/events")
async def get_events(interest: Optional[str] = None):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        # REMOVED 'AND tickets_sold < total_capacity' so Organizers see sold out stats
        # Frontend will filter sold out events for Riders
        cursor.execute("""
            SELECT id, name, venue_name, venue_lat, venue_lng, event_time, ticket_price, total_capacity, tickets_sold, bid_amount, event_type, organizer_id
            FROM events 
            WHERE is_active = TRUE
            ORDER BY CASE WHEN event_type = %s THEN 1 ELSE 0 END DESC, bid_amount DESC
        """, (interest,))
        events = []
        for row in cursor.fetchall():
            events.append({
                "id": row[0], "name": row[1], "venue": row[2], "lat": row[3], "lng": row[4],
                "time": str(row[5]), "ticket_price": float(row[6]), "capacity": row[7], "sold": row[8], 
                "bid": float(row[9]), "type": row[10], "organizer_id": row[11]
            })
        return {"events": events}
    finally: db_pool.putconn(conn)

@app.post("/api/events/book")
async def book_event_ride_proxy(b: BookingRequest):
    try:
        url = f"{ORCHESTRATOR_URL}/api/events/book-ride"
        res = requests.post(url, params=b.dict())
        if res.status_code != 200: raise HTTPException(res.status_code, res.json().get("detail", "Booking failed"))
        return res.json()
    except requests.exceptions.ConnectionError: raise HTTPException(503, "Main Server Down")