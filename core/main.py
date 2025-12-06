import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import hashlib

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

db_pool = None

class AuthModel(BaseModel):
    username: str
    password: str
    role: str
    interest: Optional[str] = None

class UserRequest(BaseModel):
    user_id: str
    source_lat: float
    source_lng: float
    dest_lat: float
    dest_lng: float
    source_name: str
    dest_name: str

class DriverRegistration(BaseModel):
    driver_id: str
    name: str
    current_lat: float
    current_lng: float
    current_loc_name: str
    status: str

def get_db_connection():
    if db_pool: return db_pool.getconn()
    return None

def put_db_connection(conn):
    if db_pool and conn: db_pool.putconn(conn)

def initialize_db_pool():
    global db_pool
    try:
        db_pool = SimpleConnectionPool(minconn=1, maxconn=10, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        create_initial_tables()
    except Exception as e:
        print(f"DB Error: {e}")

def create_initial_tables():
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        # Tables (Auth, Drivers, Users, Events, Bookings)
        cursor.execute("CREATE TABLE IF NOT EXISTS auth_users (username VARCHAR(255) PRIMARY KEY, password_hash VARCHAR(255) NOT NULL, role VARCHAR(50) NOT NULL, interest VARCHAR(50));")
        cursor.execute("CREATE TABLE IF NOT EXISTS drivers (id SERIAL PRIMARY KEY, driver_id VARCHAR(255) UNIQUE NOT NULL, name VARCHAR(255) NOT NULL, status VARCHAR(50) DEFAULT 'accepting', current_lat DOUBLE PRECISION, current_lng DOUBLE PRECISION, current_loc_name VARCHAR(255));")
        cursor.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL, source_lat DOUBLE PRECISION, source_lng DOUBLE PRECISION, dest_lat DOUBLE PRECISION, dest_lng DOUBLE PRECISION, source_name VARCHAR(255), dest_name VARCHAR(255), request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, request_status VARCHAR(50) DEFAULT 'pending', match_time TIMESTAMP NULL, completion_time TIMESTAMP NULL, driver_fk_id INTEGER REFERENCES drivers(id) NULL);")
        cursor.execute("CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, organizer_id VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, venue_name VARCHAR(255) NOT NULL, venue_lat DOUBLE PRECISION, venue_lng DOUBLE PRECISION, event_time TIMESTAMP NOT NULL, ticket_price NUMERIC(10, 2) DEFAULT 0.00, total_capacity INTEGER DEFAULT 100, tickets_sold INTEGER DEFAULT 0, bid_amount NUMERIC(10, 2) DEFAULT 0.00, event_type VARCHAR(50) NOT NULL, is_active BOOLEAN DEFAULT TRUE);")
        cursor.execute("CREATE TABLE IF NOT EXISTS event_bookings (id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL, event_fk_id INTEGER REFERENCES events(id) NOT NULL, to_event_ride_fk_id INTEGER REFERENCES users(id) NULL, from_event_ride_fk_id INTEGER REFERENCES users(id) NULL, booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, trip_type VARCHAR(50) NOT NULL);")
        conn.commit()
    finally:
        db_pool.putconn(conn)

initialize_db_pool()

def hash_password(password: str):
    return hashlib.sha256(password.encode()).hexdigest()

@app.post("/api/auth/signup")
async def signup(auth: AuthModel):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        pwd_hash = hash_password(auth.password)
        cursor.execute("INSERT INTO auth_users (username, password_hash, role, interest) VALUES (%s, %s, %s, %s) RETURNING username", (auth.username, pwd_hash, auth.role, auth.interest))
        conn.commit()
        return {"message": "User created", "username": auth.username, "interest": auth.interest}
    except psycopg2.IntegrityError:
        conn.rollback(); raise HTTPException(400, "Username taken")
    finally: put_db_connection(conn)

@app.post("/api/auth/login")
async def login(auth: AuthModel):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        pwd_hash = hash_password(auth.password)
        cursor.execute("SELECT role, interest FROM auth_users WHERE username = %s AND password_hash = %s", (auth.username, pwd_hash))
        result = cursor.fetchone()
        if not result: raise HTTPException(401, "Invalid credentials")
        return {"message": "Login successful", "username": auth.username, "role": result[0], "interest": result[1]}
    finally: put_db_connection(conn)

@app.post("/api/register-driver")
async def register_driver(d: DriverRegistration):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO drivers (driver_id, name, status, current_lat, current_lng, current_loc_name) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (driver_id) DO UPDATE SET name=EXCLUDED.name, status=EXCLUDED.status, current_lat=EXCLUDED.current_lat, current_lng=EXCLUDED.current_lng, current_loc_name=EXCLUDED.current_loc_name RETURNING id", (d.driver_id, d.name, d.status, d.current_lat, d.current_lng, d.current_loc_name))
        conn.commit()
        return {"message": "Driver registered"}
    finally: put_db_connection(conn)

@app.post("/api/request-ride")
async def handle_ride_request(r: UserRequest):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (user_id, source_lat, source_lng, dest_lat, dest_lng, source_name, dest_name, request_status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending') RETURNING id", (r.user_id, r.source_lat, r.source_lng, r.dest_lat, r.dest_lng, r.source_name, r.dest_name))
        req_id = cursor.fetchone()[0]
        conn.commit()
        return {"message": "Ride requested", "request_id": req_id, "status": "pending"}
    finally: put_db_connection(conn)

@app.get("/api/ride-status/{request_id}")
async def get_ride_status(request_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT u.request_status, u.source_name, u.dest_name, d.name, d.driver_id, d.current_loc_name FROM users u LEFT JOIN drivers d ON u.driver_fk_id = d.id WHERE u.id = %s", (request_id,))
        row = cursor.fetchone()
        if not row: raise HTTPException(404, "Ride not found")
        status, src, dest, d_name, d_id, d_loc = row
        res = {"request_id": request_id, "status": status, "source": src, "destination": dest}
        if d_name: res["driver_info"] = {"name": d_name, "driver_id": d_id, "current_location": d_loc}
        return res
    finally: put_db_connection(conn)

# --- UPDATED BOOKING LOGIC ---
@app.post("/api/events/book-ride")
async def book_event_ride(user_id: str, event_id: int, user_lat: float, user_lng: float, user_source_name: str, trip_type: str = "round-trip", ticket_qty: int = 1):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Lock row for update
        cursor.execute("SELECT venue_lat, venue_lng, venue_name, total_capacity, tickets_sold, ticket_price FROM events WHERE id = %s FOR UPDATE", (event_id,))
        event = cursor.fetchone()
        if not event: raise HTTPException(404, "Event not found")
        v_lat, v_lng, v_name, cap, sold, price = event
        
        # Check Capacity
        if sold + ticket_qty > cap: 
            raise HTTPException(400, f"Not enough tickets. Only {cap - sold} left.")

        # Book Ride
        cursor.execute("INSERT INTO users (user_id, source_lat, source_lng, dest_lat, dest_lng, source_name, dest_name, request_status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending') RETURNING id", (user_id, user_lat, user_lng, v_lat, v_lng, user_source_name, v_name))
        to_id = cursor.fetchone()[0]
        
        from_id = None
        if trip_type == "round-trip":
            cursor.execute("INSERT INTO users (user_id, source_lat, source_lng, dest_lat, dest_lng, source_name, dest_name, request_status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'scheduled') RETURNING id", (user_id, v_lat, v_lng, user_lat, user_lng, v_name, user_source_name))
            from_id = cursor.fetchone()[0]
            
        # Update Tickets
        cursor.execute("UPDATE events SET tickets_sold = tickets_sold + %s WHERE id = %s", (ticket_qty, event_id))
        cursor.execute("INSERT INTO event_bookings (user_id, event_fk_id, to_event_ride_fk_id, from_event_ride_fk_id, trip_type) VALUES (%s, %s, %s, %s, %s)", (user_id, event_id, to_id, from_id, trip_type))
        
        conn.commit()
        total_cost = float(price) * ticket_qty
        return {"message": "Booked!", "ride_to_id": to_id, "ride_from_id": from_id, "total_cost": total_cost}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally: put_db_connection(conn)

@app.post("/api/ride/activate/{ride_id}")
async def activate_ride(ride_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET request_status = 'pending', request_time = NOW() WHERE id = %s AND request_status = 'scheduled'", (ride_id,))
        if cursor.rowcount == 0: raise HTTPException(400, "Ride not found/scheduled")
        conn.commit()
        return {"status": "pending"}
    finally: put_db_connection(conn)

@app.get("/api/driver/pending-rides")
async def get_pending_rides():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, source_name, dest_name, source_lat, source_lng FROM users WHERE request_status = 'pending'")
        rides = [{"id": r[0], "user": r[1], "from": r[2], "to": r[3], "lat": r[4], "lng": r[5]} for r in cursor.fetchall()]
        return {"rides": rides}
    finally: put_db_connection(conn)

@app.post("/api/driver/accept-ride")
async def driver_accept_ride(ride_id: int, driver_id: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM drivers WHERE driver_id = %s", (driver_id,))
        if not cursor.fetchone(): raise HTTPException(404, "Driver not found")
        cursor.execute("UPDATE users SET request_status = 'matched', driver_fk_id = (SELECT id FROM drivers WHERE driver_id = %s), match_time = NOW() WHERE id = %s AND request_status = 'pending' RETURNING id", (driver_id, ride_id))
        if cursor.rowcount == 0: raise HTTPException(400, "Ride taken")
        cursor.execute("UPDATE drivers SET status = 'in a drive' WHERE driver_id = %s", (driver_id,))
        conn.commit()
        return {"status": "success"}
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: put_db_connection(conn)

@app.post("/api/driver/complete-ride")
async def driver_complete_ride(ride_id: int, driver_id: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET request_status = 'completed' WHERE id = %s", (ride_id,))
        cursor.execute("UPDATE drivers SET status = 'accepting' WHERE driver_id = %s", (driver_id,))
        conn.commit()
        return {"status": "completed"}
    finally: put_db_connection(conn)

@app.get("/api/driver/my-ride/{driver_id}")
async def get_driver_current_ride(driver_id: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT u.id, u.user_id, u.source_name, u.dest_name FROM users u JOIN drivers d ON u.driver_fk_id = d.id WHERE d.driver_id = %s AND u.request_status = 'matched'", (driver_id,))
        ride = cursor.fetchone()
        if ride: return {"active_ride": {"id": ride[0], "user": ride[1], "from": ride[2], "to": ride[3]}}
        return {"active_ride": None}
    finally: put_db_connection(conn)