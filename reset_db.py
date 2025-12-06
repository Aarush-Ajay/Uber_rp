import psycopg2
import os

# --- Configuration ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Uber_rp")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Aarush") 
DB_PORT = os.environ.get("DB_PORT", "5432")

def reset_database():
    print("--- STARTING DATABASE RESET ---")
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()

        print("1. Dropping old 'events' table...")
        # Cascade deletes bookings associated with events too, to prevent orphans
        cursor.execute("DROP TABLE IF EXISTS event_bookings CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS events CASCADE;")

        print("2. Recreating 'events' table with NEW columns...")
        cursor.execute("""
            CREATE TABLE events (
                id SERIAL PRIMARY KEY,
                organizer_id VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                
                -- NEW LOCATION FIELDS
                venue_name VARCHAR(255) NOT NULL,
                venue_lat DOUBLE PRECISION NOT NULL,
                venue_lng DOUBLE PRECISION NOT NULL,
                
                event_time TIMESTAMP WITH TIME ZONE NOT NULL,
                
                -- NEW MONETIZATION FIELDS
                ticket_price NUMERIC(10, 2) DEFAULT 0.00,
                total_capacity INTEGER DEFAULT 100,
                tickets_sold INTEGER DEFAULT 0,
                bid_amount NUMERIC(10, 2) DEFAULT 0.00,
                
                -- NEW METADATA
                event_type VARCHAR(50) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                
                promo_code VARCHAR(50) NULL,
                discount_rate NUMERIC(3, 2) DEFAULT 0.00
            );
        """)
        
        print("3. Recreating 'event_bookings' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_bookings (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                event_fk_id INTEGER REFERENCES events(id) NOT NULL,
                to_event_ride_fk_id INTEGER REFERENCES users(id) NULL,
                from_event_ride_fk_id INTEGER REFERENCES users(id) NULL,
                booking_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                trip_type VARCHAR(50) NOT NULL
            );
        """)

        print("✅ SUCCESS: Database schema is now up to date!")
        conn.close()

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")

if __name__ == "__main__":
    reset_database()