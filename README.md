# Uber_rp: Ride-Sharing & Event Backend Architecture

A full-stack prototype for an enterprise-grade ride-sharing platform with integrated event-driven transportation workflows. This project demonstrates advanced backend architecture, real-time ride matching, event inventory management, and multi-role access patterns—all powered by FastAPI and PostgreSQL.

## 🚀 Project Overview

**Uber_rp** models a sophisticated transportation ecosystem combining:

- **Ride-Sharing Core**: Dynamic rider request management, real-time driver matching, and trip lifecycle tracking
- **Event Integration Layer**: Event creation, ticketed bookings, and venue-based ride coordination
- **High-Performance Backend**: FastAPI-based orchestration with PostgreSQL persistence and connection pooling
- **Multi-Role System**: Seamless workflows for riders, drivers, and event organizers
- **Scalability-Ready Architecture**: Worker-based processing, background job handling, and simulation tooling for load testing

This is an operational prototype showcasing production-grade design patterns and best practices for shared-mobility platforms.

## ✨ Key Features

### Core Ride-Sharing
- ✅ User authentication with role-based access control (rider, driver, organizer)
- ✅ Real-time ride request creation and status tracking
- ✅ Driver registration and dynamic availability management
- ✅ Intelligent ride matching and assignment logic
- ✅ Ride completion workflows and trip history

### Event Transportation
- ✅ Event creation with venue location and capacity management
- ✅ Ticketed event bookings with dynamic pricing
- ✅ Round-trip and one-way event travel options
- ✅ Inventory optimization for demand-driven events
- ✅ Promo codes and discount rate support

### Advanced Architecture
- ✅ Connection pooling for optimized database performance
- ✅ Worker-based asynchronous matching and completion
- ✅ Simulation and stress-testing tools for platform validation
- ✅ Bulk driver generation for realistic load scenarios
- ✅ Proxy-based service composition between core and event layers

## 📋 Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend Framework** | FastAPI (Python 3.9+) |
| **Server** | Uvicorn with auto-reload |
| **Database** | PostgreSQL with psycopg2 connection pooling |
| **Data Validation** | Pydantic |
| **Inter-Service Communication** | HTTP/REST with Requests |
| **Frontend** | HTML5 + JavaScript (vanilla) |
| **Deployment Helper** | Windows batch scripts (extensible) |

## 📁 Repository Structure

```
Uber_rp/
│
├── 📂 core/
│   └── main.py                      # FastAPI main orchestrator
│                                    # - Ride request handling
│                                    # - Driver lifecycle
│                                    # - Real-time matching
│                                    # - DB connection pool
│
├── 📂 events/
│   ├── event_server.py              # Event service microservice
│   │                                # - Event CRUD operations
│   │                                # - Event discovery & filtering
│   │                                # - Booking proxying to core
│   └── event_frontend.html          # Event UI (venue, pricing, capacity)
│
├── 📂 driver_app/
│   └── driver_frontend.html         # Legacy driver interface
│
├── 📂 frontend/
│   ├── config.js                    # API endpoint configuration
│   ├── index.html                   # Main shell/router
│   │
│   ├── 📂 driver/
│   │   └── index.html               # Driver dashboard
│   │                                # - Pending rides display
│   │                                # - Accept/complete actions
│   │                                # - Trip details
│   │
│   ├── 📂 rider/
│   │   └── index.html               # Rider booking interface
│   │                                # - Trip creation form
│   │                                # - Status tracking
│   │                                # - Event discovery
│   │
│   └── 📂 organiser/
│       └── index.html               # Event organizer dashboard
│                                    # - Event creation wizard
│                                    # - Capacity & pricing config
│                                    # - Booking management
│
├── 📂 workers/
│   ├── matchmaking.py               # Async ride matching logic
│   │                                # - Spatial proximity matching
│   │                                # - Driver availability filtering
│   │                                # - Match ranking
│   │
│   └── ridecompletion.py            # Trip completion workflow
│                                    # - State machine orchestration
│                                    # - Payment processing flow
│                                    # - Rating & feedback handling
│
├── 📂 simulation/
│   ├── bulk_driver.py               # Bulk driver generation tool
│   │                                # - Batch registration
│   │                                # - Random location seeding
│   │                                # - Load simulation
│   │
│   └── stress.py                    # Platform stress testing
│                                    # - Concurrent ride requests
│                                    # - Driver acceptance simulation
│                                    # - Performance metrics
│
├── 📂 venv/                         # Python virtual environment
│
├── launch_servers.cmd               # Windows multi-server launcher
├── reset_db.py                      # PostgreSQL schema initialization
├── .gitignore
└── README.md                        # This file
```

## 🏗️ Architecture & Design

### Microservices Model

The project follows a lightweight microservices architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Browser Clients                           │
│        (Rider / Driver / Organizer Frontends)               │
└────────────┬────────────────────────────────────────────────┘
             │
      ┌──────┴──────┬──────────────┐
      │             │              │
      ▼             ▼              ▼
┌──────────┐  ┌──────────┐  ┌──────────────┐
│  Rider   │  │  Driver  │  │  Organizer   │
│    UI    │  │    UI    │  │      UI      │
│(8000)    │  │(8000)    │  │   (8080)     │
└────┬─────┘  └────┬─────┘  └──────┬───────┘
     │             │               │
     └─────────────┼───────────────┘
                   │
        ┌──────────▼──────────┐
        │   Core API (8000)   │────────────────┐
        │  - Orchestrator     │                │
        │  - Ride Management  │                │
        │  - Driver Logic     │                │
        │  - Auth & Matching  │                │
        └──────────┬──────────┘                │
                   │                           │
        ┌──────────▼──────────┐  ┌─────────────▼──────────┐
        │   PostgreSQL DB     │  │  Event API (8080)      │
        │  - auth_users       │  │  - Event CRUD          │
        │  - drivers          │  │  - Event Filtering     │
        │  - users (rides)    │  │  - Booking Proxy       │
        │  - events           │  │  - Inventory Mgmt      │
        │  - event_bookings   │  └────────────┬───────────┘
        └─────────────────────┘               │
                   ▲                           │
                   └───────────────────────────┘
```

### Database Schema

**Core Tables:**

- `auth_users` — User credentials, roles (rider/driver/organizer), and interests
- `drivers` — Driver profiles, availability status, and location tracking
- `users` — Ride requests with source/destination coordinates and statuses
- `events` — Event details: venue, capacity, pricing, bid amounts
- `event_bookings` — Links users to events with trip type (round-trip/one-way)

**Key Design Patterns:**

- Foreign key constraints for referential integrity
- Indexed lookups on status fields for efficient filtering
- Timestamp tracking for audit and analytics
- Connection pooling via `psycopg2.SimpleConnectionPool` for performance

## 🔌 Core API Endpoints

### Authentication
- `POST /api/auth/signup` — Register new user (rider, driver, organizer)
- `POST /api/auth/login` — Authenticate and return role/interests

### Rider Operations
- `POST /api/request-ride` — Create a new ride request
- `GET /api/ride-status/{request_id}` — Poll ride status and driver assignment
- `POST /api/events/book-ride` — Book a ride linked to an event

### Driver Operations
- `POST /api/register-driver` — Register driver availability
- `GET /api/driver/pending-rides` — List all pending (unmatched) rides
- `POST /api/driver/accept-ride` — Claim a ride request
- `POST /api/driver/complete-ride` — Mark ride as completed
- `GET /api/driver/my-ride/{driver_id}` — Get current assigned ride

### Event Operations (via Event Service on port 8080)
- `POST /api/organizer/events` — Create an event with venue/pricing
- `GET /api/events` — Discover events (filterable by interest)
- `POST /api/events/book` — Book rides to/from event venues

## 🚀 Getting Started

### Prerequisites

Ensure you have:

- **Python 3.9+** installed
- **PostgreSQL** server running locally (or remote with connection config)
- **pip** for package management
- A PostgreSQL user and database (default: `Uber_rp` database, `postgres` user, password `Aarush`)

### Step 1: Clone & Set Up Environment

```bash
git clone https://github.com/Aarush-Ajay/Uber_rp.git
cd Uber_rp
```

### Step 2: Create Virtual Environment

**On Windows:**
```powershell
python -m venv venv
.\venv\Scripts\activate
```

**On macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install fastapi uvicorn psycopg2-binary pydantic requests
```

Or, if a `requirements.txt` exists:
```bash
pip install -r requirements.txt
```

### Step 4: Initialize PostgreSQL Database

Ensure PostgreSQL is running, then initialize the schema:

```bash
python reset_db.py
```

This script creates the `events` and `event_bookings` tables with the full schema.

> **Note:** If tables already exist for `drivers` and `users`, they are preserved. Only `event_bookings` and `events` are reset.

### Step 5: Configure Environment (Optional)

By default, the code connects to:
```
DB_HOST=localhost
DB_NAME=Uber_rp
DB_USER=postgres
DB_PASS=Aarush
DB_PORT=5432
```

To override, set environment variables before launching servers:

```bash
# Windows (PowerShell)
$env:DB_PASS = "your_password"
$env:DB_HOST = "your_host"

# Linux/macOS
export DB_PASS="your_password"
export DB_HOST="your_host"
```

## ▶️ Running the Services

### Option A: Windows Batch Launcher (Recommended for Windows)

```powershell
.\launch_servers.cmd
```

This launches two terminal windows:
- Main server on `http://127.0.0.1:8000` (core API + UI)
- Event server on `http://127.0.0.1:8080` (event API)

### Option B: Manual Launch (All Platforms)

**Terminal 1 — Core Service:**
```bash
cd core
uvicorn main:app --reload --port 8000
```

**Terminal 2 — Event Service:**
```bash
cd events
uvicorn event_server:app --reload --port 8080
```

Both services will auto-reload on code changes (due to `--reload` flag).

### Verify Services Are Running

- Core API: `http://localhost:8000/docs` (Swagger UI)
- Event API: `http://localhost:8080/docs` (Swagger UI)

### Access the Frontend

- **Main UI**: Open `http://localhost:8000/` in a browser
- **Driver Dashboard**: Navigate to driver login via the main UI
- **Rider Booking**: Navigate to rider login via the main UI
- **Organizer Console**: Navigate to organizer login and event creation

## 📊 Typical User Flow

### 1. Signup & Registration

```
User (any role)
  ↓
POST /api/auth/signup {"username", "password", "role", "interest"}
  ↓
✅ User Created → Redirected to login
```

### 2. Driver Availability

```
Driver
  ↓
POST /api/register-driver {"driver_id", "name", "lat", "lng", "current_loc_name"}
  ↓
✅ Driver Ready to Accept Rides
```

### 3. Rider Request & Matching

```
Rider
  ↓
POST /api/request-ride {"user_id", "source_lat/lng", "dest_lat/lng", "source_name", "dest_name"}
  ↓
Request Created (status: "pending")
  ↓
GET /api/driver/pending-rides (pulled by driver app periodically)
  ↓
Driver Selects Ride
  ↓
POST /api/driver/accept-ride
  ↓
Request Status → "matched" + driver info assigned
  ↓
Rider Polls GET /api/ride-status/{request_id}
  ↓
✅ Ride In Progress
  ↓
POST /api/driver/complete-ride
  ↓
Status → "completed"
```

### 4. Event Booking Flow

```
Organizer
  ↓
POST /api/organizer/events {"name", "venue_lat/lng", "ticket_price", "total_capacity", "event_time"}
  ↓
Event Created (e.g., concert, conference, sports event)
  ↓
Rider Views Events
  ↓
GET /api/events (filtered by interest)
  ↓
Rider Books Event Ride
  ↓
POST /api/events/book-ride {"user_id", "event_id", "trip_type": "round-trip", "ticket_qty"}
  ↓
✅ Two Rides Created (to-event + from-event)
  ↓
✅ Event Booking Confirmed
  ↓
Matching Process Triggers → Drivers Accept Rides
```

## 🧪 Testing & Simulation

### Bulk Driver Generation

Populate your platform with simulated drivers for load testing:

```bash
python simulation/bulk_driver.py
```

This script:
- Generates N random drivers
- Seeds them across geographic coordinates
- Registers each via the Core API
- Validates successful registration

### Stress Testing

Run concurrent ride requests and driver actions:

```bash
python simulation/stress.py
```

This script:
- Creates multiple simultaneous ride requests
- Simulates driver acceptance patterns
- Tracks response times and success rates
- Helps identify bottlenecks and capacity limits

## 🔐 Security Considerations

> ⚠️ **This is a prototype project.** Do not use in production without addressing:

1. **Credentials**: Hardcoded database password in code → Move to `.env` + secrets manager
2. **Password Hashing**: SHA-256 only → Use `bcrypt` or `argon2`
3. **Authentication**: No JWT/OAuth → Implement token-based auth
4. **Authorization**: No RBAC middleware → Add permission checks per endpoint
5. **CORS**: Wildcard `allow_origins=["*"]` → Restrict to known domains
6. **Input Validation**: Basic Pydantic models → Add custom validators & sanitization
7. **Rate Limiting**: None → Implement per-user/per-IP rate limiting
8. **HTTPS**: Not configured → Use TLS/SSL in production
9. **Database**: No encryption → Enable PostgreSQL SSL connections

## 📈 Performance & Scalability

### Current Design
- **Connection Pooling**: `SimpleConnectionPool` with configurable min/max connections
- **Query Optimization**: Indexed lookups on status and user_id fields
- **Lock Management**: Uses PostgreSQL `FOR UPDATE` on event bookings to prevent race conditions

### Future Improvements
1. **Caching Layer**: Redis for frequently-accessed events and driver status
2. **Message Queue**: RabbitMQ/Kafka for async matching and notifications
3. **Load Balancing**: Nginx/HAProxy to distribute traffic
4. **Distributed Workers**: Celery for background tasks
5. **Geospatial Indexing**: PostGIS for efficient location-based queries
6. **API Gateway**: Kong or AWS API Gateway for rate limiting, logging, and auth
7. **Monitoring & Observability**: Prometheus + Grafana for metrics, ELK stack for logs

## 🛠️ Development Workflow

### Adding a New Endpoint

1. Define Pydantic model in `core/main.py`:
   ```python
   class MyRequest(BaseModel):
       field1: str
       field2: float
   ```

2. Create the endpoint:
   ```python
   @app.post("/api/my-endpoint")
   async def my_endpoint(req: MyRequest):
       conn = get_db_connection()
       try:
           cursor = conn.cursor()
           # Query logic here
           conn.commit()
           return {"result": "..."}
       finally:
           put_db_connection(conn)
   ```

3. Test via Swagger UI: `http://localhost:8000/docs`

### Modifying the Database Schema

1. Edit the table creation SQL in `reset_db.py`
2. Run `python reset_db.py` to reinitialize (⚠️ this drops existing tables)
3. Consider using a migration tool (Alembic) for production

### Debugging

- Enable logging in FastAPI:
  ```python
  import logging
  logging.basicConfig(level=logging.DEBUG)
  ```

- Check database logs:
  ```bash
  SELECT * FROM pg_stat_statements;  -- PostgreSQL
  ```

- Use Swagger interactive docs to test endpoints in real-time

## 📚 Project Files Reference

| File | Purpose |
|------|---------|
| `core/main.py` | FastAPI orchestrator; all ride & auth logic |
| `events/event_server.py` | Event microservice; CRUD & booking proxy |
| `workers/matchmaking.py` | Ride matching algorithm & worker logic |
| `workers/ridecompletion.py` | Ride completion state machine |
| `simulation/bulk_driver.py` | Load testing utility for drivers |
| `simulation/stress.py` | Concurrency stress testing tool |
| `reset_db.py` | PostgreSQL schema initialization |
| `launch_servers.cmd` | Windows batch helper to launch services |
| `frontend/config.js` | API endpoint URLs for frontend |
| `frontend/index.html` | Main UI shell & router |
| `frontend/rider/index.html` | Rider booking interface |
| `frontend/driver/index.html` | Driver dashboard |
| `frontend/organiser/index.html` | Event organizer console |

## 📝 Example Curl Commands

### Sign Up as Rider
```bash
curl -X POST "http://localhost:8000/api/auth/signup" \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"pass123","role":"rider","interest":"sports"}'
```

### Register Driver
```bash
curl -X POST "http://localhost:8000/api/register-driver" \
  -H "Content-Type: application/json" \
  -d '{"driver_id":"d001","name":"Bob","current_lat":40.7128,"current_lng":-74.0060,"current_loc_name":"NYC"}'
```

### Request a Ride
```bash
curl -X POST "http://localhost:8000/api/request-ride" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":"alice",
    "source_lat":40.7128,
    "source_lng":-74.0060,
    "dest_lat":40.7580,
    "dest_lng":-73.9855,
    "source_name":"Times Square",
    "dest_name":"Central Park"
  }'
```

### Check Ride Status
```bash
curl "http://localhost:8000/api/ride-status/1"
```

### Get Pending Rides (Driver View)
```bash
curl "http://localhost:8000/api/driver/pending-rides"
```

### Driver Accepts Ride
```bash
curl -X POST "http://localhost:8000/api/driver/accept-ride" \
  -H "Content-Type: application/json" \
  -d '{"ride_id":1,"driver_id":"d001"}'
```

### Create an Event
```bash
curl -X POST "http://localhost:8080/api/organizer/events" \
  -H "Content-Type: application/json" \
  -d '{
    "organizer_id":"org001",
    "name":"Tech Conference 2024",
    "venue_name":"Convention Center",
    "venue_lat":40.7489,
    "venue_lng":-73.9680,
    "event_time":"2024-06-15T09:00:00",
    "ticket_price":99.99,
    "total_capacity":500,
    "event_type":"conference"
  }'
```

### List Events
```bash
curl "http://localhost:8080/api/events?interest=conference"
```

## 🤝 Contributing

We welcome contributions! Areas for enhancement:

- **Frontend Modernization**: React/Vue.js migration for rich UX
- **Mobile App**: React Native or Flutter clients
- **Real-Time Features**: WebSocket support for live driver tracking
- **Payment Integration**: Stripe/PayPal for ticketing and rides
- **Analytics**: Event-driven dashboards and business intelligence
- **Internationalization**: Multi-language support
- **API Documentation**: OpenAPI/Swagger enhancements

Please fork the repository, create a feature branch, and submit a pull request.

## 📄 License

This project does not currently specify a license. Before using or distributing, check the repository settings and reach out to the maintainer.

## 📞 Support & Questions

For issues, feature requests, or questions:
- Open a GitHub issue with detailed context
- Reach out to the project maintainer

## 🎯 Roadmap

**Phase 1 (Current):** Core ride-sharing + event integration
**Phase 2:** Real-time notifications, WebSocket tracking
**Phase 3:** Payment processing, ratings/reviews
**Phase 4:** ML-based matching, surge pricing
**Phase 5:** Mobile apps, analytics dashboard, admin portal

---

**Built with ❤️ for the shared mobility ecosystem.**

*Last updated: September 2024*
