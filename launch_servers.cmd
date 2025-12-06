@echo off
ECHO Starting Main Server (8000) and Event Server (8080) in background...

REM Set Environment Variables (Required for DB access)
set DB_PASS=Aarush
set DB_HOST=localhost
set DB_NAME=Uber_rp
set DB_USER=postgres
set DB_PORT=5432

REM Launch Main Server (8000) - Start
start "" python -m uvicorn main:app --reload --port 8000

REM Launch Event Server (8080) - Start
start "" python -m uvicorn event.event_server:app --reload --port 8080

ECHO Servers launched.
Check the new terminal windows for logs.