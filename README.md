#Job Queue Assignment

This is a small job-queue system built using Go (Golang), gRPC, and PostgreSQL.

The system basically has:
A gRPC server to receive and track jobs
A worker process that keeps checking for pending jobs, leases them, processes them, handles retries, and pushes failed jobs to a DLQ
A PostgreSQL database where jobs are stored permanently
A small dashboard UI (HTML + JS) to see metrics and failed jobs
Simple Go tests to check the main API logic

Tech Stack Used

Go 1.21+
gRPC + Protocol Buffers
PostgreSQL
pgx (Postgres driver)
HTML + JavaScript (only for dashboard)

Steps to run the project:

Make sure Postgres is installed and running on: localhost:5432

Create database and table inside the database

psql -U postgres
CREATE DATABASE jobqueue;
psql -U postgres -d jobqueue -f internal/db/schema.sql
psql -U postgres -d jobqueue

Update db connection string in both server and worker main.go.
connString := "postgres://postgres:<YOUR_PASSWORD>@localhost:5432/jobqueue?sslmode=disable"

Start the server
go run ./cmd/server

you should see
gRPC JobService server listening on :50051
HTTP server listening on :8080 (dashboard & metrics)

Start worker in a seperate terminal
go run ./cmd/worker

worker will start picking jobs and processing it. Currently, the worker_id should be manually editted in the main.go under worker
for the worker to start processing accordingly. It includes logic for success, failures and retries.

Test the API using postman.

1. select a new grpc request
2. import the proto file
3. URL -> localhost:50051
4. select the required endpoint
5. Invoke

SubmitJob Request

{
  "worker_id": "worker-1a",
  "payload_json": "{\"task\":\"email\",\"details\":\"hello\"}",
  "idempotency_key": "",
  "max_retries": 2
}

Submitjob response

{
    "job_id": "7c15d90f-0b36-4c2a-8437-c812d8141dd4",
    "deduplicated": false
}

GetJob request

{
  "job_id": "7c15d90f-0b36-4c2a-8437-c812d8141dd4"
}

GetJob response

{
    "job": {
        "id": "7c15d90f-0b36-4c2a-8437-c812d8141dd4",
        "worker_id": "worker-1a",
        "status": "pending",
        "payload_json": "{\"task\": \"email\", \"details\": \"hello\"}",
        "attempt_count": 0,
        "max_retries": 2,
        "last_error": "",
        "created_at": "2025-11-28T02:50:30+05:30",
        "updated_at": "2025-11-28T02:50:30+05:30"
    }
}

ListJobs request

{
  "worker_id": "worker-1a"
}

ListJobs response

{
    "jobs": [
        {
            "id": "7c15d90f-0b36-4c2a-8437-c812d8141dd4",
            "worker_id": "worker-1a",
            "status": "pending",
            "payload_json": "{\"task\": \"email\", \"details\": \"hello\"}",
            "attempt_count": 0,
            "max_retries": 2,
            "last_error": "",
            "created_at": "2025-11-28T02:50:30+05:30",
            "updated_at": "2025-11-28T02:50:30+05:30"
        }
    ]
}

ListFailedJobs request

{
  "worker_id": "worker-1a"
}

ListFailedJobs response

{
    "jobs": [
        {
            "id": "cd662847-dccf-46d1-8b10-5e3dab14d1ab",
            "worker_id": "worker-1a",
            "status": "failed",
            "payload_json": "{\"task\": \"email\", \"details\": \"hello\"}",
            "attempt_count": 2,
            "max_retries": 2,
            "last_error": "simulated failure",
            "created_at": "2025-11-28T02:53:57+05:30",
            "updated_at": "2025-11-28T02:55:26+05:30"
        },
        {
            "id": "86aa0810-5f9b-4704-a8c2-2d42c94505b9",
            "worker_id": "worker-1a",
            "status": "failed",
            "payload_json": "{\"task\": \"email\", \"details\": \"hello\"}",
            "attempt_count": 2,
            "max_retries": 2,
            "last_error": "simulated failure",
            "created_at": "2025-11-28T02:53:55+05:30",
            "updated_at": "2025-11-28T02:55:20+05:30"
        },
        {
            "id": "7c15d90f-0b36-4c2a-8437-c812d8141dd4",
            "worker_id": "worker-1a",
            "status": "failed",
            "payload_json": "{\"task\": \"email\", \"details\": \"hello\"}",
            "attempt_count": 2,
            "max_retries": 2,
            "last_error": "simulated failure",
            "created_at": "2025-11-28T02:50:30+05:30",
            "updated_at": "2025-11-28T02:55:14+05:30"
        }
    ]
}

Start server, then open http://localhost:8080 

It shows:

Total jobs
Pending
Running
Done
Failed
DLQ items (failed jobs)

It refreshes automatically every 3 seconds.

![alt text](<Screenshot (16).png>)

Job Persistence & Restart Behavior

If the server or worker restarts:
All jobs remain in Postgres
Running jobs will be picked again after lease expiry
Failed jobs stay in DLQ
Nothing is lost