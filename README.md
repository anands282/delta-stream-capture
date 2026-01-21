# delta-stream-capture

A minimal scaffold for a polling-only Change Data Capture (CDC) control plane: FastAPI backend, a simple polling worker, and a Docker Compose dev stack with Postgres, MySQL and MinIO (S3 compatible).

Quickstart (local dev)
1. Install Docker & Docker Compose
2. From repository root run:
   docker-compose -f infra/docker-compose.yml up --build
3. FastAPI backend will be at http://localhost:8000 and MinIO console at http://localhost:9000 (access key: minio / secret: minio123)

Files created:
- infra/docker-compose.yml
- backend/Dockerfile
- backend/requirements.txt
- backend/.env.example
- backend/app/main.py
- backend/app/worker.py

This scaffold implements a basic Jobs API and a simple worker that polls the backend for running jobs and emits a sample JSON object to the configured S3 destination (MinIO) as a proof-of-concept. It is intentionally minimal and marked with TODOs for extension.
