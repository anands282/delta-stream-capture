from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import uuid4
import time
import sqlalchemy
from sqlalchemy import text

app = FastAPI(title="delta-stream-capture (polling)")

# In-memory stores (swap for DB in real project)
JOBS: Dict[str, Dict[str, Any]] = {}

class SourceConfig(BaseModel):
    vendor: str = Field(..., description="postgres or mysql")
    host: str
    port: int
    database: str
    user: str
    password: str

class DestinationConfig(BaseModel):
    provider: str = Field("s3", const=True)
    bucket: str
    base_path: Optional[str] = ""

class CaptureConfig(BaseModel):
    table: str
    schema: Optional[str] = None
    watermark_column: Optional[str] = None  # e.g., id or updated_at
    polling_interval_ms: Optional[int] = 5000
    format: Optional[str] = "json"

class JobCreate(BaseModel):
    name: str
    source: SourceConfig
    destination: DestinationConfig
    capture: CaptureConfig
    enabled: Optional[bool] = False

@app.post("/jobs")
def create_job(payload: JobCreate):
    job_id = str(uuid4())
    job = payload.dict()
    job.update({
        "id": job_id,
        "status": "created",
        "created_at": time.time(),
        "updated_at": time.time(),
        "state": {"last_watermark": None}
    })
    JOBS[job_id] = job
    return job

@app.get("/jobs")
def list_jobs():
    return list(JOBS.values())

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/jobs/{job_id}/start")
def start_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job["enabled"] = True
    job["status"] = "running"
    job["updated_at"] = time.time()
    return {"message": "Job started", "job": job}

@app.post("/jobs/{job_id}/stop")
def stop_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job["enabled"] = False
    job["status"] = "stopped"
    job["updated_at"] = time.time()
    return {"message": "Job stopped", "job": job}

# New endpoint: update persistent state for a job (used by workers)
@app.post("/jobs/{job_id}/_update_state")
def update_state(job_id: str, payload: Dict[str, Any]):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    # Expect payload to contain a 'state' dictionary
    incoming_state = payload.get("state")
    if incoming_state is None:
        raise HTTPException(status_code=400, detail="payload must include 'state'")
    job["state"] = incoming_state
    job["updated_at"] = time.time()
    return {"ok": True}