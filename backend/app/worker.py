"""
Simple polling worker (minimal).
"""
import time
import requests
import os
import json
import boto3
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")
POLL_JOBS_INTERVAL = int(os.getenv("POLL_JOBS_INTERVAL", "5"))  # seconds between polling job list

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=os.getenv("S3_REGION", "us-east-1"),
)

def fetch_jobs():
    try:
        r = requests.get(f"{BACKEND_URL}/jobs", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("failed to fetch jobs:", e)
        return []

def connect_source(src):
    vendor = src["vendor"].lower()
    if vendor == "postgres":
        url = f"postgresql+psycopg2://{src['user']}:{src['password']}@{src['host']}:{src['port']}/{src['database']}"
    elif vendor == "mysql":
        url = f"mysql+pymysql://{src['user']}:{src['password']}@{src['host']}:{src['port']}/{src['database']}"
    else:
        raise RuntimeError("unsupported vendor")
    engine = sqlalchemy.create_engine(url, pool_pre_ping=True)
    return engine

def build_query(capture, last_watermark):
    table = capture["table"]
    wm_col = capture.get("watermark_column")
    if wm_col:
        if last_watermark is None:
            return f"SELECT * FROM {table} ORDER BY {wm_col} ASC LIMIT 100", None
        else:
            return f"SELECT * FROM {table} WHERE {wm_col} > :wm ORDER BY {wm_col} ASC LIMIT 100", {"wm": last_watermark}
    else:
        return f"SELECT * FROM {table} LIMIT 100", None

def push_to_s3(job, batch_rows):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{job['id']}/{job['capture']['table']}/{ts}.json"
    key = os.path.join(job["destination"].get("base_path", "").lstrip("/"), filename).lstrip("/")
    body = json.dumps(batch_rows, default=str).encode("utf-8")
    s3_bucket = job["destination"]["bucket"]
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=body)
    print(f"wrote {len(batch_rows)} rows to s3://{s3_bucket}/{key}")

def process_job(job):
    try:
        if not job.get("enabled"):
            return
        state = job.setdefault("state", {})
        last_wm = state.get("last_watermark")

        engine = connect_source(job["source"])
        q, params = build_query(job["capture"], last_wm)
        with engine.connect() as conn:
            if params:
                res = conn.execute(text(q), params)
            else:
                res = conn.execute(text(q))
            rows = [dict(r) for r in res.mappings().all()]
        engine.dispose()

        if not rows:
            return

        wm_col = job["capture"].get("watermark_column")
        if wm_col:
            try:
                new_wm = max(r.get(wm_col) for r in rows if r.get(wm_col) is not None)
                state["last_watermark"] = new_wm
            except Exception:
                state["last_watermark"] = datetime.utcnow().isoformat()

        push_to_s3(job, rows)

        try:
            requests.post(f"{BACKEND_URL}/jobs/{job['id']}/_update_state", json={"state": state}, timeout=5)
        except Exception:
            pass

    except Exception as e:
        print("error processing job", job.get("id"), e)

def main_loop():
    print("worker started, polling backend for jobs:", BACKEND_URL)
    while True:
        jobs = fetch_jobs()
        for job in jobs:
            if job.get("enabled"):
                process_job(job)
        time.sleep(POLL_JOBS_INTERVAL)

if __name__ == "__main__":
    main_loop()
