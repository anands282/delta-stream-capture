#!/usr/bin/env bash
set -euo pipefail

# Ensure S3 bucket exists before starting the service
python - <<PY
from s3_utils import ensure_bucket_exists
import os
endpoint = os.getenv('S3_ENDPOINT', 'http://minio:9000')
access = os.getenv('S3_ACCESS_KEY', 'minio')
secret = os.getenv('S3_SECRET_KEY', 'minio123')
bucket = os.getenv('S3_BUCKET', 'delta-cdc')
region = os.getenv('S3_REGION', 'us-east-1')
print('ensuring bucket', bucket)
ok = ensure_bucket_exists(endpoint, access, secret, bucket, region=region)
if not ok:
    print('warning: failed to ensure bucket exists; continuing')
PY

# If first arg starts with '-', assume uvicorn flags and pass to uvicorn
if [[ "${1:-}" == "-"* ]]; then
  set -- uvicorn "$@"
fi

exec "$@"
