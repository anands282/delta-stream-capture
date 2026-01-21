import boto3
from botocore.exceptions import ClientError

def ensure_bucket_exists(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, region: str = "us-east-1"):
    """Ensure the S3 bucket exists. Returns True if bucket exists or was created."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    try:
        # If the bucket exists this will succeed
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        # Attempt to create the bucket (useful for local/dev S3 like MinIO)
        try:
            create_kwargs = {"Bucket": bucket_name}
            # Some S3 implementations require LocationConstraint
            if region and region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**create_kwargs)
            return True
        except ClientError as e2:
            print("failed to create bucket:", e2)
            return False
PY