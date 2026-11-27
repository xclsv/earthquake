"""Minimal helpers for S3-compatible object storage (RustFS).

Connection settings come from environment variables: S3_ENDPOINT,
S3_ACCESS_KEY, S3_SECRET_KEY and S3_BUCKET. The bucket is created
automatically on the first write.
"""

"""Минимальные помощники для объектного хранилища, совместимого с S3 (RustFS).

Настройки подключения берутся из переменных окружения: S3_ENDPOINT, 
S3_ACCESS_KEY, S3_SECRET_KEY и S3_BUCKET. Корзина создается 
автоматически при первой записи.
"""

import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "earthquake-raw")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(client) -> None:
    """Create the bucket if it does not exist yet."""
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError:
        client.create_bucket(Bucket=BUCKET)


def put_object(key: str, data: bytes) -> str:
    client = get_client()
    ensure_bucket(client)
    client.put_object(Bucket=BUCKET, Key=key, Body=data)
    return key


def get_object(key: str) -> bytes:
    client = get_client()
    return client.get_object(Bucket=BUCKET, Key=key)["Body"].read()