"""
s3_fetch.py

Download a JSONL file from S3 and convert it into a JSON file.
"""

import json
from pathlib import Path

import boto3


def download_from_s3(*, bucket: str, key: str, dest_path: str) -> Path:
    """Download an object from S3 to a local file path."""
    s3 = boto3.client("s3")  # uses notebook IAM role automatically
    destination = Path(dest_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(destination))
    return destination


def jsonl_to_json(*, jsonl_path: str, json_path: str) -> Path:
    """Convert JSONL (one JSON object per line) into one JSON list file."""
    src = Path(jsonl_path)
    out = Path(json_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    records = []
    with src.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    with out.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return out
