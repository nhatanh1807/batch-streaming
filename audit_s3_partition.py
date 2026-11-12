
import os
import json
import hashlib
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

import boto3


# =========================================================
# CONFIG
# =========================================================

BUCKET = os.getenv("S3_BUCKET", "batch-streaming-bronze")
PREFIX = "raw/binance/trades/year=2026/month=10/day=21/"

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

DB_PATH = "/tmp/audit_20261021.sqlite"

# Binance raw fields may use either trade_id or t
TRADE_ID_FIELDS = ("trade_id", "t")


# =========================================================
# HELPERS
# =========================================================

def extract_trade_id(record):
    for field in TRADE_ID_FIELDS:
        if field in record and record[field] is not None:
            return str(record[field])
    return None


def canonical_hash(record):
    raw = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# =========================================================
# S3 CLIENT
# =========================================================

session = boto3.Session(
    profile_name=AWS_PROFILE or None,
    region_name=AWS_REGION,
)

s3 = session.client("s3")


# =========================================================
# SQLITE: KEEP AUDIT MEMORY OUT OF RAM
# =========================================================

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)

conn.execute("""
    CREATE TABLE trades (
        trade_id TEXT PRIMARY KEY,
        occurrences INTEGER NOT NULL,
        first_hash TEXT NOT NULL,
        first_key TEXT NOT NULL,
        first_line INTEGER NOT NULL,
        different_payload INTEGER NOT NULL DEFAULT 0
    )
""")

conn.execute("""
    CREATE TABLE duplicate_examples (
        trade_id TEXT,
        object_key TEXT,
        line_number INTEGER,
        payload_hash TEXT
    )
""")


# =========================================================
# AUDIT
# =========================================================

stats = defaultdict(int)
file_stats = []
duplicate_files = set()

paginator = s3.get_paginator("list_objects_v2")

print(f"Auditing s3://{BUCKET}/{PREFIX}")

for page in paginator.paginate(
    Bucket=BUCKET,
    Prefix=PREFIX,
):
    for obj in page.get("Contents", []):

        key = obj["Key"]

        if not key.lower().endswith(".jsonl"):
            continue

        stats["files"] += 1
        file_lines = 0
        file_duplicates = 0
        file_errors = 0

        print(f"Reading: {key}")

        response = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )

        for line_number, raw_line in enumerate(
            response["Body"].iter_lines(),
            start=1,
        ):
            if not raw_line:
                continue

            file_lines += 1
            stats["lines"] += 1

            try:
                record = json.loads(raw_line)

                if not isinstance(record, dict):
                    raise ValueError(
                        "JSON record is not an object"
                    )

            except Exception as exc:
                file_errors += 1
                stats["invalid_json"] += 1

                if stats["invalid_json"] <= 20:
                    print(
                        f"[INVALID JSON] {key}:{line_number} "
                        f"error={exc}"
                    )
                continue

            trade_id = extract_trade_id(record)

            if trade_id is None:
                stats["missing_trade_id"] += 1

                if stats["missing_trade_id"] <= 20:
                    print(
                        f"[MISSING trade_id] "
                        f"{key}:{line_number}"
                    )
                continue

            payload_hash = canonical_hash(record)

            existing = conn.execute(
                """
                SELECT occurrences, first_hash,
                       first_key, first_line
                FROM trades
                WHERE trade_id = ?
                """,
                (trade_id,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO trades (
                        trade_id,
                        occurrences,
                        first_hash,
                        first_key,
                        first_line
                    )
                    VALUES (?, 1, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        payload_hash,
                        key,
                        line_number,
                    ),
                )

            else:
                occurrences, first_hash, first_key, first_line = existing

                file_duplicates += 1
                stats["duplicate_occurrences"] += 1
                duplicate_files.add(key)

                different_payload = (
                    payload_hash != first_hash
                )

                if different_payload:
                    stats["duplicate_different_payload"] += 1

                conn.execute(
                    """
                    UPDATE trades
                    SET occurrences = occurrences + 1,
                        different_payload =
                            MAX(different_payload, ?)
                    WHERE trade_id = ?
                    """,
                    (
                        int(different_payload),
                        trade_id,
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO duplicate_examples
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        key,
                        line_number,
                        payload_hash,
                    ),
                )

                if stats["duplicate_occurrences"] <= 30:
                    print(
                        f"[DUPLICATE] trade_id={trade_id} "
                        f"first={first_key}:{first_line} "
                        f"again={key}:{line_number} "
                        f"same_payload={not different_payload}"
                    )

        conn.commit()

        file_stats.append({
            "key": key,
            "lines": file_lines,
            "invalid_json": file_errors,
            "duplicate_occurrences": file_duplicates,
        })


# =========================================================
# SUMMARY
# =========================================================

duplicate_groups = conn.execute("""
    SELECT COUNT(*)
    FROM trades
    WHERE occurrences > 1
""").fetchone()[0]

different_groups = conn.execute("""
    SELECT COUNT(*)
    FROM trades
    WHERE occurrences > 1
      AND different_payload = 1
""").fetchone()[0]

print("\n" + "=" * 65)
print("AUDIT SUMMARY - 2026-10-21")
print("=" * 65)

print(f"Files scanned:                 {stats['files']}")
print(f"Total JSONL lines:             {stats['lines']}")
print(f"Invalid JSON lines:            {stats['invalid_json']}")
print(f"Missing trade_id:              {stats['missing_trade_id']}")
print(f"Duplicate trade_id groups:     {duplicate_groups}")
print(f"Duplicate occurrences:         {stats['duplicate_occurrences']}")
print(f"Groups with different payload: {different_groups}")
print(f"Files containing duplicates:   {len(duplicate_files)}")

print("\nTOP 30 DUPLICATE TRADE IDS")

for row in conn.execute("""
    SELECT
        trade_id,
        occurrences,
        different_payload,
        first_key,
        first_line
    FROM trades
    WHERE occurrences > 1
    ORDER BY occurrences DESC, trade_id
    LIMIT 30
"""):
    print(row)

print("\nFILES WITH DUPLICATES")

for key in sorted(duplicate_files):
    print(key)

conn.close()

print(f"\nSQLite audit database: {DB_PATH}")
print("S3 source files were not modified.")
