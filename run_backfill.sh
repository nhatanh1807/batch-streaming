
#!/bin/bash

set -u

DAG_ID="batch_streaming_backfill"
S3_BUCKET="batch-streaming-bronze"
S3_PREFIX="raw/binance/trades/"
START_DATE="2026-10-01"
END_DATE="2026-11-08"

SLEEP_SECONDS=15
TIMEOUT_SECONDS=21600

echo "=========================================="
echo "DISCOVERING S3 PARTITIONS"
echo "Range: ${START_DATE} to ${END_DATE}"
echo "=========================================="

# Discover dates directly from S3 using Scheduler's AWS credentials
DATES=$(docker compose exec -T airflow-scheduler python - <<PY
import boto3
import re
from datetime import date

session = boto3.Session(
    profile_name="batch-streaming",
    region_name="ap-southeast-1"
)
s3 = session.client("s3")

start = date.fromisoformat("${START_DATE}")
end = date.fromisoformat("${END_DATE}")

pattern = re.compile(
    r"year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2})/"
)

dates = set()
paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(
    Bucket="${S3_BUCKET}",
    Prefix="${S3_PREFIX}"
):
    for obj in page.get("Contents", []):
        key = obj["Key"]

        if not key.endswith(".jsonl"):
            continue

        match = pattern.search(key)
        if not match:
            continue

        try:
            d = date(*map(int, match.groups()))
        except ValueError:
            continue

        if start <= d <= end:
            dates.add(d.isoformat())

for d in sorted(dates):
    print(d)
PY
) || {
    echo "ERROR: Cannot discover S3 partitions."
    exit 1
}

if [ -z "$DATES" ]; then
    echo "No S3 partitions found in the requested range."
    exit 1
fi

echo ""
echo "Discovered partitions:"
echo "$DATES"

TOTAL=$(echo "$DATES" | wc -l)
COUNT=0

while IFS= read -r DATE; do

    [ -z "$DATE" ] && continue

    COUNT=$((COUNT + 1))
    RUN_ID="manual__backfill_${DATE}"

    echo ""
    echo "=========================================="
    echo "[$COUNT/$TOTAL] Backfill: ${DATE}"
    echo "=========================================="

    STATE=$(docker compose exec -T postgres \
        psql -U postgres -d airflow -tAc \
        "SELECT state FROM dag_run
         WHERE dag_id='${DAG_ID}'
         AND run_id='${RUN_ID}';" | xargs)

    if [ "$STATE" = "success" ]; then
        echo "${DATE}: Already completed. Skipping."
        continue

    elif [ "$STATE" = "failed" ]; then
        echo "${DATE}: Original run failed. Checking retry..."

        RUN_ID="${RUN_ID}_retry1"

        STATE=$(docker compose exec -T postgres \
            psql -U postgres -d airflow -tAc \
            "SELECT state FROM dag_run
             WHERE dag_id='${DAG_ID}'
             AND run_id='${RUN_ID}';" | xargs)

        if [ "$STATE" = "success" ]; then
            echo "${DATE}: Retry already completed. Skipping."
            continue
        fi
    fi

    if [ -z "$STATE" ]; then
        echo "Triggering ${DATE}: ${RUN_ID}"

        docker compose exec -T airflow-scheduler \
            airflow dags trigger \
            -r "$RUN_ID" \
            -c "{\"start_date\":\"${DATE}\",\"end_date\":\"${DATE}\",\"ignore_checkpoint\":false}" \
            "$DAG_ID" || exit 1
    else
        echo "${DATE}: Existing run state=${STATE}. Monitoring."
    fi

    ELAPSED=0

    while true; do

        STATE=$(docker compose exec -T postgres \
            psql -U postgres -d airflow -tAc \
            "SELECT state FROM dag_run
             WHERE dag_id='${DAG_ID}'
             AND run_id='${RUN_ID}';" | xargs)

        echo "${DATE}: state=${STATE:-waiting}"

        if [ "$STATE" = "success" ]; then
            echo "${DATE}: COMPLETED"
            break
        fi

        if [ "$STATE" = "failed" ]; then
            echo "${DATE}: FAILED. Stopping."
            exit 1
        fi

        if [ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]; then
            echo "${DATE}: TIMEOUT. Stopping."
            exit 1
        fi

        sleep "$SLEEP_SECONDS"
        ELAPSED=$((ELAPSED + SLEEP_SECONDS))
    done

done <<< "$DATES"

echo ""
echo "=========================================="
echo "ALL DISCOVERED PARTITIONS COMPLETED"
echo "Range: ${START_DATE} to ${END_DATE}"
echo "Partitions: ${TOTAL}"
echo "=========================================="
