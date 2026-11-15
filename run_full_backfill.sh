
#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# FULL BACKFILL / RECONCILIATION
# Period: 2026-10-01 to 2026-11-08
# Checkpoint-aware: ignore_checkpoint=false
# ============================================================

START_DATE="2026-11-08"
END_DATE="2026-11-12"

INCREMENTAL_DAG="batch_streaming_etl"
BACKFILL_DAG="batch_streaming_backfill"
POOL_NAME="etl_pipeline_pool"

BUCKET="batch-streaming-bronze"
PREFIX="raw/binance/trades/"

RUN_TAG="$(date -u +%Y%m%dT%H%M%S)"

echo "================================================"
echo "FULL BACKFILL"
echo "Period: ${START_DATE} -> ${END_DATE}"
echo "Run tag: ${RUN_TAG}"
echo "================================================"

# ------------------------------------------------------------
# 1. Check Docker Compose services
# ------------------------------------------------------------

docker compose ps airflow-scheduler postgres

# ------------------------------------------------------------
# 2. Pause incremental DAG only
#    IMPORTANT: Backfill DAG must remain UNPAUSED
# ------------------------------------------------------------

echo "[1/7] Pausing incremental DAG..."

docker compose exec -T airflow-scheduler \
    airflow dags pause "${INCREMENTAL_DAG}"

echo "[2/7] Unpausing backfill DAG..."

docker compose exec -T airflow-scheduler \
    airflow dags unpause "${BACKFILL_DAG}"

# ------------------------------------------------------------
# 3. Check backfill DAG is unpaused
# ------------------------------------------------------------

BACKFILL_PAUSED="$(
    docker compose exec -T airflow-scheduler \
        airflow dags list --output json |
    python3 -c '
import json, sys
items = json.load(sys.stdin)
for item in items:
    if item.get("dag_id") == "batch_streaming_backfill":
        print(str(item.get("is_paused", "")).lower())
        break
'
)"

if [[ "${BACKFILL_PAUSED}" == "true" ]]; then
    echo "ERROR: Backfill DAG is still paused."
    exit 1
fi

# ------------------------------------------------------------
# 4. Check active ETL tasks
#    Do not create duplicate runs while another ETL is active.
# ------------------------------------------------------------

echo "[3/7] Checking active ETL task instances..."

ACTIVE_TASKS="$(
docker compose exec -T postgres psql -U postgres -d airflow -At -c "
SELECT dag_id || ' | ' || run_id || ' | ' || task_id || ' | ' || state
FROM task_instance
WHERE dag_id IN ('${INCREMENTAL_DAG}', '${BACKFILL_DAG}')
  AND state IN (
      'scheduled',
      'queued',
      'running',
      'up_for_retry',
      'up_for_reschedule',
      'deferred'
  )
ORDER BY dag_id, run_id;
"
)"

if [[ -n "${ACTIVE_TASKS}" ]]; then
    echo
    echo "An ETL task is still active or scheduled:"
    echo "${ACTIVE_TASKS}"
    echo
    echo "STOP. Resolve the existing task before starting another backfill."
    echo "Do not rerun this script yet."
    exit 1
fi

# ------------------------------------------------------------
# 5. Check ETL pool
# ------------------------------------------------------------

echo "[4/7] Checking Airflow pool..."

docker compose exec -T airflow-scheduler airflow pools list

# ------------------------------------------------------------
# 6. Discover S3 dates containing JSONL files
# ------------------------------------------------------------

echo "[5/7] Discovering S3 partitions..."

S3_DATES="$(
docker compose exec -T airflow-scheduler \
python - <<'PY'
import boto3
import re
from datetime import date

BUCKET = "batch-streaming-bronze"
PREFIX = "raw/binance/trades/"
START = date(2026, 11, 8)
END = date(2026, 11, 12)

session = boto3.Session(profile_name="batch-streaming")
s3 = session.client("s3")

paginator = s3.get_paginator("list_objects_v2")

dates = set()

pattern = re.compile(
    r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/"
)

for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]

        if not key.endswith(".jsonl"):
            continue

        match = pattern.search(key)
        if not match:
            continue

        year, month, day = map(int, match.groups())

        try:
            partition_date = date(year, month, day)
        except ValueError:
            continue

        if START <= partition_date <= END:
            dates.add(partition_date.isoformat())

for item in sorted(dates):
    print(item)
PY
)"

if [[ -z "${S3_DATES}" ]]; then
    echo "ERROR: No matching JSONL partitions found in S3."
    exit 1
fi

echo "Partitions discovered:"
echo "${S3_DATES}"

# ------------------------------------------------------------
# 7. Trigger one backfill run per date, sequentially
# ------------------------------------------------------------

echo "[6/7] Starting sequential backfill..."


while IFS= read -r DATE; do
    [[ -z "${DATE}" ]] && continue

    RUN_ID="manual__reconcile_${DATE}_${RUN_TAG}"

    echo
    echo "------------------------------------------------"
    echo "Processing date: ${DATE}"
    echo "Run ID: ${RUN_ID}"
    echo "------------------------------------------------"

    ACTIVE_COUNT="$(
        docker compose exec -T postgres \
            psql -U postgres -d airflow -At -c "
            SELECT COUNT(*)
            FROM task_instance
            WHERE dag_id IN ('${INCREMENTAL_DAG}', '${BACKFILL_DAG}')
              AND state IN (
                  'scheduled',
                  'queued',
                  'running',
                  'up_for_retry',
                  'up_for_reschedule',
                  'deferred'
              );
            " </dev/null
    )"

    ACTIVE_COUNT="$(echo "${ACTIVE_COUNT}" | tr -d '[:space:]')"

    if [[ "${ACTIVE_COUNT}" != "0" ]]; then
        echo "STOP: Another ETL task is active."
        echo "Active task count: ${ACTIVE_COUNT}"
        exit 1
    fi

    # Trigger one date only.
    docker compose exec -T airflow-scheduler \
        airflow dags trigger "${BACKFILL_DAG}" \
        --run-id "${RUN_ID}" \
        --conf "{\"start_date\":\"${DATE}\",\"end_date\":\"${DATE}\",\"ignore_checkpoint\":false}" \
        </dev/null

    echo "Triggered ${RUN_ID}"

    # Wait for this DAG run to finish.
    while true; do
        STATE="$(
            docker compose exec -T postgres \
                psql -U postgres -d airflow -At -c "
                SELECT state
                FROM dag_run
                WHERE dag_id = '${BACKFILL_DAG}'
                  AND run_id = '${RUN_ID}'
                LIMIT 1;
                " </dev/null
        )"

        STATE="$(echo "${STATE}" | tr -d '[:space:]')"

        echo "[$DATE] DAG state: ${STATE:-not-created}"

        case "${STATE}" in
            success)
                echo "SUCCESS: ${DATE}"
                break
                ;;
            failed)
                echo "FAILED: ${DATE}"
                echo "Stopping backfill. Check Airflow task logs."
                exit 1
                ;;
            queued|running|scheduled)
                sleep 15
                ;;
            *)
                echo "Unexpected DAG state: ${STATE}"
                sleep 15
                ;;
        esac
    done

done < <(printf '%s\n' "${S3_DATES}")

# ------------------------------------------------------------
# 8. Finish
# ------------------------------------------------------------

echo
echo "[7/7] Backfill completed."

echo "Incremental DAG remains paused."
echo "Review Silver data and checkpoint counts before resuming it."

echo "================================================"
echo "FULL BACKFILL FINISHED"
echo "================================================"
