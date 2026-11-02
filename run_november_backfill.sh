#!/bin/bash

set -euo pipefail

DAG_ID="batch_streaming_backfill"
BUCKET="batch-streaming-bronze"
PREFIX="raw/binance/trades"
PROFILE="batch-streaming"

echo "Discovering November 2026 partitions..."

mapfile -t DAYS < <(
    aws s3 ls "s3://${BUCKET}/${PREFIX}/" \
        --recursive --profile "$PROFILE" |
    awk '{print $4}' |
    grep -E 'year=2026/month=11/day=[0-9]+/.*\.jsonl$' |
    sed -E 's#.*day=([0-9]+)/.*#\1#' |
    sort -nu
)

if [ ${#DAYS[@]} -eq 0 ]; then
    echo "No November partitions found. Stopping."
    exit 1
fi

echo "Found partitions:"
printf '%s\n' "${DAYS[@]}"

for day_num in "${DAYS[@]}"; do

    printf -v day "%02d" "$day_num"
    DATE="2026-11-${day}"
    RUN_ID="manual__backfill_${DATE}"

    echo ""
    echo "======================================"
    echo "Processing ${DATE}"
    echo "======================================"

    STATE=$(docker compose exec -T postgres \
        psql -U postgres -d airflow -tAc \
        "SELECT state FROM dag_run
         WHERE dag_id='${DAG_ID}'
         AND run_id='${RUN_ID}';" | xargs)

    if [ "$STATE" = "success" ]; then
        echo "${DATE}: Already completed. Skipping."
        continue
    elif [ "$STATE" = "failed" ]; then
        echo "${DATE}: Previous run failed. Stopping."
        exit 1
    elif [ -n "$STATE" ]; then
        echo "${DATE}: Existing run state=${STATE}. Monitoring."
    else
        docker compose exec -T airflow-scheduler \
            airflow dags trigger \
            -r "$RUN_ID" \
            -c "{\"start_date\":\"${DATE}\",\"end_date\":\"${DATE}\",\"ignore_checkpoint\":false}" \
            "$DAG_ID"
    fi

    ELAPSED=0

    while true; do

        STATE=$(docker compose exec -T postgres \
            psql -U postgres -d airflow -tAc \
            "SELECT state FROM dag_run
             WHERE dag_id='${DAG_ID}'
             AND run_id='${RUN_ID}';" | xargs)

        echo "${DATE}: ${STATE:-waiting}"

        if [ "$STATE" = "success" ]; then
            echo "${DATE}: COMPLETED"
            break
        fi

        if [ "$STATE" = "failed" ]; then
            echo "${DATE}: FAILED. Stopping."
            exit 1
        fi

        if [ "$ELAPSED" -ge 21600 ]; then
            echo "Timeout. Stopping without enabling scheduled ETL."
            exit 1
        fi

        sleep 15
        ELAPSED=$((ELAPSED + 15))
    done

done

echo ""
echo "All discovered November partitions completed."
echo "Enabling regular ETL schedule..."

docker compose exec -T airflow-scheduler \
    airflow dags unpause batch_streaming_etl

echo "Regular ETL has been unpaused."
echo "Schedule: every 5 minutes."
