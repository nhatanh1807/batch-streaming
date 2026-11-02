#!/bin/bash

set -u

DAG_ID="batch_streaming_backfill"
START_DAY=12
END_DAY=30
SLEEP_SECONDS=15
TIMEOUT_SECONDS=21600

for day in $(seq -w $START_DAY $END_DAY); do

    DATE="2026-10-${day}"
    RUN_ID="manual__backfill_${DATE}"

    echo ""
    echo "=========================================="
    echo "Starting backfill: ${DATE}"
    echo "Run ID: ${RUN_ID}"
    echo "=========================================="

    # Check whether this run already exists
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
        echo "${DATE}: Existing run state = ${STATE}. Monitoring it."
    else
        echo "Triggering ${DATE}..."

        docker compose exec -T airflow-scheduler \
            airflow dags trigger \
            -r "$RUN_ID" \
            -c "{\"start_date\":\"${DATE}\",\"end_date\":\"${DATE}\",\"ignore_checkpoint\":false}" \
            "$DAG_ID" || exit 1
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
            echo "${DATE}: FAILED. Stopping backfill."
            exit 1
        fi

        if [ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]; then
            echo "${DATE}: Timeout. Stopping without triggering another run."
            exit 1
        fi

        sleep "$SLEEP_SECONDS"
        ELAPSED=$((ELAPSED + SLEEP_SECONDS))

    done

done

echo ""
echo "=========================================="
echo "ALL BACKFILL PARTITIONS COMPLETED"
echo "2026-10-12 through 2026-10-30"
echo "=========================================="
