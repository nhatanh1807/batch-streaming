
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


AWS_HOST_DIR = os.environ.get("AWS_HOST_DIR")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "batch-streaming")

if not AWS_HOST_DIR:
    raise ValueError("AWS_HOST_DIR is not configured in Airflow")


with DAG(
    dag_id="batch_streaming_backfill",
    description="Replay Binance S3 Bronze files for a selected date range",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 0,
    },
    tags=["binance", "backfill", "spark", "s3"],
) as dag:

    run_backfill = DockerOperator(
        task_id="run_backfill_etl",
        image="batch-streaming:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="batch-streaming_default",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=AWS_HOST_DIR,
                target="/root/.aws",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "batch_streaming",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "AWS_PROFILE": AWS_PROFILE,
            "AWS_REGION": "ap-southeast-1",
            "S3_BUCKET": "batch-streaming-bronze",
            "S3_RAW_PREFIX": "raw/binance/trades",
            "BACKFILL_START_DATE": (
                "{{ dag_run.conf.get('start_date', '') if dag_run else '' }}"
            ),
            "BACKFILL_END_DATE": (
                "{{ dag_run.conf.get('end_date', '') if dag_run else '' }}"
            ),
            "BACKFILL_IGNORE_CHECKPOINT": (
                "{{ dag_run.conf.get('ignore_checkpoint', true) | string | lower if dag_run else 'true' }}"
            ),
        },
        command="python main_etl.py",
    )
