from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


with DAG(
    dag_id="batch_streaming_etl",
    description="Process new Binance trade files from S3 into PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["binance", "spark", "s3", "postgres"],
) as dag:

    run_etl = DockerOperator(
        task_id="run_spark_etl",
        image="batch-streaming:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="batch-streaming_default",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/alva_ngo/.aws",
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
            "AWS_PROFILE": "batch-streaming",
            "AWS_REGION": "ap-southeast-1",
            "S3_BUCKET": "batch-streaming-bronze",
            "S3_RAW_PREFIX": "raw/binance/trades",
        },
        command="python main_etl.py",
    )