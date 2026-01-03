import sys, os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/api_request')

def safe_main_callable():
    from api_request_scripts.insert_records import main
    return main()

default_args = {
    'description':'A DAG to orchestrate data',
    'start_date':datetime(2025, 12, 19),
    'catchup': False,
}

dag = DAG( 
        dag_id = "energy-api-dbt-orchestrator", 
        default_args=default_args, 
        schedule=timedelta(minutes=30) 
            )

with dag:
    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=safe_main_callable,
    )

    transform = DockerOperator(
        task_id="dbt_run", 
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source="/home/martin/repos/energy-data-project/dbt/my_project",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                source="/home/martin/repos/energy-data-project/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
            ),
        ],
         environment={
            "DBT_HOST": os.getenv("DBT_HOST"),
            "DBT_USER": os.getenv("DBT_USER"),
            "DBT_PASS": os.getenv("DBT_PASS"),
            "DBT_DATABASE": os.getenv("DBT_DATABASE"),
            "DBT_PORT": "5432",
            "PGSSLMODE": "require",
        },
        network_mode="energy-data-project_my-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove='success',
    )

