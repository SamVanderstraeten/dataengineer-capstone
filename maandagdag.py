from pendulum import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(
    dag_id="sampxl-capstone-dag",
    description="Run capstone",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    start_date=datetime(year=2023, month=3, day=27, tz="Europe/Brussels")
)

run_job = BatchOperator(
    task_id="sampxl-batch",
    dag=dag,
    job_name="sampxl-job2",
    job_definition="arn:aws:batch:eu-west-1:338791806049:job-definition/sampxl-batch:3",
    job_queue="arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-pxl-2023-job-queue",
    overrides={}
)