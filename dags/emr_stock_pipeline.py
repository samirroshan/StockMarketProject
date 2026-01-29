from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator

JOB_FLOW_OVERRIDES = {
    "Name": "stock-market-emr-cluster",
    "ReleaseLabel": "emr-7.1.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "m5.xlarge

            "InstanceCount": 2,
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [
    {
        "Name": "Stock Data Analytics",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "s3://your-stock-bucket/scripts/stock_analytics.py",
            ],
        },
    }
]

def upload_spark_script(**context):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename='stock_analytics.py',
        key='scripts/stock_analytics.py',
        bucket_name='your-stock-bucket',
        replace=True
    )

default_args = {
    "owner": "samirroshan",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 29),
    "retries": 1,
}

dag = DAG(
    "emr_stockmarket_pipeline",
    default_args=default_args,
    description="EMR Spark jobs on stock data orchestrated by Airflow",
    schedule_interval="@daily",
    catchup=False,
    tags=["emr", "stock", "spark"],
)

upload_script = PythonOperator(
    task_id="upload_spark_script",
    python_callable=upload_spark_script,
    dag=dag,
)

create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

wait_cluster

        = EmrJobFlowSensor(
    task_id="wait_cluster_ready",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    target_states=["WAITING"],
    aws_conn_id="aws_default",
    dag=dag,
)

add_steps = EmrAddStepsOperator(
    task_id="add_spark_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    steps=SPARK_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

wait_step = EmrStepSensor(
    task_id="wait_step_complete",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[0] }}",
    target_states=["SUCCEEDED"],
    aws_conn_id="aws_default",
    dag=dag,
)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

upload_script >> create_cluster >> wait_cluster >> add_steps >> wait_step >> terminate_cluster
