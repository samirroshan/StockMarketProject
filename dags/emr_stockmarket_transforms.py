from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

CLUSTER_ID = "j-A7074LFUB0J8"  # Your running EMR cluster

with DAG(
    dag_id="emr_stockmarket_transforms",
    start_date=datetime(2026, 1, 29),
    catchup=False,
    tags=["emr", "spark", "stock"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=print,
        op_args=["Starting Stock Pipeline"],
    )

    run_spark = EmrAddStepsOperator(
        task_id="run_stock_spark_job",
        job_flow_id=CLUSTER_ID,
        aws_conn_id="aws_default",
        region_name="us-east-1",
        steps=[
            {
                "Name": "Run Stock Analytics",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "s3://kafka-stock-market-market-cap-bucket/src/spark/quantflow_stock_silver.py",
                    ],
                },
            }
        ],
    )

    wait = EmrStepSensor(
        task_id="wait_for_spark_job",
        job_flow_id=CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='run_stock_spark_job')[0] }}",
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=print,
        op_args=["Stock Pipeline Finished"],
    )
    (
    start >> run_spark >> wait >> end
    )