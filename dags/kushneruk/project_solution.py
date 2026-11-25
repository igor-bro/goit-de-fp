from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Етап 4. DAG послідовно запускає landing, bronze і silver на кластері Spark.
with DAG(
    dag_id="kushneruk_fp_datalake",
    start_date=datetime(2024, 8, 4),
    schedule=None,
    catchup=False,
    tags=["kushneruk"],
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        application="dags/kushneruk/landing_to_bronze.py",
        task_id="landing_to_bronze",
        conn_id="spark-default",
        verbose=True,
    )
    bronze_to_silver = SparkSubmitOperator(
        application="dags/kushneruk/bronze_to_silver.py",
        task_id="bronze_to_silver",
        conn_id="spark-default",
        verbose=True,
    )
    silver_to_gold = SparkSubmitOperator(
        application="dags/kushneruk/silver_to_gold.py",
        task_id="silver_to_gold",
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
