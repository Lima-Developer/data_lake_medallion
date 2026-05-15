"""
Data Lake Pipeline DAG
Orchestrates: S3 Bucket Creation -> Spark Bronze Ingestion -> dbt Silver/Gold -> dbt Tests
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import task

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="lake_medallion_pipeline",
    default_args=default_args,
    description="E2E Data Lake Pipeline - Bronze/Silver/Gold",
    schedule=None,
    start_date=datetime(2026, 5, 14),
    catchup=False,
    tags=["lake", "medallion", "dbt", "spark"]
) as dag:

    # Task 1: Ensure the S3 bucket exists
    @task(task_id="create_s3_bucket")
    def create_s3_bucket():
        import boto3
        s3 = boto3.client('s3', endpoint_url='http://localstack:4566', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
        s3.create_bucket(Bucket='ingestion')
    
    creation_s3_bucket = create_s3_bucket()

    # Task 2: Submit PySpark job to ingest CSVs into bronze (Parquet on S3)
    bronze_ingestion = SparkSubmitOperator(
        task_id="bronze_spark_ingestion",
        application="/opt/airflow/spark/bronze_ingestion.py",
        conn_id="spark_default",
        conf={
            "spark.driver.extraClassPath": "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.hadoop.fs.s3a.endpoint": "http://localstack:4566",
            "spark.hadoop.fs.s3a.access.key": "test",
            "spark.hadoop.fs.s3a.secret.key": "test",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
    )

    # Task 3: Run dbt to build Silver and Gold models
    dbt_run = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir ."
    )

    # Task 4: Run dbt tests for data quality validation
    dbt_test = BashOperator(
        task_id="dbt_test_quality",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --project-dir ."
    )

    # Task 5: Generate dbt documentation with lineage graph
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir . --project-dir ."
    )

    # Defining execution order: bucket -> ingest -> transform -> test -> docs
    creation_s3_bucket >> bronze_ingestion >> dbt_run >> dbt_test >> dbt_docs