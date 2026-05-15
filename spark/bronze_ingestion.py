"""
Bronze Ingestion Job - PySpark
Reads raw CSV files and writes Parquet to S3 (LocalStack) with ingestion metadata.
"""
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, input_file_name

def create_spark_session():
    """Create SparkSession configured for LocalStack S3."""
    
    jars_path = "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .master("spark://spark-master:7077")
        .config("spark.driver.extraClassPath", jars_path)
        .config("spark.executor.extraClassPath", jars_path)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def ingest_to_bronze(spark, source_path, table_name, partition_col=None):
    """Read CSV and write to Bronze layer as Parquet."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )

    df_with_metadata = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
    )

    output_path = f"s3a://ingestion/bronze/{table_name}"

    writer = df_with_metadata.write.mode("overwrite").format("parquet")

    if partition_col and partition_col in df_with_metadata.columns:
        writer = writer.partitionBy(partition_col)
    
    writer.save(output_path)

    print(f"[BRONZE] {table_name}: {df_with_metadata.count()} rows written to {output_path}")

def main():
    spark = create_spark_session()

    try:
        ingest_to_bronze(
            spark,
            source_path="/opt/airflow/data/raw/transactions.csv",
            table_name="transactions",
            partition_col="order_date"
        )

        ingest_to_bronze(
            spark,
            source_path="/opt/airflow/data/raw/customers.csv",
            table_name="customers",
        )

        print("[BRONZE] Ingestion completed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()