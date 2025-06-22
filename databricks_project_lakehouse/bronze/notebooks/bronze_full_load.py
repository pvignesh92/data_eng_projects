# Databricks notebook source

from pyspark.sql import SparkSession

# COMMAND ----------

# Get parameters from the job
dbutils.widgets.text("source_path", "", "Source Path")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

source_path = dbutils.widgets.get("source_path")
table_name = dbutils.widgets.get("table_name")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

def main():
    """
    Uses Databricks Auto Loader to incrementally and efficiently process new data
    files as they arrive in the source S3 bucket. The stream is configured to
    run as a batch job, processing all available data and then stopping.
    """
    spark = SparkSession.builder.appName(f"Bronze Layer Ingestion").getOrCreate()

    checkpoint_path = f"/Volumes/{catalog_name}/{schema_name}/metadata_files/{table_name}"


    print(f"Starting Auto Loader stream for {table_name}")
    print(f"Source: {source_path}")
    print(f"Checkpoint: {checkpoint_path}")

    # Configure Auto Loader to read from the source path
    # 'cloudFiles' is the format name for Auto Loader
    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # or 'json', 'parquet', etc.
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", checkpoint_path)  # used to store schema
        .load(f"{source_path}")
    )    
    # Write the stream to the bronze Delta table
    # trigger(availableNow=True) makes the stream run like a batch job
    (
        raw_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog_name}.{schema_name}.{table_name}")
        .awaitTermination()
    )

    print(f"Bronze layer Auto Loader stream complete for {table_name}.")

# COMMAND ----------

if __name__ == "__main__":
    main() 