# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------
# Widgets for parameters
dbutils.widgets.text("source_path",  "", "Source Path")
dbutils.widgets.text("table_name",   "", "Table Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name",  "", "Schema Name")
dbutils.widgets.text("primary_key", "id", "Primary Key Column")
dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("job_run_id", "", "Job Run ID")


source_path  = dbutils.widgets.get("source_path")
table_name   = dbutils.widgets.get("table_name")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name  = dbutils.widgets.get("schema_name")
primary_key  = dbutils.widgets.get("primary_key")
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")

full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
checkpoint_path = f"/Volumes/{catalog_name}/{schema_name}/metadata_files/{table_name}_incr"

# COMMAND ----------
def process_batch(batch_df, batch_id):
    """
    Upsert logic for each micro-batch.
    Uses only batch_df.sparkSession and len(batch_df.take(1)) to avoid unsupported APIs.
    """
    # skip empty batches
    if len(batch_df.take(1)) == 0:
        print(f"[Batch {batch_id}] empty, skipping")
        return

    spark = batch_df.sparkSession
    # enrich with metadata
    df_meta = (
        batch_df
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_source_file", lit("auto_loader"))
          .withColumn("job_id", lit(job_id))
    )

    # create the bronze table if needed
    if not spark.catalog.tableExists(full_table_name):
        print(f"[Batch {batch_id}] Creating bronze table {full_table_name}")
        df_meta.write \
               .format("delta") \
               .mode("overwrite") \
               .option("delta.autoOptimize.optimizeWrite", "true") \
               .option("delta.autoOptimize.autoCompact", "true") \
               .saveAsTable(full_table_name)
        return

    # otherwise perform MERGE (upsert)
    print(f"[Batch {batch_id}] Merging into {full_table_name}")
    delta_tbl = DeltaTable.forName(spark, full_table_name)

    merge_cond = f"target.{primary_key} = source.{primary_key}"
    data_cols = [c for c in df_meta.columns if not c.startswith("_")]

    (delta_tbl.alias("target")
        .merge(
            source    = df_meta.alias("source"),
            condition = merge_cond
        )
        .whenMatchedUpdate(
            set = {col: f"source.{col}" for col in data_cols}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[Batch {batch_id}] Merge complete")

# COMMAND ----------
def main():
    spark = SparkSession.builder.appName("Bronze Layer Incremental Load").getOrCreate()

    print(f"Starting incremental Auto Loader for {table_name}")
    print(f"  Source Path:    {source_path}")
    print(f"  Checkpoint Path:{checkpoint_path}")

    raw_df = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaLocation", checkpoint_path)
             .load(source_path)
    )

    # only use foreachBatch as the sink; all writes happen inside process_batch
    (
      raw_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
    )

    print(f"Auto Loader stream finished for {table_name}")

# COMMAND ----------

if __name__ == "__main__":
    main()
