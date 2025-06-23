-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- # Delta Table Exploration: SQL Deep Dive
-- This notebook explores the Delta table created in previous steps using Spark SQL. It covers row counts, sample data, Delta log/history, table properties, deletion vectors, and file-level statistics.

-- COMMAND ----------
-- MAGIC %python
# Widgets for parameters (Databricks or fallback for local)
if 'dbutils' in globals():
    dbutils.widgets.text("catalog_name", "demo_catalog", "Catalog Name")
    dbutils.widgets.text("schema_name", "demo_schema", "Schema Name")
    dbutils.widgets.text("table_name", "mock_data", "Table Name")
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    table_name = dbutils.widgets.get("table_name")
else:
    catalog_name = "demo_catalog"
    schema_name = "demo_schema"
    table_name = "mock_data"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

-- COMMAND ----------
-- MAGIC %md
-- ## 1. Row Count

-- COMMAND ----------
SELECT COUNT(*) AS row_count FROM ${full_table_name};

-- COMMAND ----------
-- MAGIC %md
-- ## 2. Sample Data

-- COMMAND ----------
SELECT * FROM ${full_table_name} LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- ## 3. Table History (Delta Lake Transaction Log)

-- COMMAND ----------
DESCRIBE HISTORY ${full_table_name};

-- COMMAND ----------
-- MAGIC %md
-- ## 4. Table Properties

-- COMMAND ----------
SHOW TBLPROPERTIES ${full_table_name};

-- COMMAND ----------
-- MAGIC %md
-- ## 5. Delta Log Files (JSON/Checkpoint)
-- To view Delta log files, you can use the file browser or `%fs ls` in Databricks. Example:
-- MAGIC %python
log_path = f"/Volumes/{catalog_name}/{schema_name}/{table_name}/_delta_log"
display(dbutils.fs.ls(log_path))

-- COMMAND ----------
-- MAGIC %md
-- ## 6. Deletion Vectors (if supported)
-- Delta Lake 2.0+ supports deletion vectors. To check for them, look for 'deletionVector' in the Delta log JSON files.
-- MAGIC %python
import json
files = [f.path for f in dbutils.fs.ls(log_path) if f.name.endswith('.json')]
for file in files[-5:]:  # Check last 5 log files
    data = dbutils.fs.head(file, 4096)
    if 'deletionVector' in data:
        print(f"Deletion vector found in: {file}")
        print(data)

-- COMMAND ----------
-- MAGIC %md
-- ## 7. File-level Statistics
-- You can use the DESCRIBE DETAIL command to see file-level stats and metadata.

-- COMMAND ----------
DESCRIBE DETAIL ${full_table_name}; 