# Databricks notebook source

# COMMAND ----------

import random
import string
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

# COMMAND ----------

# Widgets for parameters (Databricks or fallback for local)
if 'dbutils' in globals():
    dbutils.widgets.text("catalog_name", "demo_catalog", "Catalog Name")
    dbutils.widgets.text("schema_name", "demo_schema", "Schema Name")
    dbutils.widgets.text("table_name", "delta_demo_table_cpy", "Table Name")
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    table_name = dbutils.widgets.get("table_name")
else:
    catalog_name = "demo_catalog"
    schema_name = "demo_schema"
    table_name = "delta_demo_table_cpy"

# COMMAND ----------

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return random_string(5) + '@example.com'

def random_phone():
    return '+1' + ''.join(random.choices(string.digits, k=10))

# COMMAND ----------

def generate_rows(start_id, count):
    start_date = datetime(2020, 1, 1)
    rows = []
    for i in range(start_id, start_id + count):
        row = (
            i,
            random_string(7),
            random.randint(18, 70),
            round(random.uniform(30000, 150000), 2),
            random.choice([True, False]),
            start_date + timedelta(days=random.randint(0, 1500)),
            start_date + timedelta(days=random.randint(0, 1500), seconds=random.randint(0, 86400)),
            random.choice(['USA', 'UK', 'IN', 'DE', 'FR', 'CA']),
            random.choice(['New York', 'London', 'Mumbai', 'Berlin', 'Paris', 'Toronto']),
            ''.join(random.choices(string.digits, k=5)),
            round(random.uniform(0, 100), 3),
            random.randint(1, 10),
            random_string(10),
            random_email(),
            random_phone(),
            random.choice(['standard', 'premium', 'gold']),
            round(random.uniform(1000, 20000), 2),
            round(random.uniform(0, 5000), 2),
            random.choice([True, False]),
            random_string(20)
        )
        rows.append(row)
    return rows

# COMMAND ----------

def main():
    spark = SparkSession.builder.appName("DeltaLakeHourlyOps").getOrCreate()
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    delta_table = DeltaTable.forName(spark, full_table_name)

    # 1. Insert 100 new rows
    max_id = spark.table(full_table_name).agg({"id": "max"}).collect()[0][0] or 0
    new_rows = generate_rows(max_id + 1, 100)
    schema = spark.table(full_table_name).schema
    df_new = spark.createDataFrame(new_rows, schema)
    df_new.write.format("delta").mode("append").saveAsTable(full_table_name)
    print(f"Inserted 100 new rows (id {max_id+1} to {max_id+100})")

    # 2. Update 100 random rows (set age=99) using MERGE
    ids = [row.id for row in spark.table(full_table_name).orderBy(col("id")).limit(1000).collect()]
    if len(ids) >= 100:
        update_ids = random.sample(ids, 100)
        update_df = spark.createDataFrame([(i, 99) for i in update_ids], ["id", "age"])
        delta_table.alias("target").merge(
            update_df.alias("updates"),
            "target.id = updates.id"
        ).whenMatchedUpdate(set={"age": col("updates.age")}).execute()
        print(f"Updated 100 random rows (set age=99) using MERGE")
    else:
        print("Not enough rows to update 100 random rows.")

    # 3. Delete 100 random rows using MERGE
    ids = [row.id for row in spark.table(full_table_name).orderBy(col("id")).limit(1000).collect()]
    if len(ids) >= 100:
        delete_ids = random.sample(ids, 100)
        delete_df = spark.createDataFrame([(i,) for i in delete_ids], ["id"])
        delta_table.alias("target").merge(
            delete_df.alias("deletes"),
            "target.id = deletes.id"
        ).whenMatchedDelete().execute()
        print(f"Deleted 100 random rows using MERGE.")
    else:
        print("Not enough rows to delete 100 random rows.")

# COMMAND ----------

if __name__ == "__main__":
    main() 