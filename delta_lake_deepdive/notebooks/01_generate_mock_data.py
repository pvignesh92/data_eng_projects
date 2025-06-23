# Databricks notebook source

# COMMAND ----------

import random
import string
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------
# Widgets for parameters (Databricks or fallback for local)
if 'dbutils' in globals():
dbutils.widgets.text("catalog_name", "demo_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "demo_schema", "Schema Name")
dbutils.widgets.text("table_name", "mock_data", "Table Name")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")


# COMMAND ----------

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return random_string(5) + '@example.com'

def random_phone():
    return '+1' + ''.join(random.choices(string.digits, k=10))

# COMMAND ----------

def generate_mock_data(num_rows=10000):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("signup_date", DateType(), True),
        StructField("last_login", TimestampType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("level", IntegerType(), True),
        StructField("referral_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("credit_limit", FloatType(), True),
        StructField("debt", FloatType(), True),
        StructField("is_verified", BooleanType(), True),
        StructField("notes", StringType(), True)
    ])
    start_date = datetime(2020, 1, 1)
    rows = []
    for i in range(1, num_rows + 1):
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
    return rows, schema

# COMMAND ----------

def main():
    rows, schema = generate_mock_data(10000)
    df = spark.createDataFrame(rows, schema)
    print(f"Generated DataFrame with {df.count()} rows and {len(df.columns)} columns.")
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
    print(f"Data written to {full_table_name} as Delta table.")

# COMMAND ----------

if __name__ == "__main__":
    main()
