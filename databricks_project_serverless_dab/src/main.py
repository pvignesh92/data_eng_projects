import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read CSV from DBFS
df = spark.read.csv('dbfs:/databricks-datasets/airlines/part-00000', header=True, inferSchema=True)

# Simple transformation: select a few columns
df_selected = df.select('Year', 'Month', 'DayofMonth', 'DepTime', 'ArrTime').limit(10)

# Write result to a temp location in DBFS
df_selected.write.mode('overwrite').parquet('dbfs:/tmp/airlines_selected')