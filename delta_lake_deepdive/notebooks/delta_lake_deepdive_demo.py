import os
import shutil
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

# --- 1. Spark Session with Delta Support ---
spark = SparkSession.builder \
    .appName("DeltaLakeDeepDiveDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. Setup Paths ---
BASE_PATH = "./data/delta_demo_table"
if os.path.exists(BASE_PATH):
    shutil.rmtree(BASE_PATH)
os.makedirs(BASE_PATH, exist_ok=True)

# --- 3. Generate Mocked Data ---
def random_name():
    return ''.join(random.choices(string.ascii_uppercase, k=5))

def generate_data(start_id, count):
    return [(i, random_name(), random.randint(18, 70)) for i in range(start_id, start_id + count)]

data = generate_data(1, 100)
df = spark.createDataFrame(data, ["id", "name", "age"])

# --- 4. Write Initial Data to Delta Table ---
df.write.format("delta").mode("overwrite").save(BASE_PATH)
print("Initial data written to Delta table.")

# --- 5. Iterative Insert, Update, Delete Operations ---
delta_table = DeltaTable.forPath(spark, BASE_PATH)

# Insert new records
new_data = generate_data(101, 20)
df_new = spark.createDataFrame(new_data, ["id", "name", "age"])
df_new.write.format("delta").mode("append").save(BASE_PATH)
print("Inserted new records.")

# Update some records
for i in range(105, 110):
    delta_table.update(
        condition=col("id") == i,
        set={"age": lit(99)}
    )
print("Updated records with id 105-109.")

# Delete some records
delta_table.delete(col("id") < 10)
print("Deleted records with id < 10.")

# --- 6. Show Delta Log Files ---
log_path = os.path.join(BASE_PATH, "_delta_log")
print("\nDelta Log Files:")
for f in sorted(os.listdir(log_path)):
    print(f)

# --- 7. Show Table History and Statistics ---
delta_table.history().show(truncate=False)

# Show statistics from the latest log (if available)
print("\nSample statistics from Delta log:")
latest_json = sorted([f for f in os.listdir(log_path) if f.endswith('.json')])[-1]
with open(os.path.join(log_path, latest_json)) as f:
    for i, line in enumerate(f):
        if 'stats' in line:
            print(line.strip())
        if i > 20:
            break

# --- 8. Deletion Vectors (if supported) ---
# Note: Deletion vectors are available in Delta Lake 2.0+ and require specific configs.
# This section is a placeholder for advanced exploration.

spark.stop() 