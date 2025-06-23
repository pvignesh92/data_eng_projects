# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, upper, trim, 
    regexp_replace, to_date, year, month, dayofmonth,
    coalesce, isnan, isnull, length, substring, lower, concat_ws, datediff,
    sum, count, avg, max, min, countDistinct, round
)
from pyspark.sql.types import StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

# Widgets for parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("bronze_schema", "", "Bronze Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
bronze_schema = dbutils.widgets.get("bronze_schema")

# COMMAND ----------

def clean_customer_data(df):
    """
    Clean and validate customer data from bronze layer.
    """
    print("Cleaning customer data...")
    
    cleaned_df = df.select(
        col("customer_id").cast(StringType()).alias("customer_id"),
        trim(upper(col("customer_name"))).alias("customer_name"),
        trim(lower(col("email"))).alias("email"),
        to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
        trim(upper(col("country"))).alias("country"),
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("job_id")
    )
    
    # Add data quality score
    cleaned_df = cleaned_df.withColumn(
        "data_quality_score",
        when(col("customer_id").isNotNull(), 1).otherwise(0) +
        when(length(col("email")) > 0, 1).otherwise(0) +
        when(length(col("customer_name")) > 0, 1).otherwise(0) +
        when(col("country").isNotNull(), 1).otherwise(0)
    )
    
    # Add derived columns
    cleaned_df = cleaned_df.withColumn(
        "customer_tenure_days",
        datediff(current_timestamp(), col("signup_date"))
    ).withColumn(
        "customer_tenure_years",
        year(current_timestamp()) - year(col("signup_date"))
    ).withColumn(
        "customer_segment",
        when(col("customer_tenure_years") < 1, "New")
        .when(col("customer_tenure_years") < 3, "Established")
        .when(col("customer_tenure_years") < 5, "Loyal")
        .otherwise("VIP")
    ).withColumn(
        "region",
        when(col("country").isin(["US", "USA", "UNITED STATES"]), "North America")
        .when(col("country").isin(["UK", "UNITED KINGDOM", "GB"]), "Europe")
        .when(col("country").isin(["CA", "CANADA"]), "North America")
        .when(col("country").isin(["AU", "AUSTRALIA"]), "Oceania")
        .otherwise("Other")
    )
    
    return cleaned_df

# COMMAND ----------

def clean_order_data(df):
    """
    Clean and validate order data from bronze layer.
    """
    print("Cleaning order data...")
    
    cleaned_df = df.select(
        col("order_id").cast(StringType()).alias("order_id"),
        col("customer_id").cast(StringType()).alias("customer_id"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        trim(upper(col("order_status"))).alias("order_status"),
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("job_id")
    )
    
    return cleaned_df

# COMMAND ----------

def clean_order_items_data(df):
    """
    Clean and validate order items data from bronze layer.
    """
    print("Cleaning order items data...")
    
    cleaned_df = df.select(
        col("order_item_id").cast(StringType()).alias("order_item_id"),
        col("order_id").cast(StringType()).alias("order_id"),
        col("product_id").cast(StringType()).alias("product_id"),
        col("quantity").cast(IntegerType()).alias("quantity"),
        col("unit_price").cast(DoubleType()).alias("unit_price"),
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("job_id")
    )
    
    # Add calculated fields
    cleaned_df = cleaned_df.withColumn(
        "line_total",
        col("quantity") * col("unit_price")
    )
    
    return cleaned_df

# COMMAND ----------

def create_customer_order_summary(customers_df, orders_df, order_items_df):
    """
    Create customer order summary with aggregated metrics.
    """
    print("Creating customer order summary...")
    
    # Rename metadata columns to avoid conflicts during joins
    customers_renamed = customers_df.select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("signup_date"),
        col("country"),
        col("customer_tenure_days"),
        col("customer_tenure_years"),
        col("customer_segment"),
        col("region"),
        col("data_quality_score"),
        col("_ingestion_timestamp").alias("customer_ingestion_timestamp"),
        col("_source_file").alias("customer_source_file"),
        col("job_id").alias("customer_job_id")
    )
    
    orders_renamed = orders_df.select(
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("order_status"),
        col("_ingestion_timestamp").alias("order_ingestion_timestamp"),
        col("_source_file").alias("order_source_file"),
        col("job_id").alias("order_job_id")
    )
    
    order_items_renamed = order_items_df.select(
        col("order_item_id"),
        col("order_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("line_total"),
        col("_ingestion_timestamp").alias("order_item_ingestion_timestamp"),
        col("_source_file").alias("order_item_source_file"),
        col("job_id").alias("order_item_job_id")
    )
    
    # Join orders with order items to get order totals
    order_totals = order_items_renamed.groupBy("order_id").agg(
        sum("line_total").alias("order_total"),
        sum("quantity").alias("total_items"),
        count("order_item_id").alias("unique_products")
    )
    
    # Join orders with order totals
    orders_with_totals = orders_renamed.join(order_totals, "order_id", "left")
    
    # Aggregate by customer
    customer_summary = orders_with_totals.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        sum("order_total").alias("total_revenue"),
        avg("order_total").alias("avg_order_value"),
        max("order_total").alias("max_order_value"),
        min("order_total").alias("min_order_value"),
        sum("total_items").alias("total_items_purchased"),
        avg("total_items").alias("avg_items_per_order"),
        countDistinct("order_id").alias("unique_orders"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date")
    )
    
    # Join with customer data
    customer_order_summary = customers_renamed.join(customer_summary, "customer_id", "left")
    
    # Add derived metrics
    final_summary = customer_order_summary.withColumn(
        "avg_order_frequency_days",
        when(col("total_orders") > 1, 
             datediff(col("last_order_date"), col("first_order_date")) / (col("total_orders") - 1))
        .otherwise(None)
    ).withColumn(
        "customer_lifetime_value",
        col("total_revenue")
    ).withColumn(
        "avg_items_per_order",
        round(col("avg_items_per_order"), 2)
    ).withColumn(
        "avg_order_value",
        round(col("avg_order_value"), 2)
    ).withColumn(
        "total_revenue",
        round(col("total_revenue"), 2)
    ).withColumn(
        "customer_value_tier",
        when(col("total_revenue") >= 1000, "High Value")
        .when(col("total_revenue") >= 500, "Medium Value")
        .when(col("total_revenue") >= 100, "Low Value")
        .otherwise("No Orders")
    )
    
    return final_summary

# COMMAND ----------

def validate_customer_summary(df):
    """
    Validate customer order summary data quality.
    """
    print("Validating customer order summary...")
    
    validation_results = {
        "total_customers": df.count(),
        "customers_with_orders": df.filter(col("total_orders") > 0).count(),
        "customers_without_orders": df.filter(col("total_orders").isNull() | (col("total_orders") == 0)).count(),
        "negative_revenue": df.filter(col("total_revenue") < 0).count(),
        "null_customer_ids": df.filter(col("customer_id").isNull()).count(),
        "duplicate_customer_ids": df.groupBy("customer_id").count().filter(col("count") > 1).count()
    }
    
    print("Customer Order Summary Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create silver customer_order_summary table with customer and order metrics.
    """
    spark = SparkSession.builder.appName("Silver Customer Order Summary").getOrCreate()
    
    # Define table names
    bronze_customers_table = f"{catalog_name}.{bronze_schema}.customers"
    bronze_orders_table = f"{catalog_name}.{bronze_schema}.orders"
    bronze_order_items_table = f"{catalog_name}.{bronze_schema}.order_items"
    silver_customer_summary_table = f"{catalog_name}.{schema_name}.customer_order_summary"
    
    print(f"Starting silver layer transformation for customer order summary")
    print(f"Source tables: {bronze_customers_table}, {bronze_orders_table}, {bronze_order_items_table}")
    print(f"Target: {silver_customer_summary_table}")
    
    try:
        # Read from bronze layer
        bronze_customers = spark.table(bronze_customers_table)
        bronze_orders = spark.table(bronze_orders_table)
        bronze_order_items = spark.table(bronze_order_items_table)
        
        print(f"Read {bronze_customers.count()} customers, {bronze_orders.count()} orders, {bronze_order_items.count()} order items")
        
        # Clean data
        cleaned_customers = clean_customer_data(bronze_customers)
        cleaned_orders = clean_order_data(bronze_orders)
        cleaned_order_items = clean_order_items_data(bronze_order_items)
        
        # Create customer order summary
        customer_summary = create_customer_order_summary(cleaned_customers, cleaned_orders, cleaned_order_items)
        
        # Validate data
        validation_results = validate_customer_summary(customer_summary)
        
        # Add processing metadata
        final_df = customer_summary.withColumn("_processed_timestamp", current_timestamp()) \
                                  .withColumn("_processing_layer", lit("silver"))
        
        # Write to silver layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(silver_customer_summary_table)
        
        print(f"✅ Successfully created silver customer order summary table: {silver_customer_summary_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from silver customer order summary:")
        final_df.select("customer_id", "customer_name", "customer_segment", "total_orders", 
                       "total_revenue", "avg_order_value", "customer_value_tier").show(5)
        
        # Show customer value tier distribution
        print("\nCustomer value tier distribution:")
        final_df.groupBy("customer_value_tier").count().orderBy(col("count").desc()).show()
        
    except Exception as e:
        print(f"❌ Error processing customer order summary: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 