# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, date_format,
    sum, count, countDistinct, avg, round, year, month, dayofmonth,
    expr, date_add, date_sub, datediff, max, min, lag, lead,
    row_number, rank, dense_rank, concat_ws
)
from pyspark.sql.types import StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

# Widgets for parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("silver_schema", "", "Silver Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

def create_customer_lifetime_value(customer_summary_df, order_line_fact_df):
    """
    Create customer lifetime value with rolling 12-month revenue, order count, and recency metrics.
    """
    print("Creating customer lifetime value analysis...")
    
    # Calculate rolling 12-month metrics from order line fact
    # Convert date to days since epoch for window calculations
    order_line_with_days = order_line_fact_df.withColumn(
        "order_date_days", 
        datediff(col("order_date"), lit("1970-01-01"))
    )
    
    # Define window for rolling 12 months (365 days)
    rolling_window = Window.partitionBy("customer_id").orderBy("order_date_days").rangeBetween(
        -365, 0  # 365 days back from current order date
    )
    
    # Add rolling metrics to order line fact (without countDistinct in window)
    order_line_with_rolling = order_line_with_days.withColumn(
        "rolling_12m_revenue", 
        sum("line_total").over(rolling_window)
    ).withColumn(
        "rolling_12m_items", 
        count("order_item_id").over(rolling_window)
    ).withColumn(
        "rolling_12m_units", 
        sum("quantity").over(rolling_window)
    )
    
    # For distinct counts, we'll aggregate after the window calculation
    # Get the latest rolling metrics for each customer
    customer_rolling_metrics = order_line_with_rolling.groupBy("customer_id").agg(
        max("rolling_12m_revenue").alias("rolling_12m_revenue"),
        max("rolling_12m_items").alias("rolling_12m_items"),
        max("rolling_12m_units").alias("rolling_12m_units"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date"),
        countDistinct("order_date").alias("unique_order_days"),
        countDistinct("product_id").alias("unique_products_purchased")
    )
    
    # Calculate rolling 12-month order count separately using a different approach
    # Get all orders within 12 months for each customer
    order_line_with_12m_flag = order_line_fact_df.withColumn(
        "order_date_days", 
        datediff(col("order_date"), lit("1970-01-01"))
    ).withColumn(
        "max_order_date_days",
        max("order_date_days").over(Window.partitionBy("customer_id"))
    ).withColumn(
        "is_within_12m",
        when(col("order_date_days") >= col("max_order_date_days") - 365, True).otherwise(False)
    )
    
    # Calculate rolling 12-month order count
    rolling_order_count = order_line_with_12m_flag.filter(col("is_within_12m") == True).groupBy("customer_id").agg(
        countDistinct("order_id").alias("rolling_12m_orders")
    )
    
    # Join the rolling order count with other metrics
    customer_rolling_metrics = customer_rolling_metrics.join(rolling_order_count, "customer_id", "left")
    
    # Calculate recency metrics
    customer_recency = customer_rolling_metrics.withColumn(
        "days_since_last_order",
        datediff(current_timestamp(), col("last_order_date"))
    ).withColumn(
        "customer_tenure_days",
        datediff(col("last_order_date"), col("first_order_date"))
    ).withColumn(
        "avg_days_between_orders",
        when(col("rolling_12m_orders") > 1, 
             col("customer_tenure_days") / (col("rolling_12m_orders") - 1)).otherwise(None)
    )
    
    # Before the join, rename or drop ambiguous columns in customer_summary_df to avoid ambiguity
    customer_summary_df = (
        customer_summary_df
        .withColumnRenamed("last_order_date", "summary_last_order_date")
        .withColumnRenamed("customer_tenure_days", "summary_customer_tenure_days")
        .withColumnRenamed("first_order_date", "summary_first_order_date")
    )
    # Then join as before
    customer_lifetime_value = customer_recency.join(customer_summary_df, "customer_id", "left")
    
    # Add derived metrics
    final_clv = customer_lifetime_value.withColumn(
        "rolling_12m_aov",
        when(col("rolling_12m_orders") > 0, col("rolling_12m_revenue") / col("rolling_12m_orders")).otherwise(0)
    ).withColumn(
        "rolling_12m_items_per_order",
        when(col("rolling_12m_orders") > 0, col("rolling_12m_items") / col("rolling_12m_orders")).otherwise(0)
    ).withColumn(
        "rolling_12m_units_per_order",
        when(col("rolling_12m_orders") > 0, col("rolling_12m_units") / col("rolling_12m_orders")).otherwise(0)
    ).withColumn(
        "rolling_12m_revenue_per_day",
        when(col("unique_order_days") > 0, col("rolling_12m_revenue") / col("unique_order_days")).otherwise(0)
    ).withColumn(
        "recency_score",
        when(col("days_since_last_order") <= 30, "Very Recent")
        .when(col("days_since_last_order") <= 90, "Recent")
        .when(col("days_since_last_order") <= 180, "Moderate")
        .when(col("days_since_last_order") <= 365, "Lapsed")
        .otherwise("Churned")
    ).withColumn(
        "frequency_score",
        when(col("rolling_12m_orders") >= 10, "Very Frequent")
        .when(col("rolling_12m_orders") >= 5, "Frequent")
        .when(col("rolling_12m_orders") >= 2, "Occasional")
        .otherwise("One-time")
    ).withColumn(
        "monetary_score",
        when(col("rolling_12m_revenue") >= 1000, "High Value")
        .when(col("rolling_12m_revenue") >= 500, "Medium Value")
        .when(col("rolling_12m_revenue") >= 100, "Low Value")
        .otherwise("No Value")
    ).withColumn(
        "rfm_segment",
        concat_ws("_", col("recency_score"), col("frequency_score"), col("monetary_score"))
    ).withColumn(
        "customer_lifetime_value",
        col("rolling_12m_revenue") * 2  # Simple CLV calculation (2x 12-month revenue)
    ).withColumn(
        "customer_health_score",
        when(col("days_since_last_order") <= 30, 100)
        .when(col("days_since_last_order") <= 90, 75)
        .when(col("days_since_last_order") <= 180, 50)
        .when(col("days_since_last_order") <= 365, 25)
        .otherwise(0)
    )
    
    # Round numeric columns
    final_clv = final_clv.withColumn(
        "rolling_12m_revenue", round(col("rolling_12m_revenue"), 2)
    ).withColumn(
        "rolling_12m_aov", round(col("rolling_12m_aov"), 2)
    ).withColumn(
        "rolling_12m_items_per_order", round(col("rolling_12m_items_per_order"), 2)
    ).withColumn(
        "rolling_12m_units_per_order", round(col("rolling_12m_units_per_order"), 2)
    ).withColumn(
        "rolling_12m_revenue_per_day", round(col("rolling_12m_revenue_per_day"), 2)
    ).withColumn(
        "customer_lifetime_value", round(col("customer_lifetime_value"), 2)
    ).withColumn(
        "avg_days_between_orders", round(col("avg_days_between_orders"), 1)
    )
    
    return final_clv

# COMMAND ----------

def validate_customer_lifetime_value(df):
    """
    Validate customer lifetime value data quality.
    """
    print("Validating customer lifetime value...")
    
    validation_results = {
        "total_customers": df.count(),
        "customers_with_12m_revenue": df.filter(col("rolling_12m_revenue") > 0).count(),
        "customers_without_12m_revenue": df.filter(col("rolling_12m_revenue") == 0).count(),
        "negative_12m_revenue": df.filter(col("rolling_12m_revenue") < 0).count(),
        "future_last_order_dates": df.filter(col("last_order_date") > current_timestamp()).count(),
        "invalid_tenure": df.filter(col("customer_tenure_days") < 0).count(),
        "null_customer_ids": df.filter(col("customer_id").isNull()).count(),
        "duplicate_customer_ids": df.groupBy("customer_id").count().filter(col("count") > 1).count()
    }
    
    print("Customer Lifetime Value Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create gold customer_lifetime_value table with rolling 12-month metrics and recency analysis.
    """
    spark = SparkSession.builder.appName("Gold Customer Lifetime Value").getOrCreate()
    
    # Define table names
    silver_customer_summary_table = f"{catalog_name}.{silver_schema}.customer_order_summary"
    silver_order_line_fact_table = f"{catalog_name}.{silver_schema}.order_line_fact"
    gold_customer_lifetime_value_table = f"{catalog_name}.{schema_name}.customer_lifetime_value"
    
    print(f"Starting gold layer transformation for customer lifetime value")
    print(f"Source tables: {silver_customer_summary_table}, {silver_order_line_fact_table}")
    print(f"Target: {gold_customer_lifetime_value_table}")
    
    try:
        # Read from silver layer
        silver_customer_summary = spark.table(silver_customer_summary_table)
        silver_order_line_fact = spark.table(silver_order_line_fact_table)
        
        print(f"Read {silver_customer_summary.count()} customer summaries and {silver_order_line_fact.count()} order line records")
        
        # Create customer lifetime value
        customer_lifetime_value = create_customer_lifetime_value(silver_customer_summary, silver_order_line_fact)
        
        # Validate data
        validation_results = validate_customer_lifetime_value(customer_lifetime_value)
        
        # Add processing metadata
        final_df = customer_lifetime_value.withColumn("_processed_timestamp", current_timestamp()) \
                                         .withColumn("_processing_layer", lit("gold"))
        
        # Write to gold layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(gold_customer_lifetime_value_table)
        
        print(f"✅ Successfully created gold customer lifetime value table: {gold_customer_lifetime_value_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from gold customer lifetime value:")
        final_df.select("customer_id", "customer_name", "rolling_12m_revenue", "rolling_12m_orders", 
                       "days_since_last_order", "rfm_segment", "customer_lifetime_value").show(5)
        
        # Show RFM segment distribution
        print("\nRFM Segment Distribution:")
        final_df.groupBy("rfm_segment").count().orderBy(col("count").desc()).show()
        
        # Show top customers by CLV
        print("\nTop 5 customers by Customer Lifetime Value:")
        final_df.orderBy(col("customer_lifetime_value").desc()).select("customer_name", 
                                                                      "customer_lifetime_value", 
                                                                      "rolling_12m_revenue", 
                                                                      "rolling_12m_orders").show(5)
        
        # Show recency distribution
        print("\nRecency Score Distribution:")
        final_df.groupBy("recency_score").agg(
            count("customer_id").alias("customer_count"),
            avg("rolling_12m_revenue").alias("avg_12m_revenue")
        ).orderBy(col("customer_count").desc()).show()
        
        # Show customer health by segment
        print("\nCustomer Health by Segment:")
        final_df.groupBy("customer_segment").agg(
            count("customer_id").alias("customer_count"),
            avg("customer_health_score").alias("avg_health_score"),
            avg("rolling_12m_revenue").alias("avg_12m_revenue")
        ).orderBy(col("avg_health_score").desc()).show()
        
    except Exception as e:
        print(f"❌ Error processing customer lifetime value: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 