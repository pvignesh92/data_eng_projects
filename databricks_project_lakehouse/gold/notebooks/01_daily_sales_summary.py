# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, date_format,
    sum, count, countDistinct, avg, round, year, month, dayofmonth,
    expr, date_add, date_sub, datediff
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

def create_daily_sales_summary(order_line_fact_df):
    """
    Create daily sales summary with revenue, order count, customers, and AOV metrics.
    """
    print("Creating daily sales summary...")
    
    # Aggregate by order date
    daily_summary = order_line_fact_df.groupBy("order_date").agg(
        # Revenue metrics
        sum("line_total").alias("daily_revenue"),
        sum("line_total_with_tax").alias("daily_revenue_with_tax"),
        sum("profit_amount").alias("daily_profit"),
        
        # Order metrics
        countDistinct("order_id").alias("daily_order_count"),
        count("order_item_id").alias("daily_line_items"),
        
        # Customer metrics
        countDistinct("customer_id").alias("daily_unique_customers"),
        
        # Product metrics
        countDistinct("product_id").alias("daily_unique_products"),
        sum("quantity").alias("daily_units_sold"),
        
        # Average metrics
        avg("line_total").alias("avg_line_value"),
        avg("quantity").alias("avg_quantity_per_line")
    )
    
    # Add derived metrics
    final_summary = daily_summary.withColumn(
        "avg_order_value",
        when(col("daily_order_count") > 0, col("daily_revenue") / col("daily_order_count")).otherwise(0)
    ).withColumn(
        "avg_items_per_order",
        when(col("daily_order_count") > 0, col("daily_line_items") / col("daily_order_count")).otherwise(0)
    ).withColumn(
        "profit_margin_percentage",
        when(col("daily_revenue") > 0, (col("daily_profit") / col("daily_revenue")) * 100).otherwise(0)
    ).withColumn(
        "revenue_per_customer",
        when(col("daily_unique_customers") > 0, col("daily_revenue") / col("daily_unique_customers")).otherwise(0)
    ).withColumn(
        "units_per_order",
        when(col("daily_order_count") > 0, col("daily_units_sold") / col("daily_order_count")).otherwise(0)
    )
    
    # Add date dimension columns
    final_summary = final_summary.withColumn(
        "year", year(col("order_date"))
    ).withColumn(
        "month", month(col("order_date"))
    ).withColumn(
        "day", dayofmonth(col("order_date"))
    ).withColumn(
        "day_of_week", date_format(col("order_date"), "EEEE")
    ).withColumn(
        "quarter",
        when(month(col("order_date")) <= 3, 1)
        .when(month(col("order_date")) <= 6, 2)
        .when(month(col("order_date")) <= 9, 3)
        .otherwise(4)
    ).withColumn(
        "season",
        when(month(col("order_date")).isin([12, 1, 2]), "Winter")
        .when(month(col("order_date")).isin([3, 4, 5]), "Spring")
        .when(month(col("order_date")).isin([6, 7, 8]), "Summer")
        .otherwise("Fall")
    ).withColumn(
        "is_weekend",
        when(date_format(col("order_date"), "EEEE").isin(["Saturday", "Sunday"]), True).otherwise(False)
    )
    
    # Round numeric columns
    final_summary = final_summary.withColumn(
        "daily_revenue", round(col("daily_revenue"), 2)
    ).withColumn(
        "daily_revenue_with_tax", round(col("daily_revenue_with_tax"), 2)
    ).withColumn(
        "daily_profit", round(col("daily_profit"), 2)
    ).withColumn(
        "avg_order_value", round(col("avg_order_value"), 2)
    ).withColumn(
        "avg_line_value", round(col("avg_line_value"), 2)
    ).withColumn(
        "profit_margin_percentage", round(col("profit_margin_percentage"), 2)
    ).withColumn(
        "revenue_per_customer", round(col("revenue_per_customer"), 2)
    ).withColumn(
        "avg_items_per_order", round(col("avg_items_per_order"), 2)
    ).withColumn(
        "avg_quantity_per_line", round(col("avg_quantity_per_line"), 2)
    ).withColumn(
        "units_per_order", round(col("units_per_order"), 2)
    )
    
    return final_summary

# COMMAND ----------

def validate_daily_sales_summary(df):
    """
    Validate daily sales summary data quality.
    """
    print("Validating daily sales summary...")
    
    validation_results = {
        "total_days": df.count(),
        "days_with_revenue": df.filter(col("daily_revenue") > 0).count(),
        "days_without_revenue": df.filter(col("daily_revenue") == 0).count(),
        "negative_revenue": df.filter(col("daily_revenue") < 0).count(),
        "negative_profit": df.filter(col("daily_profit") < 0).count(),
        "zero_orders": df.filter(col("daily_order_count") == 0).count(),
        "future_dates": df.filter(col("order_date") > current_timestamp()).count(),
        "duplicate_dates": df.groupBy("order_date").count().filter(col("count") > 1).count()
    }
    
    print("Daily Sales Summary Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create gold daily_sales_summary table with daily aggregated metrics.
    """
    spark = SparkSession.builder.appName("Gold Daily Sales Summary").getOrCreate()
    
    # Define table names
    silver_order_line_fact_table = f"{catalog_name}.{silver_schema}.order_line_fact"
    gold_daily_sales_summary_table = f"{catalog_name}.{schema_name}.daily_sales_summary"
    
    print(f"Starting gold layer transformation for daily sales summary")
    print(f"Source table: {silver_order_line_fact_table}")
    print(f"Target: {gold_daily_sales_summary_table}")
    
    try:
        # Read from silver layer
        silver_order_line_fact = spark.table(silver_order_line_fact_table)
        print(f"Read {silver_order_line_fact.count()} order line records from silver layer")
        
        # Create daily sales summary
        daily_summary = create_daily_sales_summary(silver_order_line_fact)
        
        # Validate data
        validation_results = validate_daily_sales_summary(daily_summary)
        
        # Add processing metadata
        final_df = daily_summary.withColumn("_processed_timestamp", current_timestamp()) \
                               .withColumn("_processing_layer", lit("gold"))
        
        # Write to gold layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(gold_daily_sales_summary_table)
        
        print(f"✅ Successfully created gold daily sales summary table: {gold_daily_sales_summary_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from gold daily sales summary:")
        final_df.select("order_date", "daily_revenue", "daily_order_count", 
                       "daily_unique_customers", "avg_order_value", "profit_margin_percentage").show(5)
        
        # Show top revenue days
        print("\nTop 5 days by revenue:")
        final_df.orderBy(col("daily_revenue").desc()).select("order_date", "daily_revenue", 
                                                            "daily_order_count", "avg_order_value").show(5)
        
        # Show revenue by season
        print("\nRevenue by season:")
        final_df.groupBy("season").agg(
            sum("daily_revenue").alias("total_revenue"),
            avg("daily_revenue").alias("avg_daily_revenue"),
            count("order_date").alias("days_count")
        ).orderBy(col("total_revenue").desc()).show()
        
        # Show weekend vs weekday performance
        print("\nWeekend vs Weekday Performance:")
        final_df.groupBy("is_weekend").agg(
            sum("daily_revenue").alias("total_revenue"),
            avg("daily_revenue").alias("avg_daily_revenue"),
            avg("daily_order_count").alias("avg_daily_orders"),
            count("order_date").alias("days_count")
        ).show()
        
    except Exception as e:
        print(f"❌ Error processing daily sales summary: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 