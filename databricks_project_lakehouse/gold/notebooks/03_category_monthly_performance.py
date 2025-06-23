# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, date_format,
    sum, count, countDistinct, avg, round, year, month, dayofmonth,
    expr, date_add, date_sub, datediff, concat, substring
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

def create_category_monthly_performance(product_performance_df, order_line_fact_df):
    """
    Create category monthly performance with units, revenue, and average rating metrics.
    """
    print("Creating category monthly performance analysis...")
    
    # Select and rename columns from product_performance_df to avoid ambiguity
    product_perf_sel = product_performance_df.select(
        "product_id",
        col("category").alias("agg_product_category"),
        col("product_name"),
        col("product_category_group"),
        col("price_category"),
        col("avg_rating"),
        col("avg_unit_price")
    )
    
    # Join product performance with order line fact to get category information
    order_line_with_category = order_line_fact_df.join(
        product_perf_sel,
        "product_id",
        "left"
    )
    
    # Add year-month column
    order_line_with_category = order_line_with_category.withColumn(
        "year_month",
        concat(year(col("order_date")), lit("-"), 
               when(month(col("order_date")) < 10, concat(lit("0"), month(col("order_date"))))
               .otherwise(month(col("order_date"))))
    ).withColumn(
        "year",
        year(col("order_date"))
    ).withColumn(
        "month",
        month(col("order_date"))
    )
    
    # Aggregate by category and year-month
    category_monthly = order_line_with_category.groupBy("agg_product_category", "year_month", "year", "month").agg(
        # Revenue metrics
        sum("line_total").alias("monthly_revenue"),
        sum("line_total_with_tax").alias("monthly_revenue_with_tax"),
        sum("profit_amount").alias("monthly_profit"),
        
        # Unit metrics
        sum("quantity").alias("monthly_units_sold"),
        count("order_item_id").alias("monthly_line_items"),
        
        # Order metrics
        countDistinct("order_id").alias("monthly_orders"),
        countDistinct("customer_id").alias("monthly_unique_customers"),
        
        # Product metrics
        countDistinct("product_id").alias("monthly_unique_products"),
        
        # Rating metrics (from product performance)
        avg("avg_rating").alias("avg_category_rating"),
        count("avg_rating").alias("rating_count"),
        
        # Price metrics
        avg("avg_unit_price").alias("avg_unit_price"),
        avg("line_total").alias("avg_line_value")
    )
    
    # Add derived metrics
    final_performance = category_monthly.withColumn(
        "monthly_aov",
        when(col("monthly_orders") > 0, col("monthly_revenue") / col("monthly_orders")).otherwise(0)
    ).withColumn(
        "monthly_items_per_order",
        when(col("monthly_orders") > 0, col("monthly_line_items") / col("monthly_orders")).otherwise(0)
    ).withColumn(
        "monthly_units_per_order",
        when(col("monthly_orders") > 0, col("monthly_units_sold") / col("monthly_orders")).otherwise(0)
    ).withColumn(
        "profit_margin_percentage",
        when(col("monthly_revenue") > 0, (col("monthly_profit") / col("monthly_revenue")) * 100).otherwise(0)
    ).withColumn(
        "revenue_per_customer",
        when(col("monthly_unique_customers") > 0, col("monthly_revenue") / col("monthly_unique_customers")).otherwise(0)
    ).withColumn(
        "units_per_customer",
        when(col("monthly_unique_customers") > 0, col("monthly_units_sold") / col("monthly_unique_customers")).otherwise(0)
    ).withColumn(
        "revenue_per_product",
        when(col("monthly_unique_products") > 0, col("monthly_revenue") / col("monthly_unique_products")).otherwise(0)
    ).withColumn(
        "units_per_product",
        when(col("monthly_unique_products") > 0, col("monthly_units_sold") / col("monthly_unique_products")).otherwise(0)
    )
    
    # Add month name and quarter
    final_performance = final_performance.withColumn(
        "month_name",
        when(col("month") == 1, "January")
        .when(col("month") == 2, "February")
        .when(col("month") == 3, "March")
        .when(col("month") == 4, "April")
        .when(col("month") == 5, "May")
        .when(col("month") == 6, "June")
        .when(col("month") == 7, "July")
        .when(col("month") == 8, "August")
        .when(col("month") == 9, "September")
        .when(col("month") == 10, "October")
        .when(col("month") == 11, "November")
        .otherwise("December")
    ).withColumn(
        "quarter",
        when(col("month") <= 3, 1)
        .when(col("month") <= 6, 2)
        .when(col("month") <= 9, 3)
        .otherwise(4)
    ).withColumn(
        "quarter_name",
        concat(lit("Q"), col("quarter"))
    ).withColumn(
        "year_quarter",
        concat(col("year"), lit("-"), col("quarter_name"))
    )
    
    # Add seasonality
    final_performance = final_performance.withColumn(
        "season",
        when(col("month").isin([12, 1, 2]), "Winter")
        .when(col("month").isin([3, 4, 5]), "Spring")
        .when(col("month").isin([6, 7, 8]), "Summer")
        .otherwise("Fall")
    )
    
    # Round numeric columns
    final_performance = final_performance.withColumn(
        "monthly_revenue", round(col("monthly_revenue"), 2)
    ).withColumn(
        "monthly_revenue_with_tax", round(col("monthly_revenue_with_tax"), 2)
    ).withColumn(
        "monthly_profit", round(col("monthly_profit"), 2)
    ).withColumn(
        "monthly_aov", round(col("monthly_aov"), 2)
    ).withColumn(
        "avg_category_rating", round(col("avg_category_rating"), 2)
    ).withColumn(
        "avg_unit_price", round(col("avg_unit_price"), 2)
    ).withColumn(
        "avg_line_value", round(col("avg_line_value"), 2)
    ).withColumn(
        "profit_margin_percentage", round(col("profit_margin_percentage"), 2)
    ).withColumn(
        "revenue_per_customer", round(col("revenue_per_customer"), 2)
    ).withColumn(
        "revenue_per_product", round(col("revenue_per_product"), 2)
    ).withColumn(
        "monthly_items_per_order", round(col("monthly_items_per_order"), 2)
    ).withColumn(
        "monthly_units_per_order", round(col("monthly_units_per_order"), 2)
    ).withColumn(
        "units_per_customer", round(col("units_per_customer"), 2)
    ).withColumn(
        "units_per_product", round(col("units_per_product"), 2)
    )
    
    return final_performance

# COMMAND ----------

def validate_category_monthly_performance(df):
    """
    Validate category monthly performance data quality.
    """
    print("Validating category monthly performance...")
    
    validation_results = {
        "total_records": df.count(),
        "categories_with_revenue": df.filter(col("monthly_revenue") > 0).count(),
        "categories_without_revenue": df.filter(col("monthly_revenue") == 0).count(),
        "negative_revenue": df.filter(col("monthly_revenue") < 0).count(),
        "negative_profit": df.filter(col("monthly_profit") < 0).count(),
        "zero_orders": df.filter(col("monthly_orders") == 0).count(),
        "zero_units": df.filter(col("monthly_units_sold") == 0).count(),
        "null_categories": df.filter(col("agg_product_category").isNull()).count(),
        "duplicate_category_months": df.groupBy("agg_product_category", "year_month").count().filter(col("count") > 1).count()
    }
    
    print("Category Monthly Performance Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create gold category_monthly_performance table with category and year-month aggregated metrics.
    """
    spark = SparkSession.builder.appName("Gold Category Monthly Performance").getOrCreate()
    
    # Define table names
    silver_product_performance_table = f"{catalog_name}.{silver_schema}.product_performance"
    silver_order_line_fact_table = f"{catalog_name}.{silver_schema}.order_line_fact"
    gold_category_monthly_performance_table = f"{catalog_name}.{schema_name}.category_monthly_performance"
    
    print(f"Starting gold layer transformation for category monthly performance")
    print(f"Source tables: {silver_product_performance_table}, {silver_order_line_fact_table}")
    print(f"Target: {gold_category_monthly_performance_table}")
    
    try:
        # Read from silver layer
        silver_product_performance = spark.table(silver_product_performance_table)
        silver_order_line_fact = spark.table(silver_order_line_fact_table)
        
        print(f"Read {silver_product_performance.count()} product performance records and {silver_order_line_fact.count()} order line records")
        
        # Create category monthly performance
        category_monthly_performance = create_category_monthly_performance(silver_product_performance, silver_order_line_fact)
        
        # Validate data
        validation_results = validate_category_monthly_performance(category_monthly_performance)
        
        # Add processing metadata
        final_df = category_monthly_performance.withColumn("_processed_timestamp", current_timestamp()) \
                                               .withColumn("_processing_layer", lit("gold"))
        
        # Write to gold layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(gold_category_monthly_performance_table)
        
        print(f"✅ Successfully created gold category monthly performance table: {gold_category_monthly_performance_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from gold category monthly performance:")
        final_df.select("agg_product_category", "year_month", "monthly_revenue", "monthly_units_sold", 
                       "monthly_orders", "avg_category_rating", "profit_margin_percentage").show(5)
        
        # Show top categories by revenue
        print("\nTop 5 categories by monthly revenue:")
        final_df.orderBy(col("monthly_revenue").desc()).select("agg_product_category", "year_month", 
                                                              "monthly_revenue", "monthly_units_sold").show(5)
        
        # Show performance by quarter
        print("\nPerformance by quarter:")
        final_df.groupBy("year_quarter").agg(
            sum("monthly_revenue").alias("total_revenue"),
            sum("monthly_units_sold").alias("total_units"),
            avg("monthly_revenue").alias("avg_monthly_revenue"),
            count("agg_product_category").alias("category_count")
        ).orderBy(col("year_quarter")).show()
        
        # Show performance by season
        print("\nPerformance by season:")
        final_df.groupBy("season").agg(
            sum("monthly_revenue").alias("total_revenue"),
            sum("monthly_units_sold").alias("total_units"),
            avg("monthly_revenue").alias("avg_monthly_revenue"),
            avg("avg_category_rating").alias("avg_rating")
        ).orderBy(col("total_revenue").desc()).show()
        
        # Show category rating performance
        print("\nCategory rating performance:")
        final_df.groupBy("agg_product_category").agg(
            avg("avg_category_rating").alias("avg_rating"),
            sum("monthly_revenue").alias("total_revenue"),
            sum("monthly_units_sold").alias("total_units")
        ).orderBy(col("avg_rating").desc()).show(10)
        
    except Exception as e:
        print(f"❌ Error processing category monthly performance: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 