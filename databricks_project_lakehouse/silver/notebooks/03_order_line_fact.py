# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, upper, trim, 
    regexp_replace, to_date, year, month, dayofmonth,
    coalesce, isnan, isnull, length, substring, lower, concat_ws, datediff,
    sum, count, avg, max, min, countDistinct, round, expr, dayofweek
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

def clean_product_data(df):
    """
    Clean and validate product data from bronze layer.
    """
    print("Cleaning product data...")
    
    cleaned_df = df.select(
        col("product_id").cast(StringType()).alias("product_id"),
        trim(upper(col("product_name"))).alias("product_name"),
        trim(upper(col("category"))).alias("category"),
        round(col("price").cast(DoubleType()), 2).alias("price"),
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("job_id")
    )
    
    # Add derived columns
    cleaned_df = cleaned_df.withColumn(
        "price_category",
        when(col("price") < 10, "Budget")
        .when(col("price") < 50, "Mid-range")
        .when(col("price") < 200, "Premium")
        .otherwise("Luxury")
    ).withColumn(
        "product_category_group",
        when(col("category").isin(["ELECTRONICS", "TECH"]), "Technology")
        .when(col("category").isin(["CLOTHING", "APPAREL"]), "Apparel")
        .when(col("category").isin(["FOOD", "GROCERY"]), "Grocery")
        .when(col("category").isin(["BOOKS", "EDUCATION"]), "Education")
        .when(col("category").isin(["HOME", "GARDEN"]), "Home & Garden")
        .when(col("category").isin(["SPORTS", "RECREATION"]), "Sports & Recreation")
        .when(col("category").isin(["BEAUTY", "HEALTH"]), "Health & Beauty")
        .when(col("category").isin(["TOYS", "ENTERTAINMENT"]), "Entertainment")
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
    
    # Add date dimension columns
    cleaned_df = cleaned_df.withColumn(
        "order_year", year(col("order_date"))
    ).withColumn(
        "order_month", month(col("order_date"))
    ).withColumn(
        "order_day", dayofmonth(col("order_date"))
    ).withColumn(
        "order_day_of_week", dayofweek(col("order_date"))
    ).withColumn(
        "order_quarter",
        when(col("order_month") <= 3, 1)
        .when(col("order_month") <= 6, 2)
        .when(col("order_month") <= 9, 3)
        .otherwise(4)
    ).withColumn(
        "order_season",
        when(col("order_month").isin([12, 1, 2]), "Winter")
        .when(col("order_month").isin([3, 4, 5]), "Spring")
        .when(col("order_month").isin([6, 7, 8]), "Summer")
        .otherwise("Fall")
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
    ).withColumn(
        "line_discount",
        lit(0.0)  # Placeholder for discount logic
    ).withColumn(
        "line_tax",
        col("line_total") * 0.1  # Placeholder for tax calculation
    ).withColumn(
        "line_total_with_tax",
        col("line_total") + col("line_tax") - col("line_discount")
    )
    
    return cleaned_df

# COMMAND ----------

def create_order_line_fact(customers_df, products_df, orders_df, order_items_df):
    """
    Create order line fact table with all dimensions joined.
    """
    print("Creating order line fact table...")
    
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
        col("_ingestion_timestamp").alias("customer_ingestion_timestamp"),
        col("_source_file").alias("customer_source_file"),
        col("job_id").alias("customer_job_id")
    )
    
    products_renamed = products_df.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("price"),
        col("price_category"),
        col("product_category_group"),
        col("_ingestion_timestamp").alias("product_ingestion_timestamp"),
        col("_source_file").alias("product_source_file"),
        col("job_id").alias("product_job_id")
    )
    
    orders_renamed = orders_df.select(
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("order_status"),
        col("order_year"),
        col("order_month"),
        col("order_day"),
        col("order_day_of_week"),
        col("order_quarter"),
        col("order_season"),
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
        col("line_discount"),
        col("line_tax"),
        col("line_total_with_tax"),
        col("_ingestion_timestamp").alias("order_item_ingestion_timestamp"),
        col("_source_file").alias("order_item_source_file"),
        col("job_id").alias("order_item_job_id")
    )
    
    # Join order items with orders
    order_line_with_orders = order_items_renamed.join(orders_renamed, "order_id", "left")
    
    # Join with customers
    order_line_with_customers = order_line_with_orders.join(customers_renamed, "customer_id", "left")
    
    # Join with products
    order_line_fact = order_line_with_customers.join(products_renamed, "product_id", "left")
    
    # Add business logic and derived metrics
    final_fact = order_line_fact.withColumn(
        "profit_margin",
        when(col("price") > 0, (col("unit_price") - col("price")) / col("unit_price") * 100).otherwise(0)
    ).withColumn(
        "profit_amount",
        col("line_total") * (col("profit_margin") / 100)
    ).withColumn(
        "order_value_tier",
        when(col("line_total") >= 100, "High Value")
        .when(col("line_total") >= 50, "Medium Value")
        .when(col("line_total") >= 10, "Low Value")
        .otherwise("Minimal Value")
    ).withColumn(
        "customer_product_affinity",
        when((col("customer_segment") == "VIP") & (col("price_category") == "Luxury"), "Premium Match")
        .when((col("customer_segment") == "New") & (col("price_category") == "Budget"), "Entry Match")
        .otherwise("Standard Match")
    ).withColumn(
        "seasonal_product_flag",
        when((col("order_season") == "Winter") & (col("product_category_group").isin(["Apparel", "Home & Garden"])), True)
        .when((col("order_season") == "Summer") & (col("product_category_group").isin(["Sports & Recreation", "Apparel"])), True)
        .otherwise(False)
    )
    
    # Round numeric columns
    final_fact = final_fact.withColumn(
        "line_total", round(col("line_total"), 2)
    ).withColumn(
        "line_total_with_tax", round(col("line_total_with_tax"), 2)
    ).withColumn(
        "profit_margin", round(col("profit_margin"), 2)
    ).withColumn(
        "profit_amount", round(col("profit_amount"), 2)
    ).withColumn(
        "line_tax", round(col("line_tax"), 2)
    ).withColumn(
        "line_discount", round(col("line_discount"), 2)
    )
    
    return final_fact

# COMMAND ----------

def validate_order_line_fact(df):
    """
    Validate order line fact table data quality.
    """
    print("Validating order line fact table...")
    
    validation_results = {
        "total_order_lines": df.count(),
        "null_order_item_ids": df.filter(col("order_item_id").isNull()).count(),
        "duplicate_order_item_ids": df.groupBy("order_item_id").count().filter(col("count") > 1).count(),
        "negative_line_totals": df.filter(col("line_total") < 0).count(),
        "zero_quantities": df.filter(col("quantity") <= 0).count(),
        "missing_customer_data": df.filter(col("customer_id").isNull()).count(),
        "missing_product_data": df.filter(col("product_id").isNull()).count(),
        "missing_order_data": df.filter(col("order_id").isNull()).count(),
        "future_order_dates": df.filter(col("order_date") > current_timestamp()).count()
    }
    
    print("Order Line Fact Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create silver order_line_fact table ready for star schema.
    """
    spark = SparkSession.builder.appName("Silver Order Line Fact").getOrCreate()
    
    # Define table names
    bronze_customers_table = f"{catalog_name}.{bronze_schema}.customers"
    bronze_products_table = f"{catalog_name}.{bronze_schema}.products"
    bronze_orders_table = f"{catalog_name}.{bronze_schema}.orders"
    bronze_order_items_table = f"{catalog_name}.{bronze_schema}.order_items"
    silver_order_line_fact_table = f"{catalog_name}.{schema_name}.order_line_fact"
    
    print(f"Starting silver layer transformation for order line fact table")
    print(f"Source tables: {bronze_customers_table}, {bronze_products_table}, {bronze_orders_table}, {bronze_order_items_table}")
    print(f"Target: {silver_order_line_fact_table}")
    
    try:
        # Read from bronze layer
        bronze_customers = spark.table(bronze_customers_table)
        bronze_products = spark.table(bronze_products_table)
        bronze_orders = spark.table(bronze_orders_table)
        bronze_order_items = spark.table(bronze_order_items_table)
        
        print(f"Read {bronze_customers.count()} customers, {bronze_products.count()} products, {bronze_orders.count()} orders, {bronze_order_items.count()} order items")
        
        # Clean data
        cleaned_customers = clean_customer_data(bronze_customers)
        cleaned_products = clean_product_data(bronze_products)
        cleaned_orders = clean_order_data(bronze_orders)
        cleaned_order_items = clean_order_items_data(bronze_order_items)
        
        # Create order line fact table
        order_line_fact = create_order_line_fact(cleaned_customers, cleaned_products, cleaned_orders, cleaned_order_items)
        
        # Validate data
        validation_results = validate_order_line_fact(order_line_fact)
        
        # Add processing metadata
        final_df = order_line_fact.withColumn("_processed_timestamp", current_timestamp()) \
                                 .withColumn("_processing_layer", lit("silver"))
        
        # Write to silver layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(silver_order_line_fact_table)
        
        print(f"✅ Successfully created silver order line fact table: {silver_order_line_fact_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from silver order line fact table:")
        final_df.select("order_item_id", "customer_name", "product_name", "line_total", 
                       "order_value_tier", "customer_product_affinity").show(5)
        
        # Show order value tier distribution
        print("\nOrder value tier distribution:")
        final_df.groupBy("order_value_tier").count().orderBy(col("count").desc()).show()
        
        # Show revenue by product category
        print("\nRevenue by product category:")
        final_df.groupBy("product_category_group").agg(
            sum("line_total").alias("total_revenue"),
            count("order_item_id").alias("total_orders")
        ).orderBy(col("total_revenue").desc()).show()
        
        # Show revenue by customer segment
        print("\nRevenue by customer segment:")
        final_df.groupBy("customer_segment").agg(
            sum("line_total").alias("total_revenue"),
            count("order_item_id").alias("total_orders")
        ).orderBy(col("total_revenue").desc()).show()
        
    except Exception as e:
        print(f"❌ Error processing order line fact table: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 