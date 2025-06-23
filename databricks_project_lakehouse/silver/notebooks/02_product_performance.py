# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, upper, trim, 
    regexp_replace, to_date, year, month, dayofmonth,
    coalesce, isnan, isnull, length, substring, lower, concat_ws, datediff,
    sum, count, avg, max, min, countDistinct, round, expr
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
    
    # Add data quality score
    cleaned_df = cleaned_df.withColumn(
        "data_quality_score",
        when(col("product_id").isNotNull(), 1).otherwise(0) +
        when(length(col("product_name")) > 0, 1).otherwise(0) +
        when(col("price") > 0, 1).otherwise(0) +
        when(col("category").isNotNull(), 1).otherwise(0)
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

def clean_reviews_data(df):
    """
    Clean and validate product reviews data from bronze layer.
    """
    print("Cleaning reviews data...")
    
    cleaned_df = df.select(
        col("review_id").cast(StringType()).alias("review_id"),
        col("product_id").cast(StringType()).alias("product_id"),
        col("customer_id").cast(StringType()).alias("customer_id"),
        col("rating").cast(IntegerType()).alias("rating"),
        to_date(col("review_date"), "yyyy-MM-dd").alias("review_date"),
        trim(col("review_text")).alias("review_text"),
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("job_id")
    )
    
    # Add derived columns
    cleaned_df = cleaned_df.withColumn(
        "review_length",
        length(col("review_text"))
    ).withColumn(
        "rating_category",
        when(col("rating") >= 4, "Positive")
        .when(col("rating") >= 3, "Neutral")
        .otherwise("Negative")
    )
    
    return cleaned_df

# COMMAND ----------

def create_product_performance(products_df, order_items_df, reviews_df):
    """
    Create product performance table with revenue and review aggregates.
    """
    print("Creating product performance summary...")
    
    # Rename metadata columns to avoid conflicts during joins
    products_renamed = products_df.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("price"),
        col("price_category"),
        col("product_category_group"),
        col("data_quality_score"),
        col("_ingestion_timestamp").alias("product_ingestion_timestamp"),
        col("_source_file").alias("product_source_file"),
        col("job_id").alias("product_job_id")
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
    
    reviews_renamed = reviews_df.select(
        col("review_id"),
        col("product_id"),
        col("customer_id"),
        col("rating"),
        col("review_date"),
        col("review_text"),
        col("review_length"),
        col("rating_category"),
        col("_ingestion_timestamp").alias("review_ingestion_timestamp"),
        col("_source_file").alias("review_source_file"),
        col("job_id").alias("review_job_id")
    )
    
    # Product sales metrics
    product_sales = order_items_renamed.groupBy("product_id").agg(
        sum("line_total").alias("total_revenue"),
        sum("quantity").alias("total_quantity_sold"),
        count("order_item_id").alias("total_orders"),
        countDistinct("order_id").alias("unique_orders"),
        avg("unit_price").alias("avg_unit_price"),
        max("unit_price").alias("max_unit_price"),
        min("unit_price").alias("min_unit_price"),
        avg("quantity").alias("avg_quantity_per_order")
    )
    
    # Product review metrics
    product_reviews = reviews_renamed.groupBy("product_id").agg(
        count("review_id").alias("total_reviews"),
        avg("rating").alias("avg_rating"),
        max("rating").alias("max_rating"),
        min("rating").alias("min_rating"),
        countDistinct("customer_id").alias("unique_reviewers"),
        sum(when(col("rating") >= 4, 1).otherwise(0)).alias("positive_reviews"),
        sum(when(col("rating") <= 2, 1).otherwise(0)).alias("negative_reviews"),
        avg("review_length").alias("avg_review_length")
    )
    
    # Join all metrics with product data
    product_performance = products_renamed.join(product_sales, "product_id", "left") \
                                        .join(product_reviews, "product_id", "left")
    
    # Add derived metrics
    final_performance = product_performance.withColumn(
        "revenue_per_order",
        when(col("total_orders") > 0, col("total_revenue") / col("total_orders")).otherwise(0)
    ).withColumn(
        "positive_review_rate",
        when(col("total_reviews") > 0, col("positive_reviews") / col("total_reviews") * 100).otherwise(0)
    ).withColumn(
        "negative_review_rate",
        when(col("total_reviews") > 0, col("negative_reviews") / col("total_reviews") * 100).otherwise(0)
    ).withColumn(
        "product_popularity_score",
        when(col("total_quantity_sold") > 100, "High")
        .when(col("total_quantity_sold") > 50, "Medium")
        .when(col("total_quantity_sold") > 10, "Low")
        .otherwise("Very Low")
    ).withColumn(
        "product_rating_score",
        when(col("avg_rating") >= 4.5, "Excellent")
        .when(col("avg_rating") >= 4.0, "Good")
        .when(col("avg_rating") >= 3.5, "Average")
        .when(col("avg_rating") >= 3.0, "Below Average")
        .otherwise("Poor")
    ).withColumn(
        "product_performance_tier",
        when((col("total_revenue") >= 1000) & (col("avg_rating") >= 4.0), "Star Product")
        .when((col("total_revenue") >= 500) & (col("avg_rating") >= 3.5), "Good Performer")
        .when((col("total_revenue") >= 100) & (col("avg_rating") >= 3.0), "Average Performer")
        .otherwise("Underperforming")
    )
    
    # Round numeric columns
    final_performance = final_performance.withColumn(
        "total_revenue", round(col("total_revenue"), 2)
    ).withColumn(
        "avg_unit_price", round(col("avg_unit_price"), 2)
    ).withColumn(
        "avg_rating", round(col("avg_rating"), 2)
    ).withColumn(
        "revenue_per_order", round(col("revenue_per_order"), 2)
    ).withColumn(
        "positive_review_rate", round(col("positive_review_rate"), 2)
    ).withColumn(
        "negative_review_rate", round(col("negative_review_rate"), 2)
    ).withColumn(
        "avg_review_length", round(col("avg_review_length"), 0)
    )
    
    return final_performance

# COMMAND ----------

def validate_product_performance(df):
    """
    Validate product performance data quality.
    """
    print("Validating product performance...")
    
    validation_results = {
        "total_products": df.count(),
        "products_with_sales": df.filter(col("total_revenue") > 0).count(),
        "products_with_reviews": df.filter(col("total_reviews") > 0).count(),
        "products_with_both": df.filter((col("total_revenue") > 0) & (col("total_reviews") > 0)).count(),
        "negative_revenue": df.filter(col("total_revenue") < 0).count(),
        "invalid_ratings": df.filter((col("avg_rating") < 1) | (col("avg_rating") > 5)).count(),
        "null_product_ids": df.filter(col("product_id").isNull()).count(),
        "duplicate_product_ids": df.groupBy("product_id").count().filter(col("count") > 1).count()
    }
    
    print("Product Performance Validation Results:")
    for check, count in validation_results.items():
        print(f"  {check}: {count}")
    
    return validation_results

# COMMAND ----------

def main():
    """
    Create silver product_performance table with product metrics and aggregates.
    """
    spark = SparkSession.builder.appName("Silver Product Performance").getOrCreate()
    
    # Define table names
    bronze_products_table = f"{catalog_name}.{bronze_schema}.products"
    bronze_order_items_table = f"{catalog_name}.{bronze_schema}.order_items"
    bronze_reviews_table = f"{catalog_name}.{bronze_schema}.product_reviews"
    silver_product_performance_table = f"{catalog_name}.{schema_name}.product_performance"
    
    print(f"Starting silver layer transformation for product performance")
    print(f"Source tables: {bronze_products_table}, {bronze_order_items_table}, {bronze_reviews_table}")
    print(f"Target: {silver_product_performance_table}")
    
    try:
        # Read from bronze layer
        bronze_products = spark.table(bronze_products_table)
        bronze_order_items = spark.table(bronze_order_items_table)
        bronze_reviews = spark.table(bronze_reviews_table)
        
        print(f"Read {bronze_products.count()} products, {bronze_order_items.count()} order items, {bronze_reviews.count()} reviews")
        
        # Clean data
        cleaned_products = clean_product_data(bronze_products)
        cleaned_order_items = clean_order_items_data(bronze_order_items)
        cleaned_reviews = clean_reviews_data(bronze_reviews)
        
        # Create product performance
        product_performance = create_product_performance(cleaned_products, cleaned_order_items, cleaned_reviews)
        
        # Validate data
        validation_results = validate_product_performance(product_performance)
        
        # Add processing metadata
        final_df = product_performance.withColumn("_processed_timestamp", current_timestamp()) \
                                     .withColumn("_processing_layer", lit("silver"))
        
        # Write to silver layer
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(silver_product_performance_table)
        
        print(f"✅ Successfully created silver product performance table: {silver_product_performance_table}")
        print(f"Records processed: {final_df.count()}")
        
        # Show sample data
        print("\nSample data from silver product performance:")
        final_df.select("product_id", "product_name", "category", "total_revenue", 
                       "avg_rating", "total_reviews", "product_performance_tier").show(5)
        
        # Show performance tier distribution
        print("\nProduct performance tier distribution:")
        final_df.groupBy("product_performance_tier").count().orderBy(col("count").desc()).show()
        
        # Show top performing products
        print("\nTop 5 products by revenue:")
        final_df.orderBy(col("total_revenue").desc()).select("product_name", "total_revenue", "avg_rating").show(5)
        
    except Exception as e:
        print(f"❌ Error processing product performance: {e}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main() 