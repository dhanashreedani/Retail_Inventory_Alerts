# Databricks notebook source
# MAGIC %run /Users/shreya.dani22@gmail.com/Final_project/Logging

# COMMAND ----------

silver_dim_products_all = spark.table("retail_catalog.retail_schema.silver_dim_product_all")

# COMMAND ----------

silver_dim_products = spark.table("retail_catalog.retail_schema.silver_dim_product_all").filter("is_current = true")

# COMMAND ----------

silver_fact_inventory = spark.table("retail_catalog.retail_schema.silver_fact_inventory")

# COMMAND ----------

silver_fact_product_metrics = spark.table("retail_catalog.retail_schema.silver_fact_product_metrics")

# COMMAND ----------

silver_fact_reviews = spark.table("retail_catalog.retail_schema.silver_fact_reviews")

# COMMAND ----------

# DBTITLE 1,Log table loads
log_message("INFO", "Gold layer notebook started")
log_message("INFO", "All silver tables loaded: silver_dim_product_all, silver_fact_inventory, silver_fact_product_metrics, silver_fact_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC **Which category has most products?**

# COMMAND ----------

# DBTITLE 1,Category product count
from pyspark.sql.functions import count

log_message("INFO", "Analyzing category product distribution")

silver_dim_products.groupBy("category") \
    .agg(count("product_id").alias("total_products")) \
    .orderBy("total_products", ascending=False) \
    .display()

log_message("INFO", "Category product distribution completed")

# COMMAND ----------

# MAGIC %md
# MAGIC **average price and average discount**

# COMMAND ----------

# DBTITLE 1,Pricing strategy by category
from pyspark.sql.functions import *
from pyspark.sql.functions import round as spark_round

log_message("INFO", "Analyzing pricing strategy by category and price segment")

df_seg = silver_dim_products.withColumn(
    "price_segment",
    when(silver_dim_products.price < 50, "Low")
    .when((silver_dim_products.price >= 50) & (silver_dim_products.price < 500), "Medium")
    .otherwise("High")
)

gold_pricing_strategy = df_seg.groupBy("category", "price_segment") \
    .agg(
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_round(avg("discountPercentage"), 2).alias("avg_discount"),
        count("*").alias("product_count")
    ).orderBy("category")

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/pricing_strategy"


gold_pricing_strategy.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_pricing_strategy")
display(gold_pricing_strategy)

log_message("INFO", "gold_pricing_strategy saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **comparision of warranty info, avg price and discount**

# COMMAND ----------

from pyspark.sql.functions import round as spark_round, when, col

warranty_order = (
    when(col("warrantyInformation") == "0", 0)
    .when(col("warrantyInformation") == "1 week", 1)
    .when(col("warrantyInformation") == "1 month", 2)
    .when(col("warrantyInformation") == "3 months", 3)
    .when(col("warrantyInformation") == "6 months", 4)
    .when(col("warrantyInformation") == "1 year", 5)
    .when(col("warrantyInformation") == "2 year", 6)
    .when(col("warrantyInformation") == "3 year", 7)
    .when(col("warrantyInformation") == "5 year", 8)
    .when(col("warrantyInformation") == "lifetime", 9)
    .otherwise(10)
)

gold_warranty_analysis = silver_dim_products.groupBy("warrantyInformation") \
    .agg(
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_round(avg("discountPercentage"), 2).alias("avg_discount"),
        count("*").alias("product_count")
    ).withColumn("sort_order", warranty_order) \
    .orderBy("sort_order") \
    .drop("sort_order")

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/warranty_analysis"

gold_warranty_analysis.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_warranty_analysis")
display(gold_warranty_analysis)

log_message("INFO", "gold_warranty_analysis saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **Which products are financially risky?**

# COMMAND ----------

from pyspark.sql.functions import col

risky_products = silver_dim_products.withColumn("risk_flag",(col("price") * col("minimumOrderQuantity"))) \
    .orderBy("risk_flag", ascending=False)

risky_products.select(
    "product_id", "title", "price", "minimumOrderQuantity", "risk_flag"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **price change and discount change**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, round as spark_round

windowSpec = Window.partitionBy("product_id").orderBy("effective_start_date")

gold_price_trend = silver_dim_products_all.withColumn("prev_price", lag("price").over(windowSpec)) \
    .withColumn("price_change", spark_round(col("price") - col("prev_price"), 2)) \
    .withColumn("prev_discount", lag("discountPercentage").over(windowSpec)) \
    .withColumn("discount_change", spark_round(col("discountPercentage") - col("prev_discount"), 2))

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/price_trend"

gold_price_trend.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_price_trend")
display(gold_price_trend)

log_message("INFO", "gold_price_trend saved to gold layer")

# COMMAND ----------

# DBTITLE 1,Inventory alert by category
from pyspark.sql.functions import count, avg, round as spark_round

log_message("INFO", "Analyzing inventory alerts by category and availability")

df_joined = silver_fact_inventory.join(silver_dim_products, "product_id")

gold_inventory_summary = df_joined.groupBy("category", "availabilityStatus") \
    .agg(
        count("*").alias("product_count"),
        spark_round(avg("stock"), 2).alias("avg_stock"),
        spark_round(avg("price"), 2).alias("avg_price")
    )

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/inventory_summary"

gold_inventory_summary.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_inventory_summary")
display(gold_inventory_summary)

log_message("INFO", "gold_inventory_summary saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **stock less than 10**

# COMMAND ----------

# DBTITLE 1,Low stock alert
log_message("INFO", "Identifying low stock products (stock < 10)")

gold_low_stock_alert = df_joined.filter(df_joined.stock < 10).select(
    "product_id", "title", "category", "brand",
    "stock", "price", "availabilityStatus"
)

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/low_stock_alert"

gold_low_stock_alert.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_low_stock_alert")
display(gold_low_stock_alert)

log_message("INFO", "gold_low_stock_alert saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **overstock**

# COMMAND ----------

# DBTITLE 1,Overstock alert
log_message("INFO", "Identifying overstock products (stock > 2x minimumOrderQuantity)")

gold_overstock_alert = df_joined.filter(
    df_joined.stock > df_joined.minimumOrderQuantity * 2
).select(
    "product_id", "title", "category",
    "stock", "minimumOrderQuantity", "price"
)

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/overstock_alert"

gold_overstock_alert.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_overstock_alert")
display(gold_overstock_alert)

log_message("INFO", "gold_overstock_alert saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **stock change**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

windowSpec = Window.partitionBy("product_id").orderBy("updated_date")

gold_inventory_trend = silver_fact_inventory.withColumn(
    "prev_stock", lag("stock").over(windowSpec)
).withColumn(
    "stock_change", col("stock") - col("prev_stock")
)

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/inventory_trend"

gold_inventory_trend.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_inventory_trend")
display(gold_inventory_trend)

log_message("INFO", "gold_inventory_trend saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **Demand**

# COMMAND ----------

# DBTITLE 1,Smart demand alerts
log_message("INFO", "Generating smart demand alerts")

df_all = silver_fact_inventory.alias("inv") \
    .join(silver_dim_products.alias("prod"), "product_id") \
    .join(silver_fact_product_metrics.alias("met"), "product_id")

gold_smart_demand_alerts = df_all.select(
    col("product_id"),
    col("prod.title"),
    col("prod.category"),
    col("prod.brand"),
    col("inv.stock"),
    col("inv.availabilityStatus"),
    col("prod.price"),
    col("met.rating")
).withColumn(
    "alert_type",
    when((col("stock") < 10) & (col("rating") > 4), "High Demand - Restock Urgently")
    .when((col("stock") > 100) & (col("rating") < 3), "Overstock - Low Demand")
    .otherwise("Normal")
)

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/smart_demand_alert"

gold_smart_demand_alerts.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_smart_demand_alerts")
display(gold_smart_demand_alerts)

log_message("INFO", "gold_smart_demand_alerts saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC **Average rating per product**

# COMMAND ----------

# DBTITLE 1,Reviews by category
from pyspark.sql.functions import round as spark_round

log_message("INFO", "Analyzing reviews by category")

joined_reviews = silver_fact_reviews.join(silver_dim_products, "product_id")

gold_reviews_summary = joined_reviews.groupBy("product_id", "title", "category", "brand").agg(
    count("*").alias("review_count"),
    spark_round(avg("review_rating"), 2).alias("avg_rating")
).orderBy("product_id")

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/reviews_summary"
gold_reviews_summary.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_reviews_summary")
display(gold_reviews_summary)

log_message("INFO", "gold_reviews_summary saved to gold layer")

# COMMAND ----------

# DBTITLE 1,Rating vs Price Correlation
# MAGIC %md
# MAGIC    
# MAGIC **Rating vs Price Correlation**

# COMMAND ----------

# DBTITLE 1,Rating vs Price Correlation
from pyspark.sql.functions import corr, round as spark_round

log_message("INFO", "Starting Rating vs Price Correlation analysis")

df_price_rating = silver_dim_products.join(silver_fact_product_metrics, "product_id")

gold_rating_vs_price = df_price_rating.groupBy("category").agg(
    spark_round(avg("price"), 2).alias("avg_price"),
    spark_round(avg("rating"), 2).alias("avg_rating"),
    spark_round(corr("price", "rating"), 4).alias("price_rating_correlation"),
    count("*").alias("product_count")
).orderBy("category")

target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/rating_vs_price"

gold_rating_vs_price.write.mode("overwrite").option("path", target_path)\
    .saveAsTable("retail_catalog.retail_schema.gold_rating_vs_price")
display(gold_rating_vs_price)

log_message("INFO", "gold_rating_vs_price saved to gold layer")

# COMMAND ----------

# DBTITLE 1,Time-based Review Trend
# MAGIC %md
# MAGIC    
# MAGIC **Time-based Review Trend**

# COMMAND ----------

# DBTITLE 1,Time-based Review Trend
from pyspark.sql.functions import date_format

log_message("INFO", "Starting Time-based Review Trend analysis")

gold_review_trend = joined_reviews.groupBy(
    date_format("review_date", "yyyy-MM").alias("review_month")
).agg(
    count("*").alias("review_count"),
    spark_round(avg("review_rating"), 2).alias("avg_rating")
).orderBy("review_month")


target_path = "abfss://gold@finalprojstore.dfs.core.windows.net/review_trend"

gold_review_trend.write.mode("overwrite").option("path", target_path).saveAsTable("retail_catalog.retail_schema.gold_review_trend")
   
display(gold_review_trend)

log_message("INFO", "gold_review_trend saved to gold layer")

# COMMAND ----------

# DBTITLE 1,Logging Summary
# MAGIC %md
# MAGIC    
# MAGIC **Logging Summary**

# COMMAND ----------

# DBTITLE 1,Display log records
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

log_message("INFO", "Gold layer analysis completed")

# Save log records to a DataFrame for review
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])
df_logs = spark.createDataFrame(log_records, schema)

df_logs.write.format("delta") \
    .mode("append") \
    .save("abfss://gold@finalprojstore.dfs.core.windows.net/logs/ext_loc_catalog/")

log_message("INFO", "Logs saved to abfss://gold@finalprojstore.dfs.core.windows.net/logs/ext_loc_catalog/")
print("\n--- Log Summary ---")
display(df_logs)