# Databricks notebook source
# MAGIC %run /Users/shreya.dani22@gmail.com/Final_project/Logging

# COMMAND ----------

# MAGIC %run /Users/shreya.dani22@gmail.com/Final_project/Bronze_layer

# COMMAND ----------

from pyspark.sql.functions import explode

df_products = df.select(explode("products").alias("product"))

# COMMAND ----------

df_products.display()

# COMMAND ----------

from pyspark.sql.functions import col, expr, to_timestamp, current_timestamp

products = df_products.select(
    col("product.id").alias("product_id"),
    col("product.title"),
    col("product.category"),
    col("product.brand"),
    col("product.sku"),
    col("product.price"),
    col("product.discountPercentage"),
    col("product.availabilityStatus"),
    col("product.returnPolicy"),
    col("product.minimumOrderQuantity"),
    col("product.rating"),
    col("product.stock"),
    col("product.warrantyInformation"),
    col("product.shippingInformation"),

    #time
    to_timestamp(col("product.meta.createdAt")).alias("created_at"),
    to_timestamp(col("product.meta.updatedAt")).alias("updated_at"),
    current_timestamp().alias("ingestion_time")
)

    

# COMMAND ----------

products.display()

# COMMAND ----------

from pyspark.sql.functions import lit

products_clean = products.fillna({
    "title": "Unknown",
    "category": "Unknown",
    "brand": "Unknown",
    "sku": "NA",
    "availabilityStatus": "NA",
    "returnPolicy": "Not Available",
    "warrantyInformation": "Not Available",
    "shippingInformation": "Not Available",
    "minimumOrderQuantity": 1
})

# COMMAND ----------

from pyspark.sql.functions import to_date, date_format

products_clean = products_clean \
    .withColumn("created_date", to_date("created_at"))\
    .withColumn("updated_date", to_date("updated_at")) 
    
    

# COMMAND ----------

from pyspark.sql.functions import *

products_clean = products_clean.withColumn(
    "warrantyInformation",
    
    when(lower(col("warrantyInformation")).contains("no warranty"), "0")
    
    .when(lower(col("warrantyInformation")).contains("lifetime"), "lifetime")
    
    .otherwise(
        trim(
            regexp_replace(
                lower(col("warrantyInformation")),
                "warranty",
                ""
            )
        )
    )
)

# COMMAND ----------

products_clean = products_clean.withColumn(
    "returnPolicy",
    when(col("returnPolicy").contains("day"),
         regexp_extract("returnPolicy", r"\d+", 0).cast("int"))
    
    .when(col("returnPolicy").contains("No return"), 0)
    
    .otherwise(None)
)

# COMMAND ----------

products_clean = products_clean.withColumn(
    "shippingInformation",
    
    when(col("shippingInformation").contains("overnight"), 1)
    
    .when(col("shippingInformation").rlike(r"\d+-\d+"),
          regexp_extract("shippingInformation", r"-(\d+)", 1).cast("int"))
    
    .when(col("shippingInformation").contains("week"),
          regexp_extract("shippingInformation", r"\d+", 0).cast("int") * 7)
    
    .when(col("shippingInformation").contains("month"),
          regexp_extract("shippingInformation", r"\d+", 0).cast("int") * 30)
    
    .when(col("shippingInformation").contains("day"),
          regexp_extract("shippingInformation", r"\d+", 0).cast("int"))
    
    .otherwise(None)
)

# COMMAND ----------

from pyspark.sql.functions import trim, lower

products_clean = products_clean.withColumn("category", trim(lower(col("category")))) \
                   .withColumn("brand", trim(lower(col("brand"))))

# COMMAND ----------

products_clean = products_clean.filter(col("product_id").isNotNull())

# COMMAND ----------

products_clean.display()

# COMMAND ----------

from pyspark.sql.functions import explode

df_reviews = df_products.select(
    col("product.id").alias("product_id"),
    explode("product.reviews").alias("review")
)

# COMMAND ----------

reviews = df_reviews.select(
    col("product_id"),
    col("review.rating").alias("review_rating"),
    col("review.comment").alias("review_comment"),
    col("review.date").alias("review_date"),
    col("review.reviewerName").alias("reviewer_name")
)

# COMMAND ----------

reviews.display()

# COMMAND ----------

reviews_clean = reviews.withColumn(
    "review_date",
    to_timestamp(col("review_date"))
)

# COMMAND ----------

reviews_clean = reviews.withColumn(
    "review_date",
    to_date(col("review_date"))
)

# COMMAND ----------

reviews_clean = reviews_clean.filter(col("product_id").isNotNull())

# COMMAND ----------

from pyspark.sql.functions import length

reviews_clean = reviews_clean.filter(length(col("review_comment")) > 0)

# COMMAND ----------

reviews_clean.display()

log_message("INFO","Data cleaning completed successfully")

# COMMAND ----------

dim_products = products_clean.select(
    "product_id",
    "title",
    "category",
    "brand",
    "sku",
    "price",
    "discountPercentage",
    "warrantyInformation",
    "shippingInformation",
    "returnPolicy",
    "minimumOrderQuantity",
    "ingestion_time"
)
dim_products.display()

# COMMAND ----------

fact_inventory = products_clean.select(
    "product_id",
    "stock",
    "availabilityStatus",
    "updated_at",
    "updated_date"
)
fact_inventory.display()

# COMMAND ----------

fact_products_metrics = products_clean.select(
    "product_id",
    "rating",
    "updated_at"
)
fact_products_metrics.display()


# COMMAND ----------

fact_reviews = reviews_clean.select(
    "product_id",
    "review_rating",
    "review_comment",
    "review_date"
)
fact_reviews.display()

log_message("INFO","Table splitting completed (dim + fact tables created)")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, lit

try:
    log_message("INFO", "Starting SCD Type 2 process")

    table_name = "retail_catalog.retail_schema.silver_dim_product_all"
    target_path = "abfss://silver@finalprojstore.dfs.core.windows.net/dim_product_all"

    if not spark.catalog.tableExists(table_name):

        source_df = dim_products.withColumn("effective_start_date", current_timestamp()) \
            .withColumn("effective_end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))

        source_df.write.format("delta") \
            .mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable(table_name)

    else:
        target = DeltaTable.forName(spark, table_name)
        target_df = target.toDF()

        joined_df = dim_products.alias("s").join(
            target_df.filter(col("is_current") == True).alias("t"),
            "product_id",
            "left"
        )

        changed_df = joined_df.filter("""
            t.product_id IS NOT NULL AND (
                t.price <> s.price OR
                t.discountPercentage <> s.discountPercentage OR
                t.warrantyInformation <> s.warrantyInformation OR
                t.shippingInformation <> s.shippingInformation OR
                t.returnPolicy <> s.returnPolicy OR
                t.minimumOrderQuantity <> s.minimumOrderQuantity
            )
        """).select("s.*")

        new_df = joined_df.filter("t.product_id IS NULL").select("s.*")

        final_insert_df = changed_df.union(new_df) \
            .withColumn("effective_start_date", current_timestamp()) \
            .withColumn("effective_end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))

        target.alias("t").merge(
            changed_df.alias("s"),
            "t.product_id = s.product_id AND t.is_current = true"
        ).whenMatchedUpdate(
            set={
                "effective_end_date": "current_timestamp()",
                "is_current": "false"
            }
        ).execute()

        final_insert_df.write.format("delta") \
            .mode("append") \
            .saveAsTable(table_name)

    log_message("INFO", "SCD Type 2 completed successfully")

except Exception as e:
    log_message("ERROR", f"SCD failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.retail_schema.silver_dim_product_all

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

try:
    log_message("INFO", "Starting fact_inventory process")

    table_name = "retail_catalog.retail_schema.silver_fact_inventory"
    target_path = "abfss://silver@finalprojstore.dfs.core.windows.net/fact_inventory"

    if spark.catalog.tableExists(table_name):
        last_run = spark.sql(f"""
            SELECT max(updated_at) as last_run 
            FROM {table_name}
        """).collect()[0]["last_run"]

        fact_inventory_inc = fact_inventory.filter(col("updated_at") > last_run)
    else:
        fact_inventory_inc = fact_inventory

    if not spark.catalog.tableExists(table_name):
        log_message("INFO", "Initial load for fact_inventory")

        fact_inventory_inc.write.format("delta") \
            .mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable(table_name)

    else:
        log_message("INFO", "Performing incremental merge")

        target = DeltaTable.forName(spark, table_name)

        target.alias("t").merge(
            fact_inventory_inc.alias("s"),
            "t.product_id = s.product_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    log_message("INFO", "fact_inventory process completed")

except Exception as e:
    log_message("ERROR", f"Incremental load failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.retail_schema.silver_fact_inventory

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

try:
    log_message("INFO", "Starting incremental load for fact_products_metrics")

    table_name = "retail_catalog.retail_schema.silver_fact_product_metrics"
    target_path = "abfss://silver@finalprojstore.dfs.core.windows.net/fact_product_metrics"

    if spark.catalog.tableExists(table_name):
        last_run = spark.sql(f"""
            SELECT max(updated_at) as last_run 
            FROM {table_name}
        """).collect()[0]["last_run"]

        fact_products_metrics_inc = fact_products_metrics.filter(col("updated_at") > last_run)
    else:
        fact_products_metrics_inc = fact_products_metrics

    if not spark.catalog.tableExists(table_name):

        fact_products_metrics_inc.write.format("delta") \
            .mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable(table_name)

        log_message("INFO", "Initial load completed")

    else:
        target = DeltaTable.forName(spark, table_name)

        target.alias("t").merge(
            fact_products_metrics_inc.alias("s"),
            "t.product_id = s.product_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        log_message("INFO", "Incremental load completed")

except Exception as e:
    log_message("ERROR", f"Incremental load failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.retail_schema.silver_fact_product_metrics

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

try:
    log_message("INFO", "Starting incremental load for fact_reviews")

    table_name = "retail_catalog.retail_schema.silver_fact_reviews"
    target_path = "abfss://silver@finalprojstore.dfs.core.windows.net/fact_reviews"

    if spark.catalog.tableExists(table_name):
        last_run = spark.sql(f"""
            SELECT max(review_date) as last_run 
            FROM {table_name}
        """).collect()[0]["last_run"]

        fact_reviews_inc = fact_reviews.filter(col("review_date") > last_run)

        log_message("INFO", f"Filtering new records after {last_run}")
    else:
        fact_reviews_inc = fact_reviews

    if not spark.catalog.tableExists(table_name):

        fact_reviews.write.format("delta") \
        .mode("overwrite") \
        .option("path", target_path) \
        .saveAsTable(table_name)

        log_message("INFO", "Initial load completed for fact_reviews")

    else:
        target = DeltaTable.forName(spark, table_name)

        target.alias("t").merge(
            fact_reviews_inc.alias("s"),
            """
            t.product_id = s.product_id AND
            t.review_comment = s.review_comment AND
            t.review_date = s.review_date
            """
        ).whenNotMatchedInsertAll().execute()

        log_message("INFO", "Incremental load completed for fact_reviews")

except Exception as e:
    log_message("ERROR", f"Incremental load failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.retail_schema.silver_fact_reviews