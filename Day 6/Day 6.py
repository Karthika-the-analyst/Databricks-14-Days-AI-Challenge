# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 6 Tasks:
# MAGIC
# MAGIC 1. Design 3-layer architecture
# MAGIC 2. Build Bronze: raw ingestion
# MAGIC 3. Build Silver: cleaning & validation
# MAGIC 4. Build Gold: business aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze, Silver, Gold Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_gold;

# COMMAND ----------

# MAGIC %sql SHOW SCHEMAS IN workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer – Raw Ingestion

# COMMAND ----------

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/")
)

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce_bronze.orders")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer – Cleaning & Validation

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df_silver = (
    spark.read.table("workspace.ecommerce_bronze.orders")
    .filter(col("user_id").isNotNull())
    .filter(col("price") > 0)
    .withColumn("event_time", to_timestamp("event_time"))
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce_silver.orders_clean")


# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, to_date

df_gold = (
    spark.read.table("workspace.ecommerce_silver.orders_clean")
    .withColumn("order_date", to_date("event_time"))
    .groupBy("order_date")
    .agg(
        sum("price").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("price").alias("avg_order_value")
    )
)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce_gold.daily_sales")


# COMMAND ----------

display(df_gold)
