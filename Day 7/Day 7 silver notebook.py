# Databricks notebook source
dbutils.widgets.text("source_table", "workspace.ecommerce_bronze.orders", "Source Table")
dbutils.widgets.text("target_table", "workspace.ecommerce_silver.orders_clean", "Target Table")


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

df_silver = (
    spark.read.table(source_table)
    .filter(col("user_id").isNotNull())
    .filter(col("price") > 0)
    .withColumn("event_time", to_timestamp("event_time"))
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)
