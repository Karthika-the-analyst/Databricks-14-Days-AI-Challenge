# Databricks notebook source
dbutils.widgets.text("source_table", "workspace.ecommerce_silver.orders_clean", "Source Table")
dbutils.widgets.text("target_table", "workspace.ecommerce_gold.daily_sales", "Target Table")


# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, to_date

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

df_gold = (
    spark.read.table(source_table)
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
    .saveAsTable(target_table)
