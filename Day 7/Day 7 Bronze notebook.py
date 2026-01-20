# Databricks notebook source
dbutils.widgets.text("input_path", "/Volumes/workspace/ecommerce/ecommerce_data/", "Input Path")
dbutils.widgets.text("target_table", "workspace.ecommerce_bronze.orders", "Target Table")


# COMMAND ----------

input_path = dbutils.widgets.get("input_path")
target_table = dbutils.widgets.get("target_table")

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)
