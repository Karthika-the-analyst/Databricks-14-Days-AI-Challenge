# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 8 Tasks:
# MAGIC
# MAGIC 1. Create catalog & schemas
# MAGIC 2. Register Delta tables
# MAGIC 3. Set up permissions
# MAGIC 4. Create views for controlled access

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog & schemas

# COMMAND ----------

# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_catalog;
# MAGIC
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.gold;
# MAGIC
# MAGIC SHOW SCHEMAS IN ecommerce_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Read Raw Data

# COMMAND ----------

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register Bronze Table

# COMMAND ----------

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecommerce_catalog.bronze.orders;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze table

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df_silver = (
    spark.read.table("ecommerce_catalog.bronze.orders")
    .filter(col("user_id").isNotNull())
    .filter(col("price") > 0)
    .withColumn("event_time", to_timestamp("event_time"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register Silver table

# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.silver.orders_clean")


# COMMAND ----------

# MAGIC %md
# MAGIC validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecommerce_catalog.silver.orders_clean;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Register Gold Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC Create Aggregates

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, to_date

df_gold = (
    spark.read.table("ecommerce_catalog.silver.orders_clean")
    .withColumn("order_date", to_date("event_time"))
    .groupBy("order_date")
    .agg(
        sum("price").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("price").alias("avg_order_value")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register Gold Table

# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.daily_sales")


# COMMAND ----------

# MAGIC %md
# MAGIC validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_catalog.gold.daily_sales LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA ecommerce_catalog.bronze
# MAGIC TO `karthika2738@gmail.com`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA ecommerce_catalog.silver
# MAGIC TO `karthika2738@gmail.com`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON SCHEMA ecommerce_catalog.gold
# MAGIC TO `karthika2738@gmail.com`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create views for controlled access

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ecommerce_catalog.gold.daily_sales_view AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue,
# MAGIC   total_orders,
# MAGIC   avg_order_value
# MAGIC FROM ecommerce_catalog.gold.daily_sales;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 28
# MAGIC %sql
# MAGIC GRANT SELECT
# MAGIC ON VIEW ecommerce_catalog.gold.daily_sales_view
# MAGIC TO `account users`;