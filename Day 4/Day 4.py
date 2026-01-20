# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 4 Tasks:
# MAGIC
# MAGIC 1. Convert CSV to Delta format
# MAGIC 2. Create Delta tables (SQL and PySpark)
# MAGIC 3. Test schema enforcement
# MAGIC 4. Handle duplicate inserts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Convert CSV to Delta Format

# COMMAND ----------

#Read CSV
oct_df = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)





# COMMAND ----------

#
oct_df.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/ecommerce/ecommerce_data/oct_events_delta")


# COMMAND ----------

# MAGIC %md
# MAGIC Verify Delta Files

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/ecommerce/ecommerce_data/oct_events_delta
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Create Delta Tables (SQL & PySpark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Create table from Delta location

# COMMAND ----------

# DBTITLE 1,Create Delta table in Unity Catalog
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.ecommerce.oct_events_delta_sql
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/ecommerce/ecommerce_data/oct_events_delta`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.ecommerce.oct_events_delta_sql;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table using PySpark

# COMMAND ----------

delta_df = spark.read.format("delta") \
    .load("/Volumes/workspace/ecommerce/ecommerce_data/oct_events_delta")


# COMMAND ----------

# MAGIC %md
# MAGIC Save as permanent table

# COMMAND ----------

delta_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce.oct_events_delta_pyspark")


# COMMAND ----------

# MAGIC %md
# MAGIC Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM workspace.ecommerce.oct_events_delta_pyspark;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Schema Enforcement

# COMMAND ----------

# MAGIC %md
# MAGIC Try–Except Testing

# COMMAND ----------

try:
    wrong_schema_df.write.format("delta") \
        .mode("append") \
        .saveAsTable("workspace.ecommerce.oct_events_delta_sql")
    print("No error – schema evolution enabled")

except Exception as e:
    print("Schema enforcement triggered")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Handle Duplicate Inserts

# COMMAND ----------

# MAGIC %md
# MAGIC Problem: Duplicate Append

# COMMAND ----------

# DBTITLE 1,Append to Delta table
oct_df = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)

oct_df.write.format("delta") \
    .mode("append") \
    .save("/Volumes/workspace/ecommerce/ecommerce_data/oct_events_delta")


# COMMAND ----------

# MAGIC %md
# MAGIC Solution: Use MERGE (Upsert)

# COMMAND ----------

#Create temp view

oct_df.createOrReplaceTempView("oct_events_temp")

# COMMAND ----------

# DBTITLE 1,Use MERGE in SQL
# MAGIC %sql
# MAGIC MERGE INTO workspace.ecommerce.oct_events_delta_sql target
# MAGIC USING oct_events_temp source
# MAGIC ON target.event_time = source.event_time
# MAGIC    AND target.user_id = source.user_id
# MAGIC    AND target.product_id = source.product_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC