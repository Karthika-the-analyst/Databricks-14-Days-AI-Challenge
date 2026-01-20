# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 5 Tasks:
# MAGIC
# MAGIC 1. Implement incremental MERGE
# MAGIC 2. Query historical versions
# MAGIC 3. Optimize tables
# MAGIC 4. Clean old files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Implement Incremental MERGE

# COMMAND ----------

#Read November CSV
Nov_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

Nov_events.createOrReplaceTempView("incremental_events")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.ecommerce.oct_events_delta_sql AS target
# MAGIC USING incremental_events AS source
# MAGIC ON target.event_time = source.event_time
# MAGIC    AND target.user_id = source.user_id
# MAGIC    AND target.product_id = source.product_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Query Historical Versions (Delta Time Travel)

# COMMAND ----------

# MAGIC %md
# MAGIC View Delta Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Query a Specific Version (VERSION AS OF)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql
# MAGIC VERSION AS OF 0;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Compare with Current Version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Query by Timestamp (TIMESTAMP AS OF)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql
# MAGIC TIMESTAMP AS OF '2026-01-14 21:00:00'
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Time Travel Works
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.ecommerce.oct_events_delta_sql VERSION AS OF 1;
# MAGIC

# COMMAND ----------

# After the incremental load,
%sql
SELECT COUNT(*)
FROM workspace.ecommerce.oct_events_delta_sql VERSION AS OF 2;


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Optimize Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understand WHY OPTIMIZE is needed
# MAGIC
# MAGIC Delta tables often end up with many small files due to:
# MAGIC
# MAGIC Frequent appends
# MAGIC
# MAGIC MERGE operations
# MAGIC
# MAGIC Incremental pipelines
# MAGIC
# MAGIC 👉 Small files = slow queries
# MAGIC
# MAGIC 👉 OPTIMIZE = compacts files into larger ones

# COMMAND ----------

# MAGIC %md
# MAGIC Run OPTIMIZE (Basic)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize with Z-ORDER (Recommended ⭐)
# MAGIC
# MAGIC Use Z-ORDER on columns that are:
# MAGIC
# MAGIC Frequently filtered
# MAGIC
# MAGIC Used in joins

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.ecommerce.oct_events_delta_sql
# MAGIC ZORDER BY (user_id, event_time);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Best Practices (Remember This )
# MAGIC
# MAGIC Run OPTIMIZE:
# MAGIC
# MAGIC - After large MERGE operations
# MAGIC
# MAGIC - On frequently queried tables
# MAGIC
# MAGIC - Don’t run it after every small write
# MAGIC
# MAGIC - Use Z-ORDER only on selective columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Clean Old Files (VACUUM)

# COMMAND ----------

# MAGIC %md
# MAGIC Understand What VACUUM Does
# MAGIC
# MAGIC After:
# MAGIC
# MAGIC MERGE,
# MAGIC OPTIMIZE,
# MAGIC DELETE
# MAGIC
# MAGIC Delta keeps old files for:
# MAGIC
# MAGIC Time travel,
# MAGIC Rollback,
# MAGIC Auditing
# MAGIC
# MAGIC 👉 VACUUM removes those unused files

# COMMAND ----------

# MAGIC %md
# MAGIC Run VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC What this does
# MAGIC
# MAGIC Keeps last 7 days of history (default)
# MAGIC
# MAGIC Removes older unused files
# MAGIC
# MAGIC Safe for most use cases
# MAGIC
# MAGIC ✅ Recommended for learning & production

# COMMAND ----------

# MAGIC %md
# MAGIC Verify VACUUM Ran

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.ecommerce.oct_events_delta_sql;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Understand Retention Policy 
# MAGIC
# MAGIC Default retention:
# MAGIC
# MAGIC 7 days (168 hours)
# MAGIC
# MAGIC
# MAGIC Meaning:
# MAGIC
# MAGIC Time travel works for last 7 days
# MAGIC
# MAGIC Older versions are removed

# COMMAND ----------

# MAGIC %md
# MAGIC Best Practices
# MAGIC
# MAGIC ✔ Use default VACUUM in production
# MAGIC
# MAGIC ✔ Run VACUUM after OPTIMIZE
# MAGIC
# MAGIC ✔ Never run RETAIN 0 HOURS accidentally
# MAGIC
# MAGIC ✔ Schedule VACUUM off-peak