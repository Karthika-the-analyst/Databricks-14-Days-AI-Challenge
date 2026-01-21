# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ 10 Tasks:
# MAGIC
# MAGIC 1. Analyze query plans
# MAGIC 2. Partition large tables
# MAGIC 3. Apply ZORDER
# MAGIC 4. Benchmark improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Analyze Query Plan

# COMMAND ----------

# MAGIC %md
# MAGIC Pick a Query to Analyze
# MAGIC
# MAGIC Use EXPLAIN (Logical + Physical Plan)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales
# MAGIC WHERE order_date >= '2019-10-01'
# MAGIC ORDER BY order_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Partition Large Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Partitioning helps:
# MAGIC
# MAGIC Reduce scanned files
# MAGIC
# MAGIC Enable partition pruning
# MAGIC
# MAGIC Improve query speed on filters
# MAGIC
# MAGIC 👉 We partition on high-cardinality filter columns used in WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC Recreate Gold Table with Partition

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce_catalog.gold.daily_sales_partitioned
# MAGIC PARTITIONED BY (order_date)
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM ecommerce_catalog.gold.daily_sales;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Verify Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ecommerce_catalog.gold.daily_sales_partitioned;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 3: Apply Z-ORDER

# COMMAND ----------

# MAGIC %md
# MAGIC We already did partitioning ✅
# MAGIC But partitioning alone is not enough when:
# MAGIC
# MAGIC Queries filter on non-partition columns
# MAGIC
# MAGIC Data inside a partition is still large
# MAGIC
# MAGIC You want faster data skipping
# MAGIC
# MAGIC 👉 Z-ORDER improves data skipping inside partitions

# COMMAND ----------

# MAGIC %md
# MAGIC Apply Z-ORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_catalog.gold.daily_sales_partitioned
# MAGIC ZORDER BY (total_revenue);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run EXPLAIN (Validation)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned
# MAGIC WHERE order_date >= '2019-10-01'
# MAGIC ORDER BY order_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 🔍 What improved
# MAGIC
# MAGIC ✔ Partition pruning (order_date)
# MAGIC
# MAGIC ✔ Data skipping (total_revenue)
# MAGIC
# MAGIC ✔ Fewer files scanned
# MAGIC
# MAGIC ✔ Faster execution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Benchmark Improvements
# MAGIC We now compare BEFORE vs AFTER

# COMMAND ----------

# MAGIC %md
# MAGIC Capture BEFORE metrics (unpartitioned table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales
# MAGIC WHERE order_date >= '2019-10-01'
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 Note:
# MAGIC
# MAGIC Runtime
# MAGIC
# MAGIC Files scanned (from UI)
# MAGIC
# MAGIC Data read

# COMMAND ----------

# MAGIC %md
# MAGIC Capture AFTER metrics (optimized table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned
# MAGIC WHERE order_date >= '2019-10-01'
# MAGIC ORDER BY order_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 Compare:
# MAGIC
# MAGIC Runtime ↓
# MAGIC
# MAGIC Files scanned ↓
# MAGIC
# MAGIC IO cost ↓

# COMMAND ----------

# MAGIC %md
# MAGIC Although runtime remained similar due to small data size and Photon execution, execution plans confirmed partition pruning, reduced scans, and improved IO efficiency. These optimizations will significantly improve performance at scale