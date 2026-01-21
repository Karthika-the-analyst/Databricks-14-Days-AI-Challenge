# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 9 Tasks:
# MAGIC
# MAGIC 1. Create SQL warehouse
# MAGIC 2. Write analytical queries
# MAGIC 3. Build dashboard: revenue trends, funnels, top products
# MAGIC 4. Add filters & schedule refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create SQL Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC I already have 1 serverless warehouse
# MAGIC
# MAGIC Free edition allows only one
# MAGIC
# MAGIC I am reusing it

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 2: Write Analytical Queries

# COMMAND ----------

# MAGIC %md
# MAGIC Write these queries inside dashboard in add data section

# COMMAND ----------

# MAGIC %md
# MAGIC 🔹 Revenue Trend (Time Series)
# MAGIC
# MAGIC ✅ SQL Query
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC
# MAGIC 🔹 Orders Trend
# MAGIC
# MAGIC ✅ SQL Query
# MAGIC
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_orders
# MAGIC FROM ecommerce_catalog.gold.daily_sales
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC
# MAGIC 🔹  Average Order Value (AOV)
# MAGIC
# MAGIC ✅ SQL Query
# MAGIC
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   avg_order_value
# MAGIC FROM ecommerce_catalog.gold.daily_sales
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC
# MAGIC 🔹 Funnel Analysis
# MAGIC
# MAGIC (Using Silver table – real analytics)
# MAGIC
# MAGIC ✅ SQL Query
# MAGIC
# MAGIC SELECT
# MAGIC   event_type,
# MAGIC   COUNT(DISTINCT user_id) AS users
# MAGIC FROM ecommerce_catalog.silver.orders_clean
# MAGIC GROUP BY event_type
# MAGIC ORDER BY users DESC;
# MAGIC
# MAGIC
# MAGIC 🔹  Top Products by Revenue
# MAGIC
# MAGIC ✅ SQL Query
# MAGIC
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   SUM(price) AS revenue
# MAGIC FROM ecommerce_catalog.silver.orders_clean
# MAGIC WHERE event_type = 'purchase'
# MAGIC GROUP BY product_id
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 :Build dashboard: revenue trends, funnels, top products

# COMMAND ----------

# MAGIC %md
# MAGIC 🔹Create Dashboard
# MAGIC
# MAGIC Go to Dashboards
# MAGIC
# MAGIC Click Create Dashboard
# MAGIC
# MAGIC Name: Day 9 – Ecommerce Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 : Add filters & schedule refresh

# COMMAND ----------

# MAGIC %md
# MAGIC 🔹  Add Filters
# MAGIC
# MAGIC In dashboard:
# MAGIC
# MAGIC Click Add filter
# MAGIC
# MAGIC Column: order_date
# MAGIC
# MAGIC Filter type: Date range
# MAGIC
# MAGIC Apply to all charts
# MAGIC
# MAGIC ✅ Now dashboard is interactive
# MAGIC
# MAGIC 🔹 Step 4.2: Schedule Refresh
# MAGIC
# MAGIC Dashboard → Schedule
# MAGIC
# MAGIC Frequency: Daily
# MAGIC
# MAGIC Time: Morning (ex: 6 AM)
# MAGIC
# MAGIC Warehouse: Serverless Starter Warehouse
# MAGIC
# MAGIC 📌 Free edition note:
# MAGIC
# MAGIC Scheduling may be limited
# MAGIC
# MAGIC If not available → mention as design decision