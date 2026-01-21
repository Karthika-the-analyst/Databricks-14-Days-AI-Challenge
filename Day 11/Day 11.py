# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️  Day 11 Tasks:
# MAGIC
# MAGIC 1. Calculate statistical summaries
# MAGIC 2. Test hypotheses (weekday vs weekend)
# MAGIC 3. Identify correlations
# MAGIC 4. Engineer features for ML

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Calculate Statistical Summaries

# COMMAND ----------

# MAGIC %md
# MAGIC Basic Statistics (Revenue)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                      AS days_count,
# MAGIC   MIN(total_revenue)            AS min_revenue,
# MAGIC   MAX(total_revenue)            AS max_revenue,
# MAGIC   AVG(total_revenue)            AS avg_revenue,
# MAGIC   STDDEV(total_revenue)         AS stddev_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution Check (Orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(total_orders)  AS min_orders,
# MAGIC   MAX(total_orders)  AS max_orders,
# MAGIC   AVG(total_orders)  AS avg_orders
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Hypothesis Testing (Weekday vs Weekend)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Weekday / Weekend Label

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE 
# MAGIC     WHEN dayofweek(order_date) IN (1, 7) THEN 'Weekend'
# MAGIC     ELSE 'Weekday'
# MAGIC   END AS day_type,
# MAGIC   AVG(total_revenue) AS avg_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned
# MAGIC GROUP BY day_type;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Average revenue on weekends > average revenue on weekdays
# MAGIC
# MAGIC This confirms the hypothesis that customers spend more on weekends.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Identify Correlations

# COMMAND ----------

# MAGIC %md
# MAGIC Correlation Calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CORR(total_orders, total_revenue) AS orders_revenue_corr
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The correlation between total orders and total revenue is extremely high (~0.998), indicating that order volume is the primary driver of revenue. This validates that increasing customer conversions directly impacts revenue growth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Feature Engineering for ML

# COMMAND ----------

# MAGIC %md
# MAGIC Create Feature Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce_catalog.gold.daily_sales_features AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   dayofweek(order_date)                AS day_of_week,
# MAGIC   CASE 
# MAGIC     WHEN dayofweek(order_date) IN (1,7) THEN 1 ELSE 0 
# MAGIC   END                                  AS is_weekend,
# MAGIC   total_orders,
# MAGIC   total_revenue,
# MAGIC   avg_order_value,
# MAGIC   LAG(total_revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue
# MAGIC FROM ecommerce_catalog.gold.daily_sales_partitioned;
# MAGIC