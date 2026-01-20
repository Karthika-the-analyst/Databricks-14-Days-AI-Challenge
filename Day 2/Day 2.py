# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 2 Tasks:
# MAGIC
# MAGIC 1. Upload sample e-commerce CSV
# MAGIC 2. Read data into DataFrame
# MAGIC 3. Perform basic operations: select, filter, groupBy, orderBy
# MAGIC 4. Export results

# COMMAND ----------

Oct_events = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv" , header=True , inferSchema=True )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Operation Using Python

# COMMAND ----------

# MAGIC %md
# MAGIC Select (Choose columns)

# COMMAND ----------

Oct_events.select(
    "event_time", "event_type", "category_code", "price"
).show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC Filter (Apply conditions)

# COMMAND ----------

Oct_events.filter(
    Oct_events.price > 100
).show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy (Aggregation)

# COMMAND ----------

Oct_events.groupBy("event_type").count().show()


# COMMAND ----------

# MAGIC %md
# MAGIC OrderBy (Sorting)

# COMMAND ----------

Oct_events.groupBy("event_type") \
          .count() \
          .orderBy("count", ascending=False) \
          .show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Operation Using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a TEMP VIEW
# MAGIC
# MAGIC You must register the DataFrame as a SQL view.

# COMMAND ----------

Oct_events.createOrReplaceTempView("oct_events")


# COMMAND ----------

# MAGIC %md
# MAGIC Select (Choose columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, event_type, category_code, price
# MAGIC FROM oct_events
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Filter (Apply conditions)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM oct_events
# MAGIC WHERE price > 100
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy (Aggregation)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS total_events
# MAGIC FROM oct_events
# MAGIC GROUP BY event_type;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC OrderBy (Sorting)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS total_events
# MAGIC FROM oct_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY total_events DESC;
# MAGIC