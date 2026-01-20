# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 3 Tasks:
# MAGIC
# MAGIC 1. Load full e-commerce dataset
# MAGIC 2. Perform complex joins
# MAGIC 3. Calculate running totals with window functions
# MAGIC 4. Create derived features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 :  Load full e-commerce dataset

# COMMAND ----------

Oct_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)

Nov_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

# MAGIC %md
# MAGIC Combining (Union) both months

# COMMAND ----------

full_events = Oct_events.unionByName(Nov_events)


# COMMAND ----------

# MAGIC %md
# MAGIC Validating

# COMMAND ----------

full_events.count()



# COMMAND ----------

full_events.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Perform Complex Joins

# COMMAND ----------

from pyspark.sql.functions import sum, count

# Create user-level summary
user_summary = full_events.groupBy("user_id").agg(
    count("*").alias("total_events"),
    sum("price").alias("total_spent")
)

# Join with main dataset
joined_df = full_events.join(
    user_summary,
    on="user_id",
    how="left"
)


# COMMAND ----------

# MAGIC %md
# MAGIC Validation

# COMMAND ----------

joined_df.select("user_id", "price", "total_spent").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Running Totals with Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum

# Define window specification
window_spec = Window.partitionBy("user_id") \
    .orderBy("event_time") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate running total
running_total_df = joined_df.withColumn(
    "running_total_spent",
    sum("price").over(window_spec)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Validation

# COMMAND ----------

running_total_df.select(
    "user_id", "event_time", "price", "running_total_spent"
).show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Create Derived Features

# COMMAND ----------

# MAGIC %md
# MAGIC High-value Transaction Flag

# COMMAND ----------

from pyspark.sql.functions import when

feature_df = running_total_df.withColumn(
    "high_value_transaction",
    when(running_total_df.price > 100, 1).otherwise(0)
)


# COMMAND ----------

# MAGIC %md
# MAGIC User Spending Category

# COMMAND ----------

feature_df = feature_df.withColumn(
    "user_spending_category",
    when(feature_df.total_spent > 1000, "High")
    .when(feature_df.total_spent > 500, "Medium")
    .otherwise("Low")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Event Date Extraction

# COMMAND ----------

from pyspark.sql.functions import to_date

feature_df = feature_df.withColumn(
    "event_date",
    to_date("event_time")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Final Validation

# COMMAND ----------

feature_df.select(
    "user_id",
    "price",
    "running_total_spent",
    "high_value_transaction",
    "user_spending_category",
    "event_date"
).show(5)
