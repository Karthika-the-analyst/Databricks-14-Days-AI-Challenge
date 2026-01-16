# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 1 Tasks:
# MAGIC
# MAGIC 1. Create Databricks Community Edition account
# MAGIC 2. Navigate Workspace, Compute, Data Explorer
# MAGIC 3. Create first notebook
# MAGIC 4. Run basic PySpark commands

# COMMAND ----------

# Create simple DataFrame
data = [("iPhone", 999), ("Samsung", 799), ("MacBook", 1299)]
df = spark.createDataFrame(data, ["product", "price"])
df.show()




# COMMAND ----------

# Filter expensive products
df.filter(df.price > 1000).show()