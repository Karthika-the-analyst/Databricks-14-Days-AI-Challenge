# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠️ Day 12 Tasks:
# MAGIC
# MAGIC 1. Train simple regression model
# MAGIC 2. Log parameters, metrics, model
# MAGIC 3. View in MLflow UI
# MAGIC 4. Compare runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Train a Simple Regression Model

# COMMAND ----------

# MAGIC %md
# MAGIC Load Feature Data 

# COMMAND ----------

df = spark.table("ecommerce_catalog.gold.daily_sales_features")
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Prepare Pandas Data (Simple ML)

# COMMAND ----------

pdf = df.select(
    "total_orders",
    "is_weekend",
    "total_revenue"
).dropna().toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC Train Linear Regression Model

# COMMAND ----------

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

X = pdf[["total_orders", "is_weekend"]]
y = pdf["total_revenue"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = LinearRegression()
model.fit(X_train, y_train)

y_pred = model.predict(X_test)


# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate Model

# COMMAND ----------

# DBTITLE 1,Evaluate Model
import numpy as np
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)

rmse, r2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Log Parameters, Metrics & Model (MLflow)

# COMMAND ----------



# COMMAND ----------

# --------------------------------------------------
# STEP 1: Import required MLflow libraries
# --------------------------------------------------
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature


# --------------------------------------------------
# STEP 2: Set (or create) an MLflow experiment
# All runs will be tracked under this experiment
# --------------------------------------------------
mlflow.set_experiment("/Day12_Ecommerce_Revenue_Prediction")


# --------------------------------------------------
# STEP 3: Create an input example
# This helps MLflow understand expected model inputs
# We take one row from training data
# --------------------------------------------------
input_example = X_train.iloc[:1]


# --------------------------------------------------
# STEP 4: Infer model signature
# Signature captures:
# - input schema (feature names & types)
# - output schema (prediction type)
# This is critical for deployment & validation
# --------------------------------------------------
signature = infer_signature(
    X_train,
    model.predict(X_train)
)


# --------------------------------------------------
# STEP 5: Start an MLflow run
# Everything inside this block belongs to one run
# --------------------------------------------------
with mlflow.start_run():

    # ----------------------------------------------
    # STEP 6: Log model parameters
    # Parameters describe how the model was built
    # ----------------------------------------------
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("features_used", "total_orders, is_weekend")

    # ----------------------------------------------
    # STEP 7: Log evaluation metrics
    # Metrics describe model performance
    # ----------------------------------------------
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2_score", r2)

    # ----------------------------------------------
    # STEP 8: Log the trained model
    # - Stores model artifact
    # - Attaches signature
    # - Attaches input example
    # ----------------------------------------------
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        input_example=input_example,
        signature=signature
    )

# --------------------------------------------------
# STEP 9: Run automatically ends here
# MLflow assigns a Run ID and saves everything
# --------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: View in MLflow UI

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 Commands / Steps
# MAGIC 1. Open the Databricks notebook used for Day 12
# MAGIC 2. Click the Experiments icon (🧪 flask) on the right sidebar
# MAGIC 3. Select experiment: Day12_Ecommerce_Revenue_Prediction
# MAGIC 4. View the list of experiment runs
# MAGIC 5. Click on a Run ID
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 🔍 What you will 
# MAGIC
# MAGIC
# MAGIC - Run ID
# MAGIC - Start / End time
# MAGIC - Parameters (model_type, features_used)
# MAGIC - Metrics (rmse, r2_score)
# MAGIC - Model artifacts (sklearn model, signature, input example)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 : Compare Runs in MLflow UI

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 Commands / Steps
# MAGIC 1. Open MLflow Experiments UI
# MAGIC 2. Select experiment: Day12_Ecommerce_Revenue_Prediction
# MAGIC 3. Select multiple runs using checkboxes
# MAGIC 4. Click Compare
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 📊 Comparison View Shows
# MAGIC - Side-by-side parameters
# MAGIC - RMSE comparison
# MAGIC - R2 score comparison
# MAGIC - Feature differences
# MAGIC - Parallel coordinates plot

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Decision Rule
# MAGIC - Lower RMSE = better model
# MAGIC - Higher R2 score = better model
# MAGIC
# MAGIC 🧠 Final Conclusion 
# MAGIC
# MAGIC Both models achieved similar performance.
# MAGIC Total_orders alone explains most revenue variance.
# MAGIC Additional features improve robustness but not accuracy for this dataset.