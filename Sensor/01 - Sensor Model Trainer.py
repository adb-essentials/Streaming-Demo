# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Streaming for Data Engineers
# MAGIC Welcome to the quickstart lab for data engineers on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Transform and store your data in a reliable and performant Delta Lake
# MAGIC 3. Use Update,Delete,Merge,Schema Evolution and Time Travel Capabilities, CDF (Change Data Feed) of Delta Lake
# MAGIC 
# MAGIC ## The Use Case
# MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initial Set-up
# MAGIC 
# MAGIC ### Enter the Blob_Container , Blob_Account and Account_Key for the Cloudlabs Environment

# COMMAND ----------

dbutils.widgets.text("ACCOUNT_KEY", "", "ACCOUNT_KEY")
dbutils.widgets.text("BLOB_CONTAINER", "", "BLOB_CONTAINER")
dbutils.widgets.text("BLOB_ACCOUNT", "", "BLOB_ACCOUNT")

# COMMAND ----------

BLOB_CONTAINER = dbutils.widgets.get("BLOB_CONTAINER")
BLOB_ACCOUNT = dbutils.widgets.get("BLOB_ACCOUNT")
ACCOUNT_KEY = dbutils.widgets.get("ACCOUNT_KEY")

# COMMAND ----------

# DBTITLE 1,Mount Blob Storage to DBFS
run = dbutils.notebook.run("/Streaming-Demo/Setup Notebooks/00 - Setup Storage", 60, {"BLOB_CONTAINER" : BLOB_CONTAINER,"BLOB_ACCOUNT" : BLOB_ACCOUNT,"ACCOUNT_KEY" : ACCOUNT_KEY })

# COMMAND ----------

# DBTITLE 1,Delete Existing Files and Create Database
spark.sql('CREATE DATABASE IF NOT EXISTS streamingdemo')

dbutils.fs.rm("/mnt/streamingdemo/data/sensorStreamBronze_training", True)

schema = spark.read.format("json").load("/mnt/streamingdemo/IoT_Ingest/file2013-11-01 00:00:00.json").schema

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- <img src="https://publicimg.blob.core.windows.net/images/DS.png" width="1200"> -->
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/DatabricksML.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load training table

# COMMAND ----------

# Autoloader Configuration
df = (
  spark.read.format("json")
    .schema(schema)
    .load('/mnt/streamingdemo/IoT_Ingest/')
)

# COMMAND ----------

# Write the device data to the Bronze Delta Lake table
(
  df.write
    .format('delta')
    .mode("overwrite")
    .option('path', '/mnt/streamingdemo/data/sensorStreamBronze_training')
    .saveAsTable('StreamingDemo.sensorStreamBronze_training')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train ML Model

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import mlflow

mlflow.spark.autolog()

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md Import Delta Data

# COMMAND ----------

dataDf = spark.table('StreamingDemo.sensorStreamBronze_training')
display(dataDf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Databricks Auto ML

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/Automl.png" width="1200">

# COMMAND ----------

from databricks import automl

summary = automl.regress(
  dataDf, 
  target_col="sensor5",
  exclude_columns=["datetime", "device"], 
  primary_metric="r2", 
  timeout_minutes=5)

# COMMAND ----------

# MAGIC %md #### Register the model with the MLflow Model Registry
# MAGIC 
# MAGIC Now that a ML model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI (Workflow 1) or the MLflow API (Workflow 2).
# MAGIC 
# MAGIC Follow the instructions for your preferred workflow (UI or API) to register your forecasting model, add rich model descriptions, and perform stage transitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Register model via the API

# COMMAND ----------

#get the best trial from our AutoML run programmatically
print(summary.best_trial)

# COMMAND ----------

# Register the best model run
model_name = "SensorModel"

model_uri = f"runs:/{summary.best_trial.mlflow_run_id}/model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DBTITLE 1,Add Model Description
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_registered_model(
  name=registered_model_version.name,
  description="This model predicts sensor5 using the first 4 sensor values."
)

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC 
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages. Your administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC 
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly by using the `MlflowClient.update_model_version()` function. If you do not have permission, you can request a stage transition using the REST API; for example:
# MAGIC 
# MAGIC ```
# MAGIC %sh curl -i -X POST -H "X-Databricks-Org-Id: <YOUR_ORG_ID>" -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" https://<YOUR_DATABRICKS_WORKSPACE_URL>/api/2.0/preview/mlflow/transition-requests/create -d '{"comment": "Please move this model into production!", "model_version": {"version": 1, "registered_model": {"name": "power-forecasting-model"}}, "stage": "Production"}'

# COMMAND ----------

# MAGIC %md Now that you've learned about stage transitions, transition the model to the `Production` stage.

# COMMAND ----------

# MAGIC %python
# MAGIC client.transition_model_version_stage(
# MAGIC   name=model_name,
# MAGIC   version=registered_model_version.version,
# MAGIC   stage='Production',
# MAGIC )

# COMMAND ----------

# MAGIC %md ### Model Serving
# MAGIC Now that the model is in Production we are ready for our next step - Model Serving
# MAGIC For this workshop we will serve the model in the streaming pipeline

# COMMAND ----------


