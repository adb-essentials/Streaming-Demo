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

# MAGIC %md
# MAGIC Once mounted, we can view and navigate the contents of our container using Databricks `%fs` file system commands.

# COMMAND ----------

# MAGIC %fs ls "/mnt/streamingdemo/"

# COMMAND ----------

# DBTITLE 1,Delete Existing Files and Create Database
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow

schema = spark.read.format("json").load("/mnt/streamingdemo/IoT_Ingest/file2013-11-01 00:00:00.json").schema

spark.sql('DROP DATABASE IF EXISTS streamingdemo CASCADE')

dbutils.fs.rm("/mnt/streamingdemo/data/", recurse=True)
dbutils.fs.rm("/mnt/streamingdemo/cp/", recurse=True)
dbutils.fs.rm("/mnt/streamingdemo/temp/", recurse=True)

spark.sql('CREATE DATABASE IF NOT EXISTS streamingdemo')

# COMMAND ----------

# DBTITLE 1,Load Reference Data
# MAGIC %run "/Streaming-Demo/Setup Notebooks/00 - Download and Load Reference Data"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###High Volume IoT Data Processing, ML, Analytics, and Monitoring
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/overall.jpg" alt="Data Flow" width="725">
# MAGIC </br></br>
# MAGIC This demo will walk through an end-to-end data flow ingesting device data in streaming 100K-row batches </br>
# MAGIC and then joining, transforming, predicting, and ultimately serving that data up through Databricks SQL and a </br>
# MAGIC Power BI monitoring dashboard.  The end-to-end data flow will have insights flowing into Power BI within </br>
# MAGIC ~10 seconds of cloud ingestion.

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Device Ingest to Delta Lake using Databricks Autoloader
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/bronze.jpg" alt="Data Flow" width="400">
# MAGIC </br></br>
# MAGIC Simple incremental ingestion from Azure Data Lake or IoT/Event Hubs at ~100K/second.

# COMMAND ----------

# Autoloader Configuration
df = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option('cloudFiles.maxFilesPerTrigger', 1) # Throttle for Demo
    .schema(schema)
    .load('/mnt/streamingdemo/IoT_Ingest/')
    .withColumn('datetime', current_timestamp()) # Added for Demo Purposes Only
)

df.createOrReplaceTempView('rawStream')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query one of the devices from the stream
# MAGIC SELECT * FROM rawStream
# MAGIC WHERE device = 87235

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")

# Write the device data to the Bronze Delta Lake table
(
  df.writeStream
    .format('delta')
    .queryName('sensorStreamBronze')
    .trigger(processingTime='3 seconds')
 #   .trigger(once=True)
    .option("checkpointLocation", "/mnt/streamingdemo/cp/sensorStreamBronze")
    .option('path', '/mnt/streamingdemo/data/sensorStreamBronze')
    .toTable('StreamingDemo.sensorStreamBronze')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading a Silver Delta Lake table with Incremental Lookups, Joins, and ML Inference
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/silver.jpg" alt="Data Flow" width="400">
# MAGIC </br></br>
# MAGIC Incremental device data is pulled out of the Bronze Delta table and then:
# MAGIC 
# MAGIC 1. it's <b>joined to 2 additional tables</b> to pull reference data
# MAGIC 2. it has a <b>few basic ETL transformations</b> applied
# MAGIC 3. an <b>MLflow model is applied</b> to predict what the Sensor5 value should be
# MAGIC 
# MAGIC All before being written to a Silver Delta Lake table.

# COMMAND ----------

#Read Streaming Records from Bronze
bronzeDf = spark.readStream.table('streamingdemo.sensorStreamBronze') 

#Read Lookup Tables
infoDf = spark.table('streamingdemo.sensorInfoBronze')
dateDf = spark.table('streamingdemo.dimDate')

#Join the data
combinedDf = bronzeDf.join(infoDf, 'device').join(dateDf, to_date(bronzeDf.datetime) == dateDf.CalendarDate)

#Define ML Model
model_uri = "models:/SensorModel/2"
sensor_predictor = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double')

#Select/Transform appropriate columns
readyDf = (
  combinedDf.select(
     col('CalendarDate')
    ,col('CalendarDay')
    ,hour(col('datetime')).alias('hourOfDay')
    ,col('datetime')
    ,col('device')
    ,col('deviceName')
    ,col('location')
    ,col('sensor1')
    ,col('sensor2')
    ,col('sensor3')
    ,col('sensor4')
    ,col('sensor5')
    ,sensor_predictor('sensor1', 'sensor2', 'sensor3', 'sensor4').alias('sensor5Prediction')
    ,(col('sensor2') - col('sensor1')).alias('sensor2Variance')
    ,(col('sensor4') - col('sensor3')).alias('sensor4Variance')
  )
)

readyDf.createOrReplaceTempView('readyView')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT datetime, sensor5, sensor5Prediction FROM readyView
# MAGIC WHERE device = 87235

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")

(
  readyDf.writeStream
    .format('delta')
    .queryName('sensorStreamSilver')
    .trigger(processingTime='3 seconds')
#   .trigger(once=True)
    .option("checkpointLocation", "/mnt/streamingdemo/cp/sensorStreamSilver")
    .option('path', '/mnt/streamingdemo/data/sensorStreamSilver')
    .toTable("streamingdemo.sensorStreamSilver")
)

# COMMAND ----------

# Dale, should we have a reference table and do some alerts/threshold detection
# and/or aggregates
