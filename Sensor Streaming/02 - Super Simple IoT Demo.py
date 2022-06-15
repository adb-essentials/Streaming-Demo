# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Streaming for Data Engineers
# MAGIC Welcome to the streaming lab for data engineers on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Build a ML model on sensor data so that you can use the model for stream scoring
# MAGIC 3. Stream, transform, and store your data in a reliable and performant Delta Lake
# MAGIC 4. Perform common streaming patterns like joining to other tables, applying a machine learning model, performing aggregations, and evaluating alerts
# MAGIC 5. Create streaming dashboards using tools like Power BI
# MAGIC 
# MAGIC ## The Use Case
# MAGIC We will ingest and transform IoT device data coming from several locations in the US.  The IoT data is somewhat generic, so it could represent devices on the factory floor or even call center detail records.  

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow

schema = spark.read.format("json").load("/mnt/streamingdemo/IoT_Ingest/file2013-11-01 00:00:00.json").schema

# COMMAND ----------

# MAGIC %md
# MAGIC View and navigate the contents of our container using Databricks `%fs` file system commands.

# COMMAND ----------

# MAGIC %fs ls "/mnt/streamingdemo/IoT_Ingest"

# COMMAND ----------

# DBTITLE 1,Load Reference Data
# MAGIC %run "/Streaming-Demo/Setup Notebooks/00 - Download and Load Reference Data"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###High Volume IoT Data Processing, ML, Analytics, and Monitoring
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/overall2.png" alt="Data Flow" width="725">
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
rawDf = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option('cloudFiles.maxFilesPerTrigger', 1) # Throttle for Demo
    .schema(schema)
    .load('/mnt/streamingdemo/IoT_Ingest/')
    .withColumn('datetime', current_timestamp()) # Added for Demo Purposes Only
)

rawDf.createOrReplaceTempView('rawStream')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query one of the devices from the stream
# MAGIC SELECT * FROM rawStream
# MAGIC WHERE device = 87235

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")

# Write the device data to the Bronze Delta Lake table
(
  rawDf.writeStream
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
# MAGIC All before being written to a Silver Delta Lake table.  Also notice how the Delta Lake table can be both a source and sink for this pipeline.  Delta supports both batch and streaming as both a source and a sink.  

# COMMAND ----------

#Read Streaming Records from Bronze
bronzeDf = spark.readStream.table('streamingdemo.sensorStreamBronze') 

#Read Lookup Tables
infoDf = spark.table('streamingdemo.sensorInfoBronze')
dateDf = spark.table('streamingdemo.dimDate')

#Join the data
combinedDf = bronzeDf.join(infoDf, 'device').join(dateDf, to_date(bronzeDf.datetime) == dateDf.CalendarDate)

#Define ML Model
model_uri = "models:/SensorModel/1"
sensor_predictor = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double')

#Select/Transform appropriate columns
combinedDf = (
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

combinedDf.createOrReplaceTempView('silverStream')

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")

(
  combinedDf.writeStream
    .format('delta')
    .queryName('sensorStreamSilver')
    .trigger(processingTime='3 seconds')
#   .trigger(once=True)
    .option("checkpointLocation", "/mnt/streamingdemo/cp/sensorStreamSilver")
    .option('path', '/mnt/streamingdemo/data/sensorStreamSilver')
    .toTable("streamingdemo.sensorStreamSilver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM streamingdemo.sensorStreamSilver 
# MAGIC WHERE device = 87235

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading a Gold Delta Lake table with Aggregations
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/gold.png" alt="Data Flow" width="400">
# MAGIC </br></br>
# MAGIC Cleansed and scored data is pulled out of the Silter Delta table and then:
# MAGIC 
# MAGIC 1. we add a group by on a window based upon the message timestamp and the deviceName
# MAGIC 2. we add a watermark for considerations of late arriving data and stateful memory management
# MAGIC 3. we take the average of the sensor values over the period
# MAGIC 4. we save the data to the Delta Lake table with the output mode being complete
# MAGIC 
# MAGIC Finally, we write to a Gold Delta Lake table.  

# COMMAND ----------

# Dale, should we have a reference table and do some alerts/threshold detection
# and/or aggregateshttps://adb-6207046991473636.16.azuredatabricks.net/?o=6207046991473636#

from pyspark.sql.functions import avg, window
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool3")

silverDf = spark.readStream.table("streamingdemo.sensorStreamSilver")

silverDf = (
        silverDf
        .withWatermark("datetime", "20 seconds")
        .groupBy(
            window("datetime", "15 seconds"),
            "deviceName"
        )
       .agg(
           avg("sensor1").alias("sensor1_avg"), 
           avg("sensor2").alias("sensor2_avg"), 
           avg("sensor3").alias("sensor3_avg"), 
           avg("sensor4").alias("sensor4_avg"), 
           avg("sensor5").alias("sensor5_avg")
       )
)
silverDf.createOrReplaceTempView("goldStream")

# COMMAND ----------

(
  silverDf.writeStream
    .format('delta')
    .queryName('sensorStreamGold')
    .trigger(processingTime='3 seconds')
#   .trigger(once=True)
    .outputMode("complete")
    .option("checkpointLocation", "/mnt/streamingdemo/cp/sensorStreamGoldAggregates")
    .option('path', '/mnt/streamingdemo/data/sensorStreamGoldAggregates')
    .toTable("streamingdemo.sensorStreamGoldAggregates")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from streamingdemo.sensorStreamGoldAggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading a Gold Delta Lake table with Threshold Detection
# MAGIC </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/gold.png" alt="Data Flow" width="400">
# MAGIC </br></br>
# MAGIC Sensor aggregations are pulled from the Gold Delta table and then:
# MAGIC 
# MAGIC 1. joined to reference data that stores out of tolerance thresholds for the IoT sensor values
# MAGIC 2. device sensor aggregates are checked against defined thresholds, i.e. the average sensor value is too high and represents something breaking
# MAGIC 3. device sensor aggregates that fail the check are sent to a Gold Delta Lake table as an Alert
# MAGIC 
# MAGIC Alerts could also be written to other Azure services and integrated into Apps that send emails/text messages.  

# COMMAND ----------


