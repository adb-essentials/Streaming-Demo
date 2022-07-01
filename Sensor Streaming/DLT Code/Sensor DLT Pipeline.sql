-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ###High Volume IoT Data Processing, ML, Analytics, and Monitoring
-- MAGIC </br>
-- MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/overall.jpg" alt="Data Flow" width="725">
-- MAGIC </br></br>
-- MAGIC This demo will walk through an end-to-end data flow ingesting device data in streaming 100K-row batches </br>
-- MAGIC and then joining, transforming, predicting, and ultimately serving that data up to SQL analytics and a </br>
-- MAGIC Power BI monitoring dashboard.  The end-to-end data flow will have insights flowing into Power BI within </br>
-- MAGIC ~10 seconds of cloud ingestion.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Device Ingest to Delta Lake using Databricks Autoloader
-- MAGIC </br>
-- MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/bronze.jpg" alt="Data Flow" width="400">
-- MAGIC </br></br>
-- MAGIC Simple incremental ingestion from Azure Data Lake or IoT/Event Hubs at ~100K/second.

-- COMMAND ----------

-- MAGIC %md Load Bronze from AutoLoader

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW dltSensorStreamRaw AS
SELECT * FROM cloud_files("/mnt/streamingdemo/IoT_Ingest/", "json", map("cloudFiles.maxFilesPerTrigger", "1"))

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE dltsensorStreamBronze
COMMENT "This is the Bronze/Raw table for the sales orders"
AS
SELECT * FROM STREAM(LIVE.dltSensorStreamRaw);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Loading a Silver Delta Lake table with Incremental Lookups, Joins, and ML Inference
-- MAGIC </br>
-- MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/silver.jpg" alt="Data Flow" width="400">
-- MAGIC </br></br>
-- MAGIC Incremental device data is pulled out of the Bronze Delta table and then:
-- MAGIC 
-- MAGIC 1. it's <b>joined to 2 additional tables</b> to pull reference data
-- MAGIC 2. it has a <b>few basic ETL transformations</b> applied
-- MAGIC 3. an <b>MLflow model is applied</b> to predict what the Sensor5 value should be
-- MAGIC 
-- MAGIC All before being written to a Silver Delta Lake table.

-- COMMAND ----------

-- MAGIC %md Create Reference to Lookup Tables

-- COMMAND ----------

CREATE LIVE VIEW dltVwSensorInfo
AS
SELECT * FROM streamingdemo.sensorInfoBronze;

CREATE LIVE VIEW dltVwDimDate
AS
SELECT * FROM streamingdemo.dimDate;

-- COMMAND ----------

-- MAGIC %md Load Silver from Bronze and Lookup Tables

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE dltsensorStreamSilver 
(
  CONSTRAINT device17X EXPECT (device NOT BETWEEN 1700 AND 1799) ON VIOLATION DROP ROW,
  CONSTRAINT miamiClosed EXPECT (location != "Miami, FL"),
  CONSTRAINT sensor5Max EXPECT (sensor5 <= 2500)
)
COMMENT "This is the Silver table which includes lookups to other tables and transformations"
AS
SELECT
  dd.CalendarDate
  ,dd.CalendarDay
  ,HOUR(ss.datetime) hourOfDay
  ,ss.datetime
  ,si.device
  ,si.deviceName
  ,si.location
  ,ss.sensor1
  ,ss.sensor2
  ,ss.sensor3
  ,ss.sensor4
  ,ss.sensor5
  ,sensorPredictor(sensor1, sensor2, sensor3, sensor4) sensor5Prediction
  ,sensor2 - sensor1 sensor2Variance
  ,sensor4 - sensor3 sensor4Variance
FROM STREAM(LIVE.dltsensorStreamBronze) ss
  INNER JOIN LIVE.dltVwSensorInfo si ON ss.device = si.device
  INNER JOIN LIVE.dltVwDimDate dd ON to_date(ss.datetime) = dd.CalendarDate


-- COMMAND ----------


