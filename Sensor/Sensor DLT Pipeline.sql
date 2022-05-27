-- Databricks notebook source
-- MAGIC %md Load Bronze from AutoLoader

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW dltSensorStreamRaw AS
SELECT * FROM cloud_files("dbfs:/tmp/mcdata", "json", map("cloudFiles.maxFilesPerTrigger", "1"))

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE dltsensorStreamBronze
COMMENT "This is the Bronze/Raw table for the sales orders"
AS
SELECT * FROM STREAM(LIVE.dltSensorStreamRaw);

-- COMMAND ----------

-- MAGIC %md Create Reference to Lookup Tables

-- COMMAND ----------

CREATE LIVE VIEW dltVwSensorInfo
AS
SELECT * FROM mcdata.sensorInfoBronze;

CREATE LIVE VIEW dltVwDimDate
AS
SELECT * FROM mcdata.dimDate;

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

-- CREATE INCREMENTAL LIVE TABLE dltsensorStreamSilver_BadDataQuarantine 
-- (
--     CONSTRAINT sensor5OverMax EXPECT (NOT(sensor5 <= 3500)) ON VIOLATION DROP ROW
-- )
-- COMMENT "This is the Silver table which contains bad data records..."
-- AS
-- SELECT
--   dd.CalendarDate
--   ,dd.CalendarDay
--   ,HOUR(ss.datetime) hourOfDay
--   ,ss.datetime
--   ,si.device
--   ,si.deviceName
--   ,si.location
--   ,ss.sensor1
--   ,ss.sensor2
--   ,ss.sensor3
--   ,ss.sensor4
--   ,ss.sensor5
--   ,sensorPredictor(sensor1, sensor2, sensor3, sensor4) sensor5Prediction
--   ,sensor2 - sensor1 sensor2Variance
--   ,sensor4 - sensor3 sensor4Variance
-- FROM STREAM(LIVE.dltsensorStreamBronze) ss
--   INNER JOIN LIVE.dltVwSensorInfo si ON ss.device = si.device
--   INNER JOIN LIVE.dltVwDimDate dd ON to_date(ss.datetime) = dd.CalendarDate


-- COMMAND ----------


