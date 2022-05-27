# Databricks notebook source
# MAGIC %md Setup

# COMMAND ----------

from pyspark.sql.functions import *
import mlflow
spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', False)
schema = spark.read.format('json').load('/tmp/mcdata/file2013-11-01 00:00:00.json').schema
spark.sql('truncate table mcdata.sensorStreamBronze')
spark.sql('truncate table mcdata.sensorStreamSilver')
spark.sql('VACUUM mcdata.sensorStreamBronze RETAIN 0 Hours')
spark.sql('VACUUM mcdata.sensorStreamSilver RETAIN 0 Hours')
#spark.sql('drop table mcdata.sensorStreamBronze')
#spark.sql('drop table mcdata.sensorStreamSilver')
dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamBronze", True)
dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamSilver", True)
# dbutils.fs.rm("dbfs:/tmp/mcdata", True)
#dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamBronze", True)
#dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamSilver", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Basic Stream (100K Devices x 5 Sensors)

# COMMAND ----------

df = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option('cloudFiles.maxFilesPerTrigger', 1) # Throttle for Demo
    .schema(schema)
    .load('/tmp/mcdata')
    .withColumn('datetime', current_timestamp()) # Added for Demo
)

df.createOrReplaceTempView('rawStream')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rawStream
# MAGIC WHERE device = 87235

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")

(
  df.writeStream
    .queryName('sensorStreamBronze')
    .trigger(processingTime='3 seconds')
 #   .trigger(once=True)
    .option("checkpointLocation", "abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamBronze")
    .toTable('mcdata.sensorStreamBronze')
#     .format('delta')
#     .start('abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamBronze')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Build a Silver table with Some Joins and a Prediction

# COMMAND ----------

#Read Streaming Records from Bronze
bronzeDf = spark.readStream.table('mcdata.sensorStreamBronze') #format('delta').load('abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamBronze')

#Read Lookup Tables
infoDf = spark.table('mcdata.sensorInfoBronze')
dateDf = spark.table('mcdata.dimDate')

#Join the data
combinedDf = bronzeDf.join(infoDf, 'device').join(dateDf, to_date(bronzeDf.datetime) == dateDf.CalendarDate)

#Define ML Model
model_uri = "models:/mcSensorModel/production"
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
# MAGIC WHERE device = 200

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")

(
  readyDf.writeStream
    .queryName('sensorStreamSilver')
     .trigger(processingTime='3 seconds')
#    .trigger(once=True)
    .option("checkpointLocation", "abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamSilver")
     .toTable("mcdata.sensorStreamSilver")
#     .format('delta')
#     .start('abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamSilver')
)
