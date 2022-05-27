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
#spark.sql('drop table mcdata.sensorStreamBronze')
#spark.sql('drop table mcdata.sensorStreamSilver')
dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamBronze", True)
dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamSilver", True)
# dbutils.fs.rm("dbfs:/tmp/mcdata", True)
# dbutils.fs.rm("abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamBronze", True)
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

# MAGIC %md
# MAGIC Add a Prediction for Sensor 5 Value

# COMMAND ----------

#Get model from model registry and make it a function
model_uri = "models:/mcSensorModel/production"
sensor_predictor = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double')

df = df.withColumn('sensor5Prediction', sensor_predictor('sensor1', 'sensor2', 'sensor3', 'sensor4'))

df.createOrReplaceTempView('rawStreamWPrediction')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT datetime, sensor5, sensor5Prediction FROM rawStreamWPrediction
# MAGIC WHERE device = 200

# COMMAND ----------

# MAGIC %md Write Stream to Bronze Delta Table

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

# MAGIC %sql
# MAGIC SELECT * FROM mcdata.sensorStreamBronze
# MAGIC WHERE device BETWEEN  1800 AND 1850

# COMMAND ----------

# MAGIC %md Window/Aggregate

# COMMAND ----------

dfAgg = (
        spark.readStream.table('mcdata.sensorStreamBronze')
          .groupBy(window(col('datetime'), "30 seconds", "15 seconds").alias('timeBlock'),'device')
          .agg(
              avg(col('sensor1')).alias('sensor1'),
              avg(col('sensor2')).alias('sensor2'),
              avg(col('sensor3')).alias('sensor3'),
              avg(col('sensor4')).alias('sensor4'),
              avg(col('sensor5')).alias('sensor5')
          )
)

dfAgg.createOrReplaceTempView('aggStream')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aggStream
# MAGIC WHERE device = 200

# COMMAND ----------

# MAGIC %md
# MAGIC Write Agg Stream to Silver Delta Table

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")

(
  dfAgg.writeStream
    .queryName('sensorStreamSilver')
     .trigger(processingTime='3 seconds')
#    .trigger(once=True)
    .outputMode('complete')
    .option("checkpointLocation", "abfss://data@mcg2stanstor00.dfs.core.windows.net/cp/sensorStreamSilver")
     .toTable("mcdata.sensorStreamSilver")
#     .format('delta')
#     .start('abfss://data@mcg2stanstor00.dfs.core.windows.net/curatedDatasets/sensorStreamSilver')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mcdata.sensorStreamSilver
# MAGIC WHERE device = 200
