# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

import mlflow

model_uri = "models:/SensorModel/2"
sensor_predictor = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double', env_manager="conda")

#sensor_predictor = mlflow.pyfunc.spark_udf(spark, "runs:/dd9a3819646f4bb998e2e4f7d802b5c3/model",result_type = 'double')
spark.udf.register('sensorPredictor', sensor_predictor) 
# df = df.withColumn('sensor5Prediction', sensor_predictor('sensor1', 'sensor2', 'sensor3', 'sensor4'))

# df.createOrReplaceTempView('rawStreamWPrediction')

# COMMAND ----------

# import dlt

# schema = spark.read.format('json').load('/tmp/mcdata/file2013-11-01 00:00:00.json').schema

# @dlt.view
# def dltSensorStreamRaw():
#   return (
#     spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option('cloudFiles.maxFilesPerTrigger', 1) # Throttle for Demo... only relevant for stream
#     .schema(schema)
#     .load('/tmp/mcdata')
#   )
