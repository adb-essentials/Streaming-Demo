# Databricks notebook source
# MAGIC %md
# MAGIC # Summary
# MAGIC This notebook streams a sample set of JSON data to an Azure Event Hub:
# MAGIC 
# MAGIC  - Mounts an Azure Data Lake Storage Gen2 location where sample JSON data is stored
# MAGIC  - Sets up Event Hub configuration
# MAGIC  - Reads each sample file into a dataframe as a stream
# MAGIC  - Converts the dataframe into JSON format
# MAGIC  - Sends the JSON objects to an Event Hub as a stream

# COMMAND ----------

# MAGIC %md
# MAGIC # Prerequisites
# MAGIC 
# MAGIC - [Install library on cluster](https://docs.microsoft.com/en-us/azure/databricks/libraries/) - Maven coordinates: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 - **REMOVE IF GOING WITH KAFKA CONNECTOR**
# MAGIC - Set up Azure Key Vault-backed secret scope [(Secret scopes - Azure Databricks | Microsoft Docs)](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope)
# MAGIC - Azure Data Lake Storage Gen2 or Azure Blob Storage account with sample JSON data
# MAGIC - (Optional) An Azure Service Principal if using to authenticate to ADLS account - [Access Azure Data Lake Storage Gen2 using OAuth 2.0 with an Azure service principal](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access)

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional References
# MAGIC - [Structured Streaming tutorial - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/en-us/azure/databricks/getting-started/spark/streaming#load-streaming-sample-data)
# MAGIC - [Structured Streaming + Event Hubs Integration Guide for PySpark | Azure/azure-event-hubs-spark (github.com)](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Key Vault and Storage Account details and Connect to Storage Account

# COMMAND ----------

import datetime

## Key Vault scope and secrets for Storage, Event Hubs
# keyVaultScope = "<KEY VAULT-BACKED SECRET SCOPE>"
# keyVaultSecretStorageKey = "<KEY VAULT SECRET FOR STORAGE KEY>"
# keyVaultSecretEvhConnStr ="<KEY VAULT SECRET FOR EVENT HUBS CONN STRING>"
keyVaultScope = "sse062022tfkv"
keyVaultSecretStorageKey = "adls-key"
keyVaultSecretEvhConnStr = "evh-connstr"

## Storage location details
# storageAccountName = "<STORAGE ACCOUNT NAME>"
# container = "<CONTAINER NAME>"
# dataDirectory = "<PATH TO READ SAMPLE FILES>"
# checkpointDirectory = "<PATH TO WRITE CHECKPOINT FILES"
storageAccountName = "sse062022tfadls"
container = "landing"
dataDirectory = "/IoT_Ingest/"
checkpointDirectory = "/write/checkpoint/"

## Authenticate to ADLS
spark.conf.set(
    f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net",
    dbutils.secrets.get(scope=keyVaultScope, key=keyVaultSecretStorageKey))

## Event Hub configuration
evhConnectionString = dbutils.secrets.get(scope=keyVaultScope,key=keyVaultSecretEvhConnStr)
operationTimeoutDuration = datetime.time(0,59,0).strftime("PT%HH%MM%SS") 

ehConf = {
    "eventhubs.connectionString" : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(evhConnectionString),
    "eventhubs.operationTimeout" : operationTimeoutDuration
}

# Kafka configuration
kafkaBootstrapServers = "sse062022tfevhns.servicebus.windows.net:9093"
kafkaTopic = "iot"
kafkaSaslJaasConfig = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{evhConnectionString}\";"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = f"abfss://{container}@{storageAccountName}.dfs.core.windows.net{dataDirectory}"
checkpointPath = f"abfss://{container}@{storageAccountName}.dfs.core.windows.net{checkpointDirectory}"

# Define the schema to speed up processing
jsonSchema = StructType([
    StructField("device", IntegerType()),
    StructField("datetime", TimestampType()),
    StructField("sensor1", DecimalType()),
    StructField("sensor2", DecimalType()),
    StructField("sensor3", DecimalType()),
    StructField("sensor4", DecimalType()),
    StructField("sensor5", DecimalType())
])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .format("json")
    .load(inputPath)
)

jsonInputDF = (
  streamingInputDF
    .select(
        to_json(struct(*[c for c in streamingInputDF.columns]))  # Convert into a DF with all columns in a json object
        .alias("value")                                           # in a single "value" column
    )
)



# COMMAND ----------

# Stream JSON data to Event Hub
ds = (
  jsonInputDF
    .select("body")
    .writeStream
    .format("eventhubs")
    .options(**ehConf)
    .trigger(processingTime='10 seconds')
    .option("checkpointLocation", checkpointPath)
    .start()
)

# COMMAND ----------

# Stream JSON data to Kafka endpoint in Event Hubs
ds = (
  jsonInputDF
    .writeStream
    .format("kafka") 
    .option("topic", kafkaTopic)
    .option("kafka.bootstrap.servers", kafkaBootstrapServers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafkaSaslJaasConfig)
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("kafka.batch.size", 5000) 
    .option("failOnDataLoss", "false")
    .option("checkpointLocation", checkpointPath)
    .start()
)




# COMMAND ----------

from pyspark.sql.avro.functions import from_avro, to_avro

evhubDF = (
  spark
    .readStream
    .format("kafka") 
    .option("subscribe", kafkaTopic)
    .option("kafka.bootstrap.servers", kafkaBootstrapServers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafkaSaslJaasConfig) 
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("failOnDataLoss", "false") 
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
    .withColumn("valueStr", col("value").cast(StringType()))
    .select("valueStr")
)

evhubDFJson = spark.read.json(evhubDF)

display(evhubDFJson)
