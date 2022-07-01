-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Streaming for Data Engineers on Delta Live Tables
-- MAGIC Welcome to the streaming lab for data engineers on Azure Databricks for Delta Live Tables (DLT)! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Create a DLT notebook with Data Quality rules
-- MAGIC 2. Create a DLT Pipelines and run it
-- MAGIC 3. See how DLT can simplify streaming pipelines and make them easier to manage
-- MAGIC 
-- MAGIC ## The Use Case
-- MAGIC We will ingest and transform IoT device data coming from several locations in the US.  The IoT data is somewhat generic, so it could represent devices on the factory floor or even call center detail records. The goal of the notebook is create a DLT pipeline which streams and transform the IoT data into a Lakehouse database for downstream consumption via Databricks SQL and BI tools like Power BI. 
-- MAGIC 
-- MAGIC ***This DLT pipeline will not work unless you have already mounted your storage account. Refer to notebook "01 - Sensor Model Trainer" for mounting instructions.***

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delta Live Tables Simplifies your Batch and Streaming ETL Pipelines  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT0.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a DLT notebook with Data Quality rules

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse the Delta Live Tables code in the following notebook  
-- MAGIC ..ADBQuickStartLabs/DLT Demo/DTL
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLTCode1.png" width="700">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Delta Live Tables Pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Navigate to Databricks Workflows
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT1.png" width="150">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new Delta Live Tables Pipeline  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT2.png" width="600">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Configure your Delta Live Tables Pipeline using the following configurations  
-- MAGIC 
-- MAGIC **Product edition:** leave as Advanced  
-- MAGIC **Pipeline name:** QuickStart Labs DLT  
-- MAGIC **Notebook Libraries:** browse to the DLT Notebook at ...ADBQuickStartLabs/DLT Demo/DTL  
-- MAGIC **Configuration:** pipelines.applyChangesPreviewEnabled  true   
-- MAGIC **Target:** dlt_demo  
-- MAGIC **Storage location:** leave blank  
-- MAGIC **Enable autoscaling:** uncheck  
-- MAGIC **Cluster workers:** 2  
-- MAGIC **Use Photon Acceleration:**  check  
-- MAGIC **Click Create**
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT3.png" width="500">  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT4.png" width="500">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Start your new DLT pipeline  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT5.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse the lineage and data quality information of your completed DLT pipeline  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT6.png" width="1200">
