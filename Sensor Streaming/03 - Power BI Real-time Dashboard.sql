-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Streaming for Data Analysts and BI Developers
-- MAGIC Welcome to the streaming lab for data analysts and BI developers on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Access your enterprise Lakehouse in Azure using Databricks SQL
-- MAGIC 2. Explore data sets with SQL using SQL Endpoints powered by Photon
-- MAGIC 3. Load your streaming tables into Power BI to create a real-time streaming dashboard
-- MAGIC 
-- MAGIC ## The Use Case
-- MAGIC IoT device data coming from several locations in the US has been ingested and transformed. The IoT data is somewhat generic, so it could represent devices on the factory floor or even call center detail records. We will explore the IoT data directly in the Databricks SQL IDE using SQL Endpoints powered by Photon. We will then load the data into Power BI for reporting.  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Serve Silver and Gold Delta Lake tables through Databricks SQL to Power BI
-- MAGIC </br>
-- MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/SimpleIoT/sql2.png" alt="Data Flow" width="400">
-- MAGIC </br></br>
-- MAGIC 
-- MAGIC Cleansed, scored, and aggregated data is pulled out of the Silter and Gold Delta tables and then:
-- MAGIC 
-- MAGIC 1. is immediately available in real-time to BI tools and other applications via Databricks SQL
-- MAGIC 2. Delta Lake and Databricks SQL provide a complete separation of compute and storage
-- MAGIC 3. so streaming pipelines can continuously be updating the streaming Delta Lake tables and Databricks SQL can consume them without any conflicts
-- MAGIC 4. BI tools like Power BI can consume the streaming data through Databricks SQL using Automatic Page Refresh

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Initial Set-up

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text("Databricks_Token", "", "Databricks_Token")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Obtain a Personal Access Token and save it to the Databricks_Token widget  
-- MAGIC 1.Navigate to Settings -> User Settings  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT1.png" width="400">
-- MAGIC 
-- MAGIC 2.Under Access tokens -> Click Generate new token  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT2.png" width="300">
-- MAGIC 
-- MAGIC 3.Enter an optional description under comment -> Click Generate  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT3.png" width="400">
-- MAGIC 
-- MAGIC 4.Copy your token value to the clipboard -> Click Done  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT4.png" width="400">
-- MAGIC 
-- MAGIC 5.Save your token value to the Databricks_Token widget  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT5.png" width="600">
-- MAGIC 
-- MAGIC You'll use the Databricks_Token again in the next lab

-- COMMAND ----------

-- MAGIC %py
-- MAGIC Databricks_Token = dbutils.widgets.get("Databricks_Token")

-- COMMAND ----------

-- DBTITLE 1,Create Lab Queries
-- MAGIC %run "/Streaming-Demo/Setup Notebooks/00 - Create Queries" $Databricks_Token = $Databricks_Token

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Overview and Getting Connected to Dataricks SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Databricks SQL is a DW and BI Engine directly on the Lakehouse  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL2.png" width="300">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Compute in Databricks SQL is called SQL Endpoints  
-- MAGIC SQL Endpoints are compute clusters optimized for DW and BI queries on the Lakehouse  
-- MAGIC They are powered by Delta and Photon - a C++ vectorized engine that is really fast  
-- MAGIC Photon is __*FREE*__ in Databricks SQL!
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL3.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse your databases and tables using Data Explorer
-- MAGIC In Data Explorer you can see the objects you have access to  
-- MAGIC For each table/view, you can view the size, schema, sample data, table details, and even version history of the table  
-- MAGIC If you are an admin, you can even manage permissions to the databases and tables in Data Explorer  
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL4.1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The SQL Editor is a powerful SQL IDE built directly into Databricks SQL
-- MAGIC Browse and search for schema and table objects, view metadata, and explore tables/views  
-- MAGIC Use functionality like intellisense, view past query execution history, create data visualization capabilities, and even download data  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL5.1.png" width="500">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Queries are SQL code, results, and visualizations that you can save
-- MAGIC Create SQL quieres, save them, and share them with your team  
-- MAGIC View query results, or create data visualizations directly in the UI  
-- MAGIC Schedule refreshes of your queries so that the results are always up to date  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL6.1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Connect to the StreamingDemo Database using Databricks SQL and Power BI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Maintain Star Schemas for Power BI with Delta Live Tables
-- MAGIC To get the optimal performance from Power BI it is recommended to use a star schema data model and to make use of user-defined aggregated tables. However, as you build out your facts, dimensions, and aggregation tables and views in Delta Lake, ready to be used by the Power BI data model, it can become complex to manage all the pipelines, dependencies, and data quality.  
-- MAGIC 
-- MAGIC To help with all of the complexities, you can use DLT to develop, model, and manage the transformations, pipelines, and Delta Lake tables that will be used by Databricks SQL and Power BI.  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT7.1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Download the Power BI Desktop file to your Windows OS  
-- MAGIC https://github.com/adb-essentials/Streaming-Demo/blob/main/Power%20BI/SimpleSensorReport.pbit?raw=true  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open your Power BI Desktop File  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT8.png" width="200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Copy the Server hostName and HTTP Path from the SQL Endpoint into your Power BI parameters. Enter the metastore as hive_metastore and database as dlt_demo. Click OK  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT10.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL2.png" width="300">
-- MAGIC 
-- MAGIC ### Click on SQL Endpoints, click on the SQL Endpoint for your lab environment, click on connection details  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/SQLEndpoint.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### With the Power BI Parameters updated, Click Refresh, you can now browse the dlt_demo database in Power BI    
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT11.png" width="800">
