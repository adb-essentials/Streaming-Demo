# Databricks notebook source
# MAGIC %md
# MAGIC ### STOP, do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

dbutils.widgets.text("Databricks_Token", "", "Databricks_Token")

# COMMAND ----------

Databricks_Token = dbutils.widgets.get("Databricks_Token")

# COMMAND ----------

Workspace = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").getOrElse(None)

# COMMAND ----------

import requests
response = requests.get(
  'https://%s/api/2.0/preview/sql/data_sources' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token}
)

# COMMAND ----------

datasource = response.json()[0]["id"]

# COMMAND ----------

query1 = {
  "data_source_id": datasource,
  "query": """
  SELECT * FROM streamingdemo.sensorinfobronze;
  """,
  "name": "Step1. Sensor Reference Data",
  "description": "Browse sensorinfobronze.",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query1
)

# COMMAND ----------

query2 = {
  "data_source_id": datasource,
  "query": """
  SELECT * FROM streamingdemo.dimdate;
  """,
  "name": "Step2. Date Dimension",
  "description": "Date Dimension",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query2
)

# COMMAND ----------

query3 = {
  "data_source_id": datasource,
  "query": """
  SELECT * FROM streamingdemo.sensorstreamsilver;
  """,
  "name": "Step3. Sensor Stream Silver",
  "description": "Silver table that has been joined with reference data and ML scored",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query3
)

# COMMAND ----------

query4 = {
  "data_source_id": datasource,
  "query": """
  SELECT window.start, window.end, * FROM streamingdemo.sensorstreamgoldaggregates;
  """,
  "name": "Step4. Sensor Stream Gold Aggregates",
  "description": "Gold aggregates table",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query4
)

# COMMAND ----------

query5 = {
  "data_source_id": datasource,
  "query": """
  SELECT
  A.window.start,
  A.window.end,
  A.*,
  T.Threshold
FROM
  streamingdemo.sensorstreamgoldaggregates A
  INNER JOIN streamingdemo.sensorthresholdsbronze T ON A.deviceName = T.deviceName
WHERE
  A.sensor5_avg > T.Threshold
  """,
  "name": "Step5. Threshold Detection Query",
  "description": "Gold Aggregates that are higher than the device threshold rule",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query5
)
