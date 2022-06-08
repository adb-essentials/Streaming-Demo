# Databricks notebook source
# MAGIC %md
# MAGIC ### Download Device Info CSV and Load Delta Table

# COMMAND ----------

# MAGIC %sh
# MAGIC # Pull CSV file from url
# MAGIC wget -nc https://raw.githubusercontent.com/adb-essentials/Streaming-Demo/main/Reference%20Data/DeviceInfo.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/DeviceInfo.csv", "/mnt/streamingdemo/temp/DeviceInfo.csv")

# COMMAND ----------

df_sensorInfo = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv("/mnt/streamingdemo/temp/DeviceInfo.csv")\
  .write.format("delta").mode("overwrite").save("/mnt/streamingdemo/data/sensorInfoBronze/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streamingdemo.sensorInfoBronze;
# MAGIC 
# MAGIC CREATE TABLE streamingdemo.sensorInfoBronze
# MAGIC USING DELTA 
# MAGIC LOCATION "/mnt/streamingdemo/data/sensorInfoBronze/";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streamingdemo.sensorInfoBronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Date dimension

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW streamingdemo.date_vw 
# MAGIC AS 
# MAGIC WITH calendarDate AS (
# MAGIC   select
# MAGIC     explode(
# MAGIC       sequence(
# MAGIC         to_date('2022-01-01'),
# MAGIC         to_date('2022-12-31'),
# MAGIC         interval 1 day
# MAGIC       )
# MAGIC     ) AS calendarDate
# MAGIC )
# MAGIC --SELECT * FROM calendarDate
# MAGIC select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
# MAGIC   CalendarDate,
# MAGIC   year(calendarDate) AS CalendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as CalendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(calendarDate) as DayOfWeek,
# MAGIC   weekday(calendarDate) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as DayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as DayOfYear,
# MAGIC   weekofyear(calendarDate) as WeekOfYearIso,
# MAGIC   quarter(calendarDate) as QuarterOfYear
# MAGIC from
# MAGIC   calendarDate
# MAGIC order by
# MAGIC   calendarDate

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS streamingdemo.dimDate
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/streamingdemo/data/dimDate"
# MAGIC AS
# MAGIC SELECT * FROM streamingdemo.date_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streamingdemo.dimDate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimized our Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE streamingdemo.sensorInfoBronze

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE streamingdemo.dimDate
