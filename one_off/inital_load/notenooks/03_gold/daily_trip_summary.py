# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
# DBTITLE 1

# COMMAND ----------

df = spark.read.table('nyctaxi.02_silver.yellow_trips_enriched')
display(df)

# COMMAND ----------

# DBTITLE 1,Cell 3
df = (df
    .groupBy(df.tpep_pickup_datetime.cast('date').alias('pickup_date'))
    .agg(
        count('*').alias('total_trips'),
        round(avg(df.passenger_count),1).alias('avg_passengers'),
        round(avg(df.trip_distance),1).alias('avg_trip_distance'),
        round(avg(df.fare_amount),2).alias('avg_fare__per_trip'),
        max(df.fare_amount).alias('max_fare_amount'),
        min(df.fare_amount).alias('min_fare_amount'),
        round(sum(df.total_amount),2).alias('total_revenue')
        ))
display(df)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.03_gold.daily_trip_summary')