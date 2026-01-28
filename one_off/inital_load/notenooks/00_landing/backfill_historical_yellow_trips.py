# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet'
responese = urllib.request.urlopen(url)

dir_path = '/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2025-11'
os.makedirs(dir_path, exist_ok=True)

local_path = os.path.join(dir_path, 'yellow_tripdata_2025-11.parquet')
with open(local_path, 'wb') as f:
  shutil.copyfileobj(responese, f)

# COMMAND ----------

# MAGIC %md
# MAGIC 视频教学同时添加多个文件数据，但是自己操作出现错误，代码相同

# COMMAND ----------

data_to_process = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06']

for date in dates_to_process:
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet'
    responese = urllib.request.urlopen(url)

    dir_path = '/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}'
    os.makedirs(dir_path, exist_ok=True)

    local_path = os.path.join(dir_path, 'yellow_tripdata_{date}.parquet')
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(responese, f)