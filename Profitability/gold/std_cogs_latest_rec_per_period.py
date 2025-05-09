# Databricks notebook source
import os
from datetime import datetime,date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType,StructType, StructField, IntegerType, StringType,TimestampType,DateType,DoubleType,BooleanType
from pyspark.sql.window import Window
from os import listdir
from os.path import isfile, join
import shutil
import re
import sys
import shutil
import time

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

def std_cogs_call(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per):
  df_copa_items = spark.table(f'{silver_schema}.ecc.copa_items') \
    .filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per)).select('fisc_per','fisc_year','material')

  df_std_cogs = spark.table(f'{silver_schema}.sharepoint.standard_cogs')

  df_per = spark.table(f'{gold_schema}.common_dim.fisc_per').filter((col("FiscalPeriod") >= v_start_fisc_per ) & (col("FiscalPeriod") <= v_end_fisc_per))

  # Joining copa with std. cogs
  df_copa_cogs = df_copa_items \
    .join(df_per, df_copa_items.fisc_per == df_per.FiscalPeriod, how="inner") \
    .join(df_std_cogs, (df_copa_items.material == df_std_cogs.material) & (df_copa_items.fisc_year == df_std_cogs.fisc_year)  ,'right_outer') \
     .drop(df_copa_items["material"],df_copa_items["fisc_year"]) \
    .filter(((df_std_cogs.__START_AT <= df_per.PeriodEnd) & (df_std_cogs.__END_AT >= df_per.PeriodEnd))  \
        | ((df_std_cogs.__START_AT <= df_per.PeriodEnd) & (df_std_cogs.__END_AT.isNull()))) \
    .distinct() \
    .fillna(0.0)
  return df_copa_cogs

# COMMAND ----------

# %sql

# select * from dev_silver.sharepoint.standard_cogs where fisc_year=2023 and material='101906'
