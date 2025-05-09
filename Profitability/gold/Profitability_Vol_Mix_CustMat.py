# Databricks notebook source
# DBTITLE 1,Import Libraries
import pandas as pd
import os
from datetime import datetime,date
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DateType,DoubleType,BooleanType
from pyspark.sql.window import Window
from os import listdir
from os.path import isfile, join
from pyspark.sql.functions import *
import shutil
import re
import sys
from functools import reduce
import shutil
from hashlib import md5, sha256
from os import urandom
import time

# COMMAND ----------

# DBTITLE 1,get and set fisc_per and env variables
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,set default fisc_per
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d")

df_per = spark.table('dev_gold.common_dim.fisc_per')
df_fisc_per = df_per.filter((lit(formatted_date).cast("date") >= col("PeriodStart")) & 
    (lit(formatted_date).cast("date") <= col("PeriodEnd"))).select('FiscalPeriod','Year')

start_fisc_per = df_fisc_per.collect()[0][0]
end_fisc_per = df_fisc_per.collect()[0][0]

dbutils.widgets.text("start_fisc_per", start_fisc_per, 'Start Fiscal Period')
dbutils.widgets.text("end_fisc_per", end_fisc_per, 'End Fiscal Period')

v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")


if not v_start_fisc_per:
    v_start_fisc_per = start_fisc_per
if not v_end_fisc_per:
    v_end_fisc_per = end_fisc_per

print(f"Start Fiscal Period: {v_start_fisc_per}")
print(f"End Fiscal Period: {v_end_fisc_per}")


# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# MAGIC %run 
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/std_cogs_latest_rec_per_period

# COMMAND ----------

# DBTITLE 1,set schemas based on env
bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

v_fisc_year = spark.sql(f"""select Year from {gold_schema}.common_dim.fisc_per where FiscalPeriod >= {v_start_fisc_per} AND FiscalPeriod <= {v_end_fisc_per} """).collect()[0][0]
print(v_fisc_year)

# COMMAND ----------

export_skus = list(export_skus)
CO2_skus = list(CO2_skus)

# COMMAND ----------

# DBTITLE 1,get data from COPA Items
#get data from COPA Items
df_copa_items = spark.table(f'{silver_schema}.ecc.copa_items') \
    .filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per) \
    & (col("material_type") == finished_goods_mat_type) \
    & (~col("material").isin(export_skus)) \
    & (~col("material").isin(CO2_skus)) \
    & (col("customer").isNotNull()) & (~col("customer").contains("899%"))) \
    .select("fisc_per","customer","material","physical_case_qty","sales_doc_type")

# COMMAND ----------

window_spec_per = Window.partitionBy("fisc_per")
window_spec_per_cust = Window.partitionBy("fisc_per","customer")
window_spec_per_mat = Window.partitionBy("fisc_per","material")
window_spec_per_cust_mat = Window.partitionBy("fisc_per","customer","material")

# COMMAND ----------

# DBTITLE 1,aggregate to customer material
df_cust_mat_per = df_copa_items.groupBy("fisc_per","customer","material") \
    .agg(sum(when(~col("sales_doc_type").isin('YDME','ZMKT'), col("physical_case_qty")).otherwise(0).cast('decimal(38,18)')).alias("financial_vol_pc"), \
        sum("physical_case_qty").alias("marketing_vol_pc"))

# COMMAND ----------

# DBTITLE 1,get std cogs
df_std_cogs = std_cogs_call(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per).select("fisc_per", "material", "conv_full_case", "conv_to_unit_case","__START_AT","__END_AT")

# COMMAND ----------

# DBTITLE 1,join std cogs
df_result = df_cust_mat_per \
    .join(df_std_cogs, ["fisc_per", "material"],'left_outer')

# COMMAND ----------

# display(df_result.filter(col("fisc_per") == '2024010'))

# COMMAND ----------

# DBTITLE 1,calculate mix columns
df_vol_mix_custmat = df_result\
    .withColumn("financial_vol_uc", col("financial_vol_pc") * col("conv_to_unit_case") * col("conv_full_case"))\
    .withColumn("total_qty_uc",sum("financial_vol_uc").over(window_spec_per))\
    .withColumn("mix_total_uc",(col("financial_vol_uc")/col("total_qty_uc")).cast("decimal(38,18)"))\
    .withColumn("total_cust_qty_uc",sum("financial_vol_uc").over(window_spec_per_cust))\
    .withColumn("mix_cust_total_uc",(col("financial_vol_uc")/col("total_cust_qty_uc")).cast("decimal(38,18)"))\
    .withColumn("total_mat_qty_uc",sum("financial_vol_uc").over(window_spec_per_mat))\
    .withColumn("mix_mat_total_uc",(col("financial_vol_uc")/col("total_mat_qty_uc")).cast("decimal(38,18)"))\
    .withColumn("marketing_vol_uc", col("marketing_vol_pc") * col("conv_to_unit_case") * col("conv_full_case"))\
    .withColumn("total_marketing_qty_uc",sum("marketing_vol_uc").over(window_spec_per))\
    .withColumn("mix_total_mktg_uc",(col("marketing_vol_uc")/col("total_marketing_qty_uc")).cast("decimal(38,18)"))\
    .withColumn("total_cust_marketing_qty_uc",sum("marketing_vol_uc").over(window_spec_per_cust))\
    .withColumn("mix_cust_total_marketing_uc",(col("marketing_vol_uc")/col("total_cust_marketing_qty_uc")).cast("decimal(38,18)"))\
    .withColumn("total_mat_marketing_qty_uc",sum("marketing_vol_uc").over(window_spec_per_mat))\
    .withColumn("mix_mat_total_marketing_uc",(col("marketing_vol_uc")/col("total_mat_marketing_qty_uc")).cast("decimal(38,18)")) \
    .select("fisc_per","customer","material","financial_vol_uc","mix_total_uc","mix_cust_total_uc","mix_mat_total_uc","marketing_vol_uc", "mix_total_mktg_uc", "mix_cust_total_marketing_uc", "mix_mat_total_marketing_uc")


# COMMAND ----------

#df_vol_mix_custmat.groupBy('fisc_per').count().display()

# COMMAND ----------

# DBTITLE 1,create target table if not exists
create_query = f"""
create table if not exists {gold_schema}.profitability.vol_mix_custmat
(fisc_per string COMMENT 'Fiscal Year and Period in SAP Database Format YYYYPPP',
customer string COMMENT 'SAP Customer Code',
material string COMMENT 'SAP Material Code',
financial_vol_uc decimal(38,6),
mix_total_uc decimal(38,18),
mix_cust_total_uc decimal(38,18),
mix_mat_total_uc decimal(38,18),
marketing_vol_uc decimal(38,6),
mix_total_mktg_uc decimal(38,18),
mix_cust_total_marketing_uc decimal(38,18),
mix_mat_total_marketing_uc decimal(38,18))
COMMENT 'contains the volume and volume mix calculations by customer and material in each period.'
CLUSTER BY (fisc_per, customer, material)
"""
spark.sql(create_query)

# COMMAND ----------

# DBTITLE 1,delete existing data for fisc_per
query = f"""
DELETE FROM {gold_schema}.profitability.vol_mix_custmat
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,write data to target
if df_vol_mix_custmat.groupBy("fisc_per","customer","material").count().filter("count > 1").count() > 0:
    raise ValueError("Duplicate values found in 'id' column. Cannot proceed.")
else:
    df_vol_mix_custmat.write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(f'{gold_schema}.profitability.vol_mix_custmat')
