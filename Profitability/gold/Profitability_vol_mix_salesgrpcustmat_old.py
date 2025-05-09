# Databricks notebook source
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit,col
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d")

df_per = spark.table('dev_gold.common_dim.fisc_per')
df_fisc_per = df_per.filter((lit(formatted_date).cast("date") >= col("PeriodStart")) & 
    (lit(formatted_date).cast("date") <= col("PeriodEnd"))).select('FiscalPeriod')

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


#################################################################################################################################
############################################## Import Liabraries   ##############################################################
#################################################################################################################################
import pandas as pd
import os
from datetime import datetime,date
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType
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

spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss",False)

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------



# COMMAND ----------

query =f"""delete from {gold_schema}.profitability.vol_mix_salesgrpcustmat where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}"""
spark.sql(query)

# COMMAND ----------

df_cust_outlet = spark.table(f"{gold_schema}.common_dim.current_outlets")

# COMMAND ----------

spark.table(f'{silver_schema}.ecc.copa_items').filter(((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per)) & (~col("sales_doc_type").isin('YDME','ZMKT')) & (col("material_type") == 'ZFER') & (col("material") != '109580')).display()

# COMMAND ----------

df_cust_outlet.display()

# COMMAND ----------

v_fisc_year = spark.sql(f"""select Year from {gold_schema}.common_dim.fisc_per where FiscalPeriod >= {v_start_fisc_per} AND FiscalPeriod <= {v_end_fisc_per} """).collect()[0][0]
print(v_fisc_year)

# COMMAND ----------

#Reading copa table
df_copa_items = spark.table(f'{silver_schema}.ecc.copa_items').filter(((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per)) & (~col("sales_doc_type").isin('YDME','ZMKT')) & (col("material_type") == 'ZFER') & (col("material") != '109580'))

df_cust_outlet = spark.table(f"{gold_schema}.common_dim.current_outlets")\
                      .withColumn("CustomerNo", col("CustomerNo").cast('string'))\
                      .withColumn("CU",col("CU").cast('int'))

df_std_cogs = spark.table(f"""{silver_schema}.sharepoint.standard_cogs""").filter(col("fisc_year") == v_fisc_year)

window_spec_per = Window.partitionBy("fisc_per")
window_spec_per_cust = Window.partitionBy("fisc_per","customer")
window_spec_per_mat = Window.partitionBy("fisc_per","material")
window_spec_per_cust_mat = Window.partitionBy("fisc_per","customer","material")
window_spec_per_salesgrp_custmat = Window.partitionBy("fisc_per","sales_group","customer","material")

df_copa_items_cust = df_copa_items.join(df_cust_outlet,[df_copa_items.customer == df_cust_outlet.CustomerNo],'left_outer').select("fisc_per","CU","customer","material","physical_case_qty")

df_copa_items = df_copa_items.groupBy("fisc_per","CU","customer","material").agg(sum("unit_case_qty").cast('decimal(38,18)').alias('volume_uc'),sum("physical_case_qty").cast('decimal(38,18)').alias('sum_physical_case_qty'))

df_result = df_copa_items\
                    .withColumn("total_qty_uc",sum("volume_uc").over(window_spec_per))\
                    .withColumn("pct_total_uc",col("volume_uc")/col("total_qty_uc").cast('decimal(38,18)'))\
                    .withColumn("total_cust_qty_uc",sum("volume_uc").over(window_spec_per_cust))\
                    .withColumn("pct_cust_total_uc",col("volume_uc")/col("total_cust_qty_uc").cast('decimal(38,18)'))\
                    .withColumn("total_mat_qty_uc",sum("volume_uc").over(window_spec_per_mat))\
                    .withColumn("pct_mat_total_uc",col("volume_uc")/col("total_mat_qty_uc").cast('decimal(38,18)'))
                             

#Reading standard cogs table
df_std_cogs = spark.table('dev_silver.sharepoint.standard_cogs').filter(col("fisc_year") == v_fisc_year)
# Joining copa with std. cogs
df_copa_cogs = df_result.join(df_std_cogs, df_result.material == df_std_cogs.material ,'left').drop(df_std_cogs["material"])

# COMMAND ----------

df_vol_mix_salesgrp_custmart = df_copa_cogs\
                           .withColumn("volume_pc", col("sum_physical_case_qty") * col("conv_full_case"))\
                           .withColumn("total_qty_pc",sum("volume_pc").over(window_spec_per))\
                           .withColumn("pct_total_pc",col("volume_pc")/col("total_qty_pc"))\
                           .withColumn("pct_grp_total_uc",col("volume_uc")/sum("volume_uc").over(window_spec_per_salesgrp_mat))\
                           .withColumn("pct_grp_total_pc",col("volume_pc")/sum("volume_pc").over(window_spec_per_salesgrp_mat))

df_vol_mix_salesgrp_custmart = df_vol_mix_salesgrp_custmart.select("fisc_per","sales_group","material","customer","volume_uc","pct_total_uc","pct_grp_total_uc","volume_pc","pct_total_pc","pct_grp_total_pc")

# COMMAND ----------

if df_vol_mix_salesgrp_custmart.groupBy("fisc_per","sales_group","customer","material").count().filter("count > 1").count() > 0:
    raise ValueError("Duplicate values found in 'fisc_per,sales_group,customer,material' column. Cannot proceed.")
else:
    df_vol_mix_salesgrp_custmart.write.partitionBy('fisc_per').mode('append').saveAsTable('dev_gold.profitability.vol_mix_salesgrpcustmat')
