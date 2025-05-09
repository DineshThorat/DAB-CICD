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

query =f"""delete from {gold_schema}.profitability.vol_mix_salesgrpcustmat where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}"""
spark.sql(query)

# COMMAND ----------

v_fisc_year = spark.sql(f"""select Year from {gold_schema}.common_dim.fisc_per where FiscalPeriod >= {v_start_fisc_per} AND FiscalPeriod <= {v_end_fisc_per} """).collect()[0][0]
print(v_fisc_year)

# COMMAND ----------

# DBTITLE 1,Column Creation
# Load and filter data for copa_items
df_copa_items = spark.table(f'{silver_schema}.ecc.copa_items') \
    .filter(
        (col("fisc_per") >= v_start_fisc_per) & 
        (col("fisc_per") <= v_end_fisc_per) &
        (~col("material").isin('109580', '101209', '101210', '109800')) & 
        (col('customer').isNotNull()) & 
        (~col('customer').like('899%'))
    )

# Prepare the customer outlet data
df_cust_outlet = spark.table(f"{gold_schema}.common_dim.current_outlets") \
    .withColumn("CustomerNo", col("CustomerNo").cast('string')) \
    .withColumn("CU", col("CU").cast('int'))

# Prepare the customer material data (filtered by material type)
df_cust_material = spark.table(f"{gold_schema}.common_dim.current_materials") \
    .withColumn("MaterialNo", col("MaterialNo").cast('string')) \
    .filter(col("MaterialType") == 'ZFER')

# Join copa_items with outlet and material data
df_copa_cust_outlet = df_copa_items.join(df_cust_outlet, df_copa_items.customer == df_cust_outlet.CustomerNo, 'left_outer')
df_copa_items_cust_mat_cogs = df_copa_cust_outlet.join(df_cust_material, df_copa_cust_outlet.material == df_cust_material.MaterialNo, 'left_outer')

# Create additional columns and perform the aggregation
df_copa_items_cust_mat_cogs = df_copa_items_cust_mat_cogs \
    .withColumn("bus_type_ext", col("GTMT")) \
    .withColumn("trade_grp", col("CUDChannelHead")) \
    .withColumn("sales_grp", col("RSM_MTMCut")) \
    .groupBy(
        "fisc_year",
        "fisc_per", 
        "bus_type_ext", 
        "trade_grp", 
        "sales_grp", 
        "customer", 
        "material"
    ) \
    .agg(
        sum(
            when(~col("sales_doc_type").isin('YDME', 'ZMKT'), col("physical_case_qty"))
            .otherwise(0)
        ).alias("physical_case_qty_sum_without"),        
        sum(
            col("physical_case_qty")
        ).alias("physical_case_qty_sum_with")
    )
# Join with standard COGS data
df_std_cogs = spark.table(f"{silver_schema}.sharepoint.standard_cogs")
df_final_0 = df_copa_items_cust_mat_cogs \
    .join(df_std_cogs, [(df_copa_items_cust_mat_cogs.material == df_std_cogs.material) & (df_copa_items_cust_mat_cogs.fisc_year == df_std_cogs.fisc_year)], how='left_outer') \
    .drop(df_std_cogs["material"], df_std_cogs["fisc_year"])

# Filter and join with fiscal period data
df_per = spark.table(f'{gold_schema}.common_dim.fisc_per') \
    .filter((col("FiscalPeriod") >= v_start_fisc_per) & (col("FiscalPeriod") <= v_end_fisc_per)) \
    .select("FiscalPeriod", "PeriodStart", "PeriodEnd", "Year")

df_final = df_final_0.join(df_per.alias("fp"), df_final_0["fisc_per"] == df_per["FiscalPeriod"], how="left_outer") \
    .filter(
        (df_final_0["__START_AT"] <= df_per["PeriodEnd"]) & 
        ((df_final_0["__END_AT"] <= df_per["PeriodEnd"]) | (df_final_0["__END_AT"].isNull()))
    )

window_spec = Window.partitionBy("fisc_per")
window_spec_per_salesgrp = Window.partitionBy("fisc_per","sales_grp")
window_spec_trad_grp = Window.partitionBy("fisc_per", "trade_grp")
window_spec_gtmt = Window.partitionBy("fisc_per", "bus_type_ext")


df_result = df_final\
    .withColumn("financial_vol_uc",col("physical_case_qty_sum_without")*col("conv_full_case")*col("conv_to_unit_case"))\
    .withColumn("total_fin_vol_uc",sum("financial_vol_uc").over(window_spec))\
    .withColumn("mix_total_fin_uc",col("financial_vol_uc") /col("total_fin_vol_uc")) \
    .withColumn("salesgrp_fin_vol_uc",sum("financial_vol_uc").over(window_spec_per_salesgrp))\
    .withColumn("mix_salesgrp_fin_uc",col("financial_vol_uc") /col("salesgrp_fin_vol_uc"))\
    .withColumn("marketing_vol_uc",col("physical_case_qty_sum_with")*col("conv_full_case")*col("conv_to_unit_case"))\
    .withColumn("total_mktg_vol_uc",sum("marketing_vol_uc").over(window_spec)) \
    .withColumn("mix_total_mktg_uc", col("marketing_vol_uc") / col("total_mktg_vol_uc"))\
    .withColumn("salesgrp_mktg_vol_uc", sum("marketing_vol_uc").over(window_spec_per_salesgrp)) \
    .withColumn("mix_salesgrp_mktg_uc", col("marketing_vol_uc") / col("salesgrp_mktg_vol_uc"))\
    .withColumn("tradegrp_mktg_vol_uc", sum("marketing_vol_uc").over(window_spec_trad_grp)) \
    .withColumn("mix_tradegrp_mktg_uc", col("marketing_vol_uc") / col("tradegrp_mktg_vol_uc")) \
     .withColumn("gtmt_mktg_vol_uc", sum("marketing_vol_uc").over(window_spec_gtmt)) \
    .withColumn("mix_gtmt_mktg_uc", col("marketing_vol_uc") / col("gtmt_mktg_vol_uc"))

ls = ["fisc_per","bus_type_ext", "trade_grp", "sales_grp", "customer","material","financial_vol_uc","total_fin_vol_uc","mix_total_fin_uc","salesgrp_fin_vol_uc","mix_salesgrp_fin_uc","marketing_vol_uc","total_mktg_vol_uc","mix_total_mktg_uc","salesgrp_mktg_vol_uc","mix_salesgrp_mktg_uc","tradegrp_mktg_vol_uc","mix_tradegrp_mktg_uc","gtmt_mktg_vol_uc","mix_gtmt_mktg_uc"]

df_vol_mix_salesgrp_custmart = df_result.select(*ls)
#df_vol_mix_salesgrp_custmart.display()

# COMMAND ----------

# DBTITLE 1,Table creation

df_vol_mix_salesgrp_custmart.write.partitionBy('fisc_per').mode('append').saveAsTable('dev_gold.profitability.vol_mix_salesgrpcustmat')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select fisc_per,count(*) from dev_gold.profitability.vol_mix_salesgrpcustmat group by fisc_per
