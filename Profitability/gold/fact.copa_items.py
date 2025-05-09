# Databricks notebook source
# MAGIC %md
# MAGIC ## Define Widgets

# COMMAND ----------

dbutils.widgets.text("start_fisc_per", "", "")
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
dbutils.widgets.text("end_fisc_per", "", "")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")
print(Env)

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# MAGIC %run 
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/std_cogs_latest_rec_per_period

# COMMAND ----------

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

query = f"""
DELETE FROM {gold_schema}.fact.copa_items
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

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

df_per = spark.table(f'{gold_schema}.common_dim.fisc_per').filter((col("FiscalPeriod") >= v_start_fisc_per ) & (col("FiscalPeriod") <= v_end_fisc_per))
v_fisc_year = df_per.select("year").collect()[0][0]
print(v_fisc_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Copa Items, Result, Vol Mix Custmat, Standard Cogs DF

# COMMAND ----------

df_copa_items = spark.table(f"""{silver_schema}.ecc.copa_items""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per) & (col('material_type') != 'ZEMP')).select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","unit_case_qty","physical_case_qty","revenue","reserve_92","trade_promo", "rebates_incl", "accruals_incl","other_surcharge","fuel_surcharge", "net_share", "cogs_conctr_tccc", "cogs_conctr_nontccc", "plant")

df_result = df_copa_items.groupBy("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel", "plant").agg(sum("unit_case_qty").alias("sum_unit_case_qty_per_line").cast('decimal(38,18)'),sum(when(~col("material").isin('101209', '101210', '109800'), col("physical_case_qty").cast('decimal(38,18)'))
.otherwise(0.0)).alias("sum_physical_case_qty_per_line").cast('decimal(38,18)'),sum(when(col("sales_doc_type").isin('YDME','ZMKT'), col("unit_case_qty")).otherwise(0)).alias("free_goods_sum_unit_case_qty_per_line"),sum(when(col("sales_doc_type").isin('YDME','ZMKT'), col("physical_case_qty")).otherwise(0)).alias("free_goods_sum_physical_case_qty_per_line"),sum("revenue").alias("sum_revenue_per_line").cast('decimal(25,2)'),sum("reserve_92").alias("sum_reserve_92").cast('decimal(20,5)'),sum("trade_promo").alias("sum_trade_promo").cast('decimal(20,5)'), (sum("rebates_incl") + sum("accruals_incl")).alias("rebates_accruals_total").cast('decimal(25,3)'),sum("other_surcharge").alias("sum_other_surcharge").cast('decimal(25,3)'),sum("fuel_surcharge").alias("sum_fuel_surcharge").cast('decimal(25,3)'), sum("net_share").alias("sum_net_share"), sum("cogs_conctr_tccc").alias("sum_cogs_conctr_tccc"), sum("cogs_conctr_nontccc").alias("sum_cogs_conctr_nontccc"))

df_vol_mix_custmat = spark.table(f"""{gold_schema}.profitability.vol_mix_custmat""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))


df_per = spark.table(f'{gold_schema}.common_dim.fisc_per').filter((col("FiscalPeriod") >= v_start_fisc_per ) & (col("FiscalPeriod") <= v_end_fisc_per)).select("FiscalPeriod","PeriodStart","PeriodEnd","Year")

# df_std_cogs = df_std_cogs.join(df_per.alias("fp"), 
#                                df_std_cogs["fisc_year"] == df_per["Year"], how="left_outer")\
#          .filter((df_std_cogs["__START_AT"] <= df_per["PeriodEnd"]) & ((df_std_cogs["__END_AT"] <= df_per["PeriodEnd"]) | (df_std_cogs["__END_AT"].isNull())))

df_per.createOrReplaceTempView('df_per_v')

# COMMAND ----------

df_std_cogs = std_cogs_call(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### ylp2 Dataframe

# COMMAND ----------

if '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per <= '2024003':
    # Entirely within Condition 1
    query = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            cond.material,
            items.plant,
            cond.profit_center,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS ylp2_amount
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_ylp2_items_posting_status}' 
          AND cond.condition_type = '{df_ylp2_cond_condition_type}'
          AND items.fisc_per >= '{v_start_fisc_per}' 
          AND items.fisc_per <= '{v_end_fisc_per}'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            cond.material, 
            items.plant, 
            cond.profit_center
    """
    df_ylp2 = spark.sql(query)

elif '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per >= '2024004':
    # Overlap between Condition 1 and Condition 2
    # Query for 2024001 to 2024003 (Condition 1)
    query_1 = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            cond.material,
            items.plant,
            cond.profit_center,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS ylp2_amount
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_ylp2_items_posting_status}' 
          AND cond.condition_type = '{df_ylp2_cond_condition_type}'
          AND items.fisc_per >= '2024001' 
          AND items.fisc_per <= '2024003'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            cond.material, 
            items.plant, 
            cond.profit_center
    """
    # Query for 2024004 to 2024012 (Condition 2)
    query_2 = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            items.sales_itemnum,
            cond.material,
            items.plant,
            cond.profit_center,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS ylp2_amount
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_ylp2_items_posting_status}' 
          AND cond.condition_type = '{df_ylp2_cond_condition_type}'
          AND items.fisc_per >= '2024004' 
          AND items.fisc_per <= '2024012'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            items.sales_itemnum, 
            cond.material, 
            items.plant, 
            cond.profit_center
    """
    # Execute both queries
    df_ylp2_1 = spark.sql(query_1)
    df_ylp2_2 = spark.sql(query_2)
    
    # Combine results if needed
    df_ylp2 = df_ylp2_1.unionByName(df_ylp2_2, allowMissingColumns=True)

elif '2024004' <= v_start_fisc_per:
    # Entirely within Condition 2
    query = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            items.sales_itemnum,
            cond.material,
            items.plant,
            cond.profit_center,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS ylp2_amount
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_ylp2_items_posting_status}' 
          AND cond.condition_type = '{df_ylp2_cond_condition_type}'
          AND items.fisc_per >= '{v_start_fisc_per}' 
          AND items.fisc_per <= '{v_end_fisc_per}'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            items.sales_itemnum, 
            cond.material, 
            items.plant, 
            cond.profit_center
    """
    df_ylp2 = spark.sql(query)
else:
    raise ValueError("Invalid fiscal period range selected.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### On Invoice Discounts Dataframe

# COMMAND ----------

if '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per <= '2024003':
    # Entirely within Condition 1
    query = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            cond.material,
            items.plant,
            cond.profit_center,
            cond.condition_type,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS total_condition_val
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_on_inv_disc_bc_posting_status}' 
          AND cond.condition_type in {df_on_inv_disc_bc_condition_type}
          AND items.fisc_per >= '{v_start_fisc_per}' 
          AND items.fisc_per <= '{v_end_fisc_per}'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            cond.material, 
            items.plant, 
            cond.profit_center,
            cond.condition_type
    """
    df_on_inv_disc = spark.sql(query)

elif '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per >= '2024004':
    # Overlap between Condition 1 and Condition 2
    # Query for 2024001 to 2024003 (Condition 1)
    query_1 = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            cond.material,
            items.plant,
            cond.profit_center,
            cond.condition_type,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS total_condition_val
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_on_inv_disc_bc_posting_status}'
          AND cond.condition_type in {df_on_inv_disc_bc_condition_type}
          AND items.fisc_per >= '2024001' 
          AND items.fisc_per <= '2024003'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            cond.material, 
            items.plant, 
            cond.profit_center,
            cond.condition_type
    """
    # Query for 2024004 to 2024012 (Condition 2)
    query_2 = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            items.sales_itemnum,
            cond.material,
            items.plant,
            cond.profit_center,
            cond.condition_type,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS total_condition_val
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_on_inv_disc_bc_posting_status}'
          AND cond.condition_type in {df_on_inv_disc_bc_condition_type}
          AND items.fisc_per >= '2024004' 
          AND items.fisc_per <= '2024012'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            items.sales_itemnum, 
            cond.material, 
            items.plant, 
            cond.profit_center,
            cond.condition_type
    """
    # Execute both queries
    df_on_inv_disc_1 = spark.sql(query_1)
    df_on_inv_disc_2 = spark.sql(query_2)
    
    # Combine results if needed
    df_on_inv_disc = df_on_inv_disc_1.unionByName(df_on_inv_disc_2, allowMissingColumns=True)

elif '2024004' <= v_start_fisc_per:
    # Entirely within Condition 2
    query = f"""
        SELECT 
            items.fisc_per,
            items.outlet,
            cond.bill_docnum,
            items.sales_docnum,
            items.sales_itemnum,
            cond.material,
            items.plant,
            cond.profit_center,
            cond.condition_type,
            SUM(CAST(cond.condition_val AS DECIMAL(38, 10))) AS total_condition_val
        FROM {silver_schema}.bw.billing_conditions cond
        INNER JOIN {silver_schema}.bw.billing_items items 
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE items.posting_status = '{df_on_inv_disc_bc_posting_status}' 
          AND cond.condition_type in {df_on_inv_disc_bc_condition_type}
          AND items.fisc_per >= '{v_start_fisc_per}' 
          AND items.fisc_per <= '{v_end_fisc_per}'
          AND items.reverse IS NULL 
          AND items.canceled IS NULL
        GROUP BY 
            items.fisc_per, 
            items.outlet, 
            cond.bill_docnum, 
            items.sales_docnum, 
            items.sales_itemnum, 
            cond.material, 
            items.plant, 
            cond.profit_center,
            cond.condition_type
    """
    df_on_inv_disc = spark.sql(query)
else:
    raise ValueError("Invalid fiscal period range selected.")

# COMMAND ----------


if '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per <= '2024003':
 # Fixed Discount
 df_fixed_disc_oninv = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))

 # Variable Discount
 df_var_disc_oninv = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("ZA01"), col("total_condition_val") / vat).otherwise(0)).alias('var_disc_amount'))

 # Pickup Discount
 df_pickup_disc = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("YLP9"), col("total_condition_val") / vat).otherwise(0)).alias('pickup_disc_amount'))

elif '2023001' <= v_start_fisc_per <= '2024003' and v_end_fisc_per >= '2024004':
 # Overlap between Condition 1 and Condition 2
 # Query for 2024001 to 2024003 (Condition 1)
 
 df_fixed_disc_oninv_1 = df_on_inv_disc.filter((col('fisc_per')>='2024001') & (col('fisc_per')<='2024003')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))
 
 # Query for 2024004 to 2024012 (Condition 2)
 df_fixed_disc_oninv_2 = df_on_inv_disc.filter((col('fisc_per')>='2024004') & (col('fisc_per')<='2024012')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))

 # Combine results if needed
 df_fixed_disc_oninv = df_fixed_disc_oninv_1.unionByName(df_fixed_disc_oninv_2, allowMissingColumns=True)

 df_var_disc_oninv_1 = df_on_inv_disc.filter((col('fisc_per')>='2024001') & (col('fisc_per')<='2024003')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("ZA01"), col("total_condition_val") / vat).otherwise(0)).alias('var_disc_amount'))

 df_var_disc_oninv_2 = df_on_inv_disc.filter((col('fisc_per')>='2024004') & (col('fisc_per')<='2024012')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("ZA01"), col("total_condition_val") / vat).otherwise(0)).alias('var_disc_amount'))

 # Combine results if needed
 df_var_disc_oninv = df_var_disc_oninv_1.unionByName(df_var_disc_oninv_2, allowMissingColumns=True)

 df_pickup_disc_1 = df_on_inv_disc.filter((col('fisc_per')>='2024001') & (col('fisc_per')<='2024003')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center').agg(sum(when(col("condition_type").isin("YLP9"), col("total_condition_val") / vat).otherwise(0)).alias('pickup_disc_amount'))

 df_pickup_disc_2 = df_on_inv_disc.filter((col('fisc_per')>='2024004') & (col('fisc_per')<='2024012')).groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("YLP9"), col("total_condition_val") / vat).otherwise(0)).alias('pickup_disc_amount'))

 # Combine results if needed
 df_pickup_disc = df_pickup_disc_1.unionByName(df_pickup_disc_2, allowMissingColumns=True)

elif '2024004' <= v_start_fisc_per:
 # Entirely within Condition 2
 # Fixed Discount
 df_fixed_disc_oninv = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))

 # Variable Discount
 df_var_disc_oninv = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum','sales_itemnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("ZA01"), col("total_condition_val") / vat).otherwise(0)).alias('var_disc_amount'))

 # Pickup Discount
 df_pickup_disc = df_on_inv_disc.groupBy("fisc_per",'outlet',"bill_docnum",'sales_docnum','sales_itemnum',"material",'plant','profit_center','sales_itemnum').agg(sum(when(col("condition_type").isin("YLP9"), col("total_condition_val") / vat).otherwise(0)).alias('pickup_disc_amount'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### IPP Dataframe

# COMMAND ----------

df_ipp = spark.sql(f"""
                  select *, amount as amount_ipp from {silver_schema}.files.ipp
                  where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                  """).drop("amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Off Invoice Dataframe

# COMMAND ----------

df_off_inv_disc = spark.sql(f"""
    SELECT 
        fisc_per, 
        customer, 
        material, 
        SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN -1 * amount ELSE 0 END) AS fixed_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN -1 * amount ELSE 0 END) AS var_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN -1 * amount ELSE 0 END) AS pickup_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Promo' THEN -1 * amount ELSE 0 END) AS promo_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Delivery Penalty' THEN -1 * amount ELSE 0 END) AS del_pen_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Fallout' THEN -1 * amount ELSE 0 END) AS dfr_fallout_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR DMI' THEN -1 * amount ELSE 0 END) AS dmi_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR DMI Fallout' THEN -1 * amount ELSE 0 END) AS dmi_fallout_amnt_off_inv
    FROM
        {silver_schema}.files.off_invoice_discounts
    WHERE 
        fisc_per >= {v_start_fisc_per} 
        AND fisc_per <= {v_end_fisc_per}
    GROUP BY
        fisc_per, customer, material
""").na.fill(0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cr Dr Memos Dataframe

# COMMAND ----------

df_cr_dr_memos_hist = spark.sql(f"""
                           select fisc_per , customer, material,
                           SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN -1 * amount ELSE 0 END) AS fixed_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN -1 * amount ELSE 0 END) AS var_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN -1 * amount ELSE 0 END) AS pickup_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Promo' THEN -1 * amount ELSE 0 END) AS promo_amnt_cr_dr_hist,
                           SUM(-1 * amount) AS total_cr_dr_amnt_hist
                           from
                           {silver_schema}.files.cr_dr_memos
                           where fisc_per >= {v_start_fisc_per} 
                           AND fisc_per <= {v_end_fisc_per}
                           group by fisc_per, customer, material
                           """).na.fill(0.0)

# COMMAND ----------

df_cr_dr_memos_cur = spark.sql(f"""
    SELECT fisc_per, customer, 
           var_amnt_cr_dr_memos_cur, 
           pickup_amnt_cr_dr_memos_cur, 
           fixed_amnt_cr_dr_memos_cur, 
           promo_amnt_cr_dr_memos_cur,
           (var_amnt_cr_dr_memos_cur + pickup_amnt_cr_dr_memos_cur 
           + fixed_amnt_cr_dr_memos_cur + promo_amnt_cr_dr_memos_cur) AS total_cr_dr_amnt_cur
    FROM (
        SELECT items.fisc_per, 
               items.outlet as customer,
               SUM(CASE WHEN cond.material = '80015898' THEN cond.condition_val ELSE 0 END) AS var_amnt_cr_dr_memos_cur,
               SUM(CASE WHEN cond.material = '80026735' THEN cond.condition_val ELSE 0 END) AS pickup_amnt_cr_dr_memos_cur,
               SUM(CASE WHEN cond.material = '80026736' THEN cond.condition_val ELSE 0 END) AS fixed_amnt_cr_dr_memos_cur,
               SUM(CASE WHEN cond.material = '80026737' THEN cond.condition_val ELSE 0 END) AS promo_amnt_cr_dr_memos_cur
        FROM dev_silver.bw.billing_conditions cond
        INNER JOIN dev_silver.bw.billing_items items
            ON cond.bill_docnum = items.bill_docnum 
            AND cond.bill_itemnum = items.bill_itemnum
        WHERE cond.condition_type = 'YADJ' 
            AND items.posting_status = 'C' 
            AND items.fisc_per >= '2024012'
        GROUP BY items.fisc_per, items.outlet
    ) AS subquery
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gl CTM Dataframe

# COMMAND ----------

df_gl_balances = spark.sql(f"""select * from dev_gold.profitability.gl_balances
                           where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                           """)

# COMMAND ----------

df_gl_ctm = spark.sql(f"""
                      select fisc_per, sum(balance) as amount_gl_ctm
                      from dev_gold.profitability.gl_balances
                      where gl_account in {df_gl_ctm_gl_account}
                      and fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                      group by fisc_per
                      """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gl DFR Curve Dataframe

# COMMAND ----------

df_gl_dfr_curve = df_gl_balances.filter((col("gl_account") == '405801210')&(col("profit_center") == 'PH12150121')).groupBy("fisc_per").agg(sum("balance").alias("dfr_curve_amnt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gl Other Rev Dataframe

# COMMAND ----------

df_gl_other_rev = df_gl_balances.filter(col("gl_account").between('305400000', '305499999')).groupBy("fisc_per").agg(sum("balance").alias("amount_gl_other_rev"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Miscellaneous_revenue Dataframe 

# COMMAND ----------

df_misc_rev = spark.sql(f"""
                         select gh.fisc_per, sum(items.amount_lc) as misc_revenue
                            from {silver_schema}.ecc.gl_items items
                            inner join {silver_schema}.ecc.gl_header gh
                            on items.acc_docnum = gh.acc_docnum and items.comp_code = gh.comp_code
                            and items.fisc_year = gh.fisc_year
                            inner join {silver_schema}.sharepoint.gl_mapping gl
                            on items.gl_account = gl.gl_account
                            where gh.fisc_per >= {v_start_fisc_per} and gh.fisc_per <= {v_end_fisc_per}
                            and gl.pnlcomponent = 'Other Miscellaneous Revenue'
                            group by fisc_per
                        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discount Not Used Dataframe

# COMMAND ----------

df_dnu = spark.sql(f"""
                         select gh.fisc_per, sum(items.amount_lc) as discounts_not_used
                            from {silver_schema}.ecc.gl_items items
                            inner join {silver_schema}.ecc.gl_header gh
                            on items.acc_docnum = gh.acc_docnum and items.comp_code = gh.comp_code
                            and items.fisc_year = gh.fisc_year
                            inner join {silver_schema}.sharepoint.gl_mapping gl
                            on items.gl_account = gl.gl_account
                            where gh.fisc_per >= {v_start_fisc_per} and gh.fisc_per <= {v_end_fisc_per}
                            and gl.pnlcomponent = 'Discounts not used'
                            group by fisc_per
                        
                        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### NSR Incidence Hist Dataframe

# COMMAND ----------

df_nsr_insidence_hist = spark.sql(f"""
                                  select * from {silver_schema}.files.2023_2024_nsr_for_incidence
                                  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vol Mix Salesgrpcustmat Dataframe

# COMMAND ----------

df_vol_mix_salesgrpcustmat = spark.sql(f"""
                                       select * from {gold_schema}.profitability.vol_mix_salesgrpcustmat
                                       where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                                       """)

# COMMAND ----------

nsr = df_nsr_insidence_hist.alias('nsr')
vm = df_vol_mix_salesgrpcustmat.alias('vm')

# COMMAND ----------

# MAGIC %md
# MAGIC ### DFR From RMS Dataframe

# COMMAND ----------

df_from_rms = spark.sql(f"""
                        select bi.fisc_per, bi.material, bi.outlet ,bc.bill_docnum,bc.condition_type, sum(bc.condition_val) as rms_amt 
                        from dev_silver.bw.billing_conditions bc inner join dev_silver.bw.billing_items bi
                        on bc.bill_docnum = bi.bill_docnum 
                        and bc.bill_itemnum = bi.bill_itemnum 
                        where bc.condition_type IN {df_from_rms_condition_types}
                        and bi.posting_status = '{df_on_inv_disc_bc_posting_status}' 
                        and bi.fisc_per >= {v_start_fisc_per} AND bi.fisc_per <= {v_end_fisc_per}
                        GROUP BY bi.fisc_per, bi.material, bi.outlet ,bc.bill_docnum,bc.condition_type
                        """)

# COMMAND ----------

df_from_rms = df_from_rms.groupBy("bill_docnum", "material").agg(sum("rms_amt")) 

# COMMAND ----------

df_haulage_rates = spark.sql(f"""
                             select fisc_year,del_loc_code,pack,pack_size,quantity as quantity_haulage,haulage_cost_pc from dev_silver.sharepoint.haulage_costs hc where fisc_year in(select distinct year from df_per_v)
                             """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculation Methods

# COMMAND ----------

df_from_rms = spark.sql(f"""
                        select bi.fisc_per, bi.material, bi.outlet ,bc.bill_docnum,bc.condition_type, sum(bc.condition_val) as rms_amt 
                        from dev_silver.bw.billing_conditions bc inner join dev_silver.bw.billing_items bi
                        on bc.bill_docnum = bi.bill_docnum 
                        and bc.bill_itemnum = bi.bill_itemnum 
                        where bc.condition_type IN {df_from_rms_condition_types}
                        and bi.posting_status = '{df_from_rms_posting_status}' 
                        and bi.fisc_per >= {v_start_fisc_per} AND bi.fisc_per <= {v_end_fisc_per}
                        GROUP BY bi.fisc_per, bi.material, bi.outlet ,bc.bill_docnum,bc.condition_type
                        """)

# COMMAND ----------

dfr_rms_total = df_from_rms.groupBy("bill_docnum", "material").agg(sum("rms_amt").alias('dfr_rms_amnt'))

# COMMAND ----------

df_var_disc_rms = (
    df_from_rms.filter(col("condition_type").isin('YRBA', 'YRB0', 'YRB2', 'YRBI', 'YRBJ', 
        'YRBK', 'YRB4', 'YRB6', 'YRB1', 'YRBC', 
        'YRBM', 'YRBS', 'YRBO', 'YRBR', 'YRBP', 'YRBQ'))
    .groupBy('bill_docnum', 'material')
    .agg(sum('rms_amt').alias('var_disc_rms_amt'))
    )

# COMMAND ----------

df_dmi_rms = (
    df_from_rms.filter(col("condition_type").isin(
        'YRB3', 'YRB5', 'YRBB', 'YRBL', 'YRBT', 'YRBN'))
    .groupBy('bill_docnum', 'material')
    .agg(sum('rms_amt').alias('dmi_rms_amt'))
    )

# COMMAND ----------

df_rev_adj = spark.sql(f"""
                   select adj.fisc_per, pc.trade_grp, sum(adj.amnt_gross_rev) * (-1) as revenue_adj
                   from dev_silver.files.revenue_adjustments adj
                   left outer join dev_silver.sharepoint.pc_mapping pc on
                   adj.profit_center = pc.profit_center
                   where adj.fisc_per >= {v_start_fisc_per} and adj.fisc_per <= {v_end_fisc_per}
                   group by all
                   """)

# COMMAND ----------

df_adj = spark.sql(f"""
SELECT 
    adj.fisc_per, 
    pc.trade_grp,
    SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN -1 * adj.amnt_total_disc ELSE 0 END) AS dfr_fixed_disc,
    SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN -1 * adj.amnt_total_disc ELSE 0 END) AS dfr_variable_disc,
    SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN -1 * adj.amnt_total_disc ELSE 0 END) AS dfr_pickup_disc,
    SUM(CASE WHEN disc_type = 'DFR Promo' THEN -1 * adj.amnt_total_disc ELSE 0 END) AS dfr_promo_disc,
    SUM(CASE WHEN disc_type = 'DFR DMI' THEN -1 * adj.amnt_total_disc ELSE 0 END) AS dfr_dmi
FROM 
    dev_silver.files.revenue_adjustments adj
LEFT OUTER JOIN 
    dev_silver.sharepoint.pc_mapping pc
ON 
    adj.profit_center = pc.profit_center
where adj.fisc_per >= {v_start_fisc_per} AND adj.fisc_per <= {v_end_fisc_per}
group by all
                  """)

# COMMAND ----------

df_curr_outlet = spark.sql("select CustomerNo,RSM_MTMCutDesc from dev_gold.common_dim.current_outlets")

# COMMAND ----------

df_material = spark.sql("select Quantity, PackSize, Package, MaterialNo from dev_gold.common_dim.current_materials")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Processing for Reusable Measures

# COMMAND ----------

#Joining condition
cond_copa_custmat = [df_result.fisc_per == df_vol_mix_custmat.fisc_per,df_result.customer == df_vol_mix_custmat.customer, df_result.material == df_vol_mix_custmat.material]

# COMMAND ----------



# COMMAND ----------

Window_spec_LVM = Window.partitionBy('fisc_per', 'customer', 'material')

# COMMAND ----------

df_result_custmat = df_result.join(df_vol_mix_custmat, cond_copa_custmat ,'left').drop(df_vol_mix_custmat["material"],df_vol_mix_custmat["customer"],df_vol_mix_custmat["fisc_per"])
## STD COGS ##                           
df_final_0 = df_result_custmat\
    .join(df_std_cogs, [(df_result_custmat.material == df_std_cogs.material) & (df_result_custmat.fisc_per == df_std_cogs.fisc_per) & (df_result_custmat.fisc_year == df_std_cogs.fisc_year)] ,how='left_outer').drop(df_std_cogs["material"], df_std_cogs["fisc_year"],df_std_cogs["fisc_per"])

## FISC PERIOD ##
df_final_1 =  df_final_0.filter(~col('material').isin('109580')) \
        .withColumn("custmat_vol_uc", sum(col("sum_physical_case_qty_per_line")*col("conv_to_unit_case")*col("conv_full_case")).over(Window_spec_LVM)) \
        .withColumn("custmat_count", count("*").over(Window_spec_LVM)) \
        .withColumn("line_vol_mix", when(col("custmat_vol_uc") != 0, ((col("sum_physical_case_qty_per_line") * col("conv_to_unit_case") * col("conv_full_case"))/ col("custmat_vol_uc")).cast('decimal(38,18)')).otherwise((1/col("custmat_count")).cast('decimal(38,18)')))


## YLP2 ##
if "sales_itemnum" in df_ylp2.columns:
    df_final_2 = df_final_1.join(
        df_ylp2,
        (
            # Condition 1: For fisc_per between 2023001 and 2024003
            (
                (df_final_1["fisc_per"] >= '2023001') &
                (df_final_1["fisc_per"] <= '2024003') &
                (df_final_1["fisc_per"] == df_ylp2["fisc_per"]) &
                (df_final_1["ref_docnum"] == df_ylp2["bill_docnum"]) &
                (df_final_1["material"] == df_ylp2["material"]) &
                (df_final_1["customer"] == df_ylp2["outlet"]) &
                (df_final_1["sales_docnum"] == df_ylp2["sales_docnum"]) &
                (df_final_1["plant"] == df_ylp2["plant"]) &
                (df_final_1["profit_center"] == df_ylp2["profit_center"])
            )
            |
            # Condition 2: For fisc_per >= 2024004
            (
                (df_final_1["fisc_per"] >= '2024004') &
                (df_final_1["fisc_per"] == df_ylp2["fisc_per"]) &
                (df_final_1["ref_docnum"] == df_ylp2["bill_docnum"]) &
                (df_final_1["material"] == df_ylp2["material"]) &
                (df_final_1["customer"] == df_ylp2["outlet"]) &
                (df_final_1["sales_docnum"] == df_ylp2["sales_docnum"]) &
                (df_final_1["plant"] == df_ylp2["plant"]) &
                (df_final_1["profit_center"] == df_ylp2["profit_center"]) &
                (df_final_1["sales_itemnum"] == df_ylp2["sales_itemnum"])
            )
        ),
        how="left_outer"
    ).drop(
        df_ylp2["fisc_per"],
        df_ylp2["material"],
        df_ylp2["bill_docnum"],
        df_ylp2["sales_docnum"],
        df_ylp2["sales_itemnum"],
        df_ylp2["plant"],
        df_ylp2["profit_center"]
    )
else:
    # Perform the join without including sales_itemnum
    df_final_2 = df_final_1.join(
        df_ylp2,
        (
            (df_final_1["fisc_per"] >= '2023001') &
            (df_final_1["fisc_per"] <= '2024003') &
            (df_final_1["fisc_per"] == df_ylp2["fisc_per"]) &
            (df_final_1["ref_docnum"] == df_ylp2["bill_docnum"]) &
            (df_final_1["material"] == df_ylp2["material"]) &
            (df_final_1["customer"] == df_ylp2["outlet"]) &
            (df_final_1["sales_docnum"] == df_ylp2["sales_docnum"]) &
            (df_final_1["plant"] == df_ylp2["plant"]) &
            (df_final_1["profit_center"] == df_ylp2["profit_center"])
        ),
        how="left_outer"
    ).drop(
        df_ylp2["fisc_per"],
        df_ylp2["material"],
        df_ylp2["bill_docnum"],
        df_ylp2["sales_docnum"],
        df_ylp2["plant"],
        df_ylp2["profit_center"]
    )

## FIXED DISC ON INV ##

if "sales_itemnum" in df_fixed_disc_oninv.columns:
    df_final_3 = df_final_2.join(
        df_fixed_disc_oninv,
        (
            # Condition 1: For fisc_per between 2023001 and 2024003
            (
                (df_final_2["fisc_per"] >= '2023001') &
                (df_final_2["fisc_per"] <= '2024003') &
                (df_final_2["fisc_per"] == df_fixed_disc_oninv["fisc_per"]) &
                (df_final_2["ref_docnum"] == df_fixed_disc_oninv["bill_docnum"]) &
                (df_final_2["material"] == df_fixed_disc_oninv["material"]) &
                (df_final_2["customer"] == df_fixed_disc_oninv["outlet"]) &
                (df_final_2["sales_docnum"] == df_fixed_disc_oninv["sales_docnum"]) &
                (df_final_2["plant"] == df_fixed_disc_oninv["plant"]) &
                (df_final_2["profit_center"] == df_fixed_disc_oninv["profit_center"])
            )
            |
            # Condition 2: For fisc_per >= 2024004
            (
                (df_final_2["fisc_per"] >= '2024004') &
                (df_final_2["fisc_per"] == df_fixed_disc_oninv["fisc_per"]) &
                (df_final_2["ref_docnum"] == df_fixed_disc_oninv["bill_docnum"]) &
                (df_final_2["material"] == df_fixed_disc_oninv["material"]) &
                (df_final_2["customer"] == df_fixed_disc_oninv["outlet"]) &
                (df_final_2["sales_docnum"] == df_fixed_disc_oninv["sales_docnum"]) &
                (df_final_2["plant"] == df_fixed_disc_oninv["plant"]) &
                (df_final_2["profit_center"] == df_fixed_disc_oninv["profit_center"]) &
                (df_final_2["sales_itemnum"] == df_fixed_disc_oninv["sales_itemnum"])
            )
        ),
        how="left_outer"
    ).drop(
        df_fixed_disc_oninv["fisc_per"],
        df_fixed_disc_oninv["material"],
        df_fixed_disc_oninv["bill_docnum"],
        df_fixed_disc_oninv["sales_docnum"],
        df_fixed_disc_oninv["sales_itemnum"],
        df_fixed_disc_oninv["plant"],
        df_fixed_disc_oninv["profit_center"],
        df_fixed_disc_oninv["outlet"]
    )
else:
    # Perform the join without including sales_itemnum
    df_final_3 = df_final_2.join(
        df_fixed_disc_oninv,
        (
            (df_final_2["fisc_per"] >= '2023001') &
            (df_final_2["fisc_per"] <= '2024003') &
            (df_final_2["fisc_per"] == df_fixed_disc_oninv["fisc_per"]) &
            (df_final_2["ref_docnum"] == df_fixed_disc_oninv["bill_docnum"]) &
            (df_final_2["material"] == df_fixed_disc_oninv["material"]) &
            (df_final_2["customer"] == df_fixed_disc_oninv["outlet"]) &
            (df_final_2["sales_docnum"] == df_fixed_disc_oninv["sales_docnum"]) &
            (df_final_2["plant"] == df_fixed_disc_oninv["plant"]) &
            (df_final_2["profit_center"] == df_fixed_disc_oninv["profit_center"])
        ),
        how="left_outer"
    ).drop(
        df_fixed_disc_oninv["fisc_per"],
        df_fixed_disc_oninv["material"],
        df_fixed_disc_oninv["bill_docnum"],
        df_fixed_disc_oninv["sales_docnum"],
        df_fixed_disc_oninv["plant"],
        df_fixed_disc_oninv["profit_center"],
        df_fixed_disc_oninv["outlet"]
    )

## VAR DISC ON INV ##

if "sales_itemnum" in df_var_disc_oninv.columns:
    df_final_4 = df_final_3.join(
        df_var_disc_oninv,
        (
            # Condition 1: For fisc_per between 2023001 and 2024003
            (
                (df_final_3["fisc_per"] >= '2023001') &
                (df_final_3["fisc_per"] <= '2024003') &
                (df_final_3["fisc_per"] == df_var_disc_oninv["fisc_per"]) &
                (df_final_3["ref_docnum"] == df_var_disc_oninv["bill_docnum"]) &
                (df_final_3["material"] == df_var_disc_oninv["material"]) &
                (df_final_3["customer"] == df_var_disc_oninv["outlet"]) &
                (df_final_3["sales_docnum"] == df_var_disc_oninv["sales_docnum"]) &
                (df_final_3["plant"] == df_var_disc_oninv["plant"]) &
                (df_final_3["profit_center"] == df_var_disc_oninv["profit_center"])
            )
            |
            # Condition 2: For fisc_per >= 2024004
            (
                (df_final_3["fisc_per"] >= '2024004') &
                (df_final_3["fisc_per"] == df_var_disc_oninv["fisc_per"]) &
                (df_final_3["ref_docnum"] == df_var_disc_oninv["bill_docnum"]) &
                (df_final_3["material"] == df_var_disc_oninv["material"]) &
                (df_final_3["customer"] == df_var_disc_oninv["outlet"]) &
                (df_final_3["sales_docnum"] == df_var_disc_oninv["sales_docnum"]) &
                (df_final_3["plant"] == df_var_disc_oninv["plant"]) &
                (df_final_3["profit_center"] == df_var_disc_oninv["profit_center"]) &
                (df_final_3["sales_itemnum"] == df_var_disc_oninv["sales_itemnum"])
            )
        ),
        how="left_outer"
    ).drop(
        df_var_disc_oninv["fisc_per"],
        df_var_disc_oninv["material"],
        df_var_disc_oninv["bill_docnum"],
        df_var_disc_oninv["sales_docnum"],
        df_var_disc_oninv["sales_itemnum"],
        df_var_disc_oninv["plant"],
        df_var_disc_oninv["profit_center"],
        df_var_disc_oninv["outlet"]
    )
else:
    # Perform the join without including sales_itemnum
    df_final_4 = df_final_3.join(
        df_var_disc_oninv,
        (
            (df_final_3["fisc_per"] >= '2023001') &
            (df_final_3["fisc_per"] <= '2024003') &
            (df_final_3["fisc_per"] == df_var_disc_oninv["fisc_per"]) &
            (df_final_3["ref_docnum"] == df_var_disc_oninv["bill_docnum"]) &
            (df_final_3["material"] == df_var_disc_oninv["material"]) &
            (df_final_3["customer"] == df_var_disc_oninv["outlet"]) &
            (df_final_3["sales_docnum"] == df_var_disc_oninv["sales_docnum"]) &
            (df_final_3["plant"] == df_var_disc_oninv["plant"]) &
            (df_final_3["profit_center"] == df_var_disc_oninv["profit_center"])
        ),
        how="left_outer"
    ).drop(
        df_var_disc_oninv["fisc_per"],
        df_var_disc_oninv["material"],
        df_var_disc_oninv["bill_docnum"],
        df_var_disc_oninv["sales_docnum"],
        df_var_disc_oninv["plant"],
        df_var_disc_oninv["profit_center"],
        df_var_disc_oninv["outlet"]
    )

## PICKUP DISC ON INV ##

if "sales_itemnum" in df_pickup_disc.columns:
    df_final_5 = df_final_4.join(
        df_pickup_disc,
        (
            # Condition 1: For fisc_per between 2023001 and 2024003
            (
                (df_final_4["fisc_per"] >= '2023001') &
                (df_final_4["fisc_per"] <= '2024003') &
                (df_final_4["fisc_per"] == df_pickup_disc["fisc_per"]) &
                (df_final_4["ref_docnum"] == df_pickup_disc["bill_docnum"]) &
                (df_final_4["material"] == df_pickup_disc["material"]) &
                (df_final_4["customer"] == df_pickup_disc["outlet"]) &
                (df_final_4["sales_docnum"] == df_pickup_disc["sales_docnum"]) &
                (df_final_4["plant"] == df_pickup_disc["plant"]) &
                (df_final_4["profit_center"] == df_pickup_disc["profit_center"])
            )
            |
            # Condition 2: For fisc_per >= 2024004
            (
                (df_final_4["fisc_per"] >= '2024004') &
                (df_final_4["fisc_per"] == df_pickup_disc["fisc_per"]) &
                (df_final_4["ref_docnum"] == df_pickup_disc["bill_docnum"]) &
                (df_final_4["material"] == df_pickup_disc["material"]) &
                (df_final_4["customer"] == df_pickup_disc["outlet"]) &
                (df_final_4["sales_docnum"] == df_pickup_disc["sales_docnum"]) &
                (df_final_4["plant"] == df_pickup_disc["plant"]) &
                (df_final_4["profit_center"] == df_pickup_disc["profit_center"]) &
                (df_final_4["sales_itemnum"] == df_pickup_disc["sales_itemnum"])
            )
        ),
        how="left_outer"
    ).drop(
        df_pickup_disc["fisc_per"],
        df_pickup_disc["material"],
        df_pickup_disc["bill_docnum"],
        df_pickup_disc["sales_docnum"],
        df_pickup_disc["sales_itemnum"],
        df_pickup_disc["plant"],
        df_pickup_disc["profit_center"],
        df_pickup_disc["outlet"]
    )
else:
    # Perform the join without including sales_itemnum
    df_final_5 = df_final_4.join(
        df_pickup_disc,
        (
            (df_final_4["fisc_per"] >= '2023001') &
            (df_final_4["fisc_per"] <= '2024003') &
            (df_final_4["fisc_per"] == df_pickup_disc["fisc_per"]) &
            (df_final_4["ref_docnum"] == df_pickup_disc["bill_docnum"]) &
            (df_final_4["material"] == df_pickup_disc["material"]) &
            (df_final_4["customer"] == df_pickup_disc["outlet"]) &
            (df_final_4["sales_docnum"] == df_pickup_disc["sales_docnum"]) &
            (df_final_4["plant"] == df_pickup_disc["plant"]) &
            (df_final_4["profit_center"] == df_pickup_disc["profit_center"])
        ),
        how="left_outer"
    ).drop(
        df_pickup_disc["fisc_per"],
        df_pickup_disc["material"],
        df_pickup_disc["bill_docnum"],
        df_pickup_disc["sales_docnum"],
        df_pickup_disc["plant"],
        df_pickup_disc["profit_center"],
        df_pickup_disc["outlet"]
    )

## CR DR MEMOS CUR ##

df_final_6 = df_final_5.join(df_cr_dr_memos_cur,[(df_final_5["fisc_per"]== df_cr_dr_memos_cur["fisc_per"]) & (df_final_5["customer"]== df_cr_dr_memos_cur["customer"])], how='left_outer').drop(df_cr_dr_memos_cur["fisc_per"], df_cr_dr_memos_cur["customer"])


## DFR RMS TOTAL ##
df_final_7 = df_final_6.join(dfr_rms_total, (df_final_6["ref_docnum"]==dfr_rms_total["bill_docnum"])&(df_final_6["material"]==dfr_rms_total["material"]), how = 'left_outer').drop(dfr_rms_total["bill_docnum"], dfr_rms_total["material"])


## VAR DISC RMS ##
df_final_8 = df_final_7.join(df_var_disc_rms , (df_final_7["ref_docnum"]==df_var_disc_rms["bill_docnum"])&(df_final_7["material"]==df_var_disc_rms["material"]), how = 'left_outer').drop(df_var_disc_rms["bill_docnum"], df_var_disc_rms["material"])


## DMI RMS ##
df_final_9 = df_final_8.join(df_dmi_rms, (df_final_8["ref_docnum"]==df_dmi_rms["bill_docnum"])&(df_final_8["material"]==df_dmi_rms["material"]), how = 'left_outer').drop(df_dmi_rms["bill_docnum"], df_dmi_rms["material"])

## Current Outlet ##
df_final_10 = df_final_9.join(df_curr_outlet, (df_final_9["customer"] == df_curr_outlet["CustomerNo"]), how = 'left_outer').drop(df_curr_outlet["CustomerNo"])

## Material ##
df_final = df_final_10.join(df_material, (df_final_10["material"] == df_material["MaterialNo"]), how='left_outer').drop(df_material["MaterialNo"])


# COMMAND ----------

df_cdo = spark.sql(f""" SELECT * from dev_gold.profitability.cdo_monthly""")
df_cdo = df_cdo.groupBy("fisc_per", "customer").agg(sum('cdo_cost').alias('cold_drink_operations'))

# COMMAND ----------

df_final = df_final.unionByName(df_rev_adj, allowMissingColumns=True).unionByName(df_adj, allowMissingColumns=True).unionByName(df_gl_other_rev,  allowMissingColumns=True).unionByName(df_gl_ctm, allowMissingColumns=True).unionByName(df_dnu,allowMissingColumns=True).unionByName(df_misc_rev, allowMissingColumns=True).unionByName(df_ipp, allowMissingColumns=True).unionByName(df_gl_dfr_curve, allowMissingColumns=True).unionByName(df_cdo,allowMissingColumns=True).unionByName(df_cr_dr_memos_hist, allowMissingColumns=True).unionByName(df_off_inv_disc, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume

# COMMAND ----------

df_final = df_final.withColumn("mktg_vol_uc", when(~col("material").isin('101209', '101210', '109800'),(col("sum_physical_case_qty_per_line")*col("conv_to_unit_case")*col("conv_full_case"))).cast('decimal(38,18)')) \
                   .withColumn("mktg_vol_pc", col("sum_physical_case_qty_per_line").cast('decimal(38,18)')) \
                   .withColumn("mktg_vol_fpc",when(~col("material").isin('101209', '101210', '109800'),(col("sum_physical_case_qty_per_line") * col("conv_full_case"))).cast('decimal(38,18)')) \
                   .withColumn("free_goods_vol_uc", (col("free_goods_sum_physical_case_qty_per_line")*col("conv_to_unit_case") * col("conv_full_case")).cast("decimal(38,18)")) \
                   .withColumn("free_goods_vol_pc", col('free_goods_sum_physical_case_qty_per_line').cast('decimal(38,18)')) \
                   .withColumn("free_goods_vol_fpc",(col("free_goods_sum_physical_case_qty_per_line") * col("conv_full_case")).cast("decimal(38,18)")) \
                   .withColumn("financial_vol_uc", (col("mktg_vol_uc")) - (col("free_goods_vol_uc")).cast('decimal(25,2)')) \
                   .withColumn("financial_vol_pc", (col("mktg_vol_pc") - col("free_goods_vol_pc")).cast('decimal(25,2)')) \
                   .withColumn("financial_vol_fpc", (col("mktg_vol_fpc") - col("free_goods_vol_fpc")).cast('decimal(25,2)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gross Revenue

# COMMAND ----------

df_final = df_final.withColumn("base_price_adj", (col("ylp2_amount")/vat).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price_excl_excise", (col("sum_net_share") - col("base_price_adj")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("excise_from_copa", col("sum_reserve_92").cast('decimal(25,2)')).na.fill(0.00) \
                   .withColumn("Excise_Tax_Std", (col("financial_vol_pc") * col("excise_tax_pc")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price", ((col("base_price_excl_excise")) + col("excise_from_copa") + col("revenue_adj")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn('ipp_discount',  (col("amount_ipp")*-1).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price_adj_w_ipp", (col("base_price_adj") + col("ipp_discount")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("gross_revenue", (col("base_price") + col("base_price_adj_w_ipp")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("gross_wholesale_price", (col("gross_revenue")*vat).cast('decimal(25,5)')).na.fill(0.00)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## On Invoice Discounts

# COMMAND ----------

df_final = df_final.withColumn("fixed_disc_oninv", col("total_amount").cast('decimal(38,18)')) \
                   .withColumn("variable_disc_oninv", col('var_disc_amount').cast('decimal(38,18)')) \
                   .withColumn("pickup_disc_oninv", (col("pickup_disc_amount")).cast('decimal(38,18)')) \
                   .withColumn("promo_disc_oninv", (col("sum_trade_promo")).cast('decimal(38,18)')) \
                   .withColumn("total_disc_oninv",(col('fixed_disc_oninv')+col('variable_disc_oninv') + col('promo_disc_oninv') + col('pickup_disc_oninv')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Off Invoice Discounts

# COMMAND ----------


df_final = df_final.withColumn("dfr_cr_dr_memos", col("total_cr_dr_amnt_hist")+(col("line_vol_mix")*col("total_cr_dr_amnt_cur")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_fixed_disc", (col("fixed_amnt_off_inv")+col("fixed_amnt_cr_dr_hist")+(col("line_vol_mix")*col("fixed_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')).na.fill(0.00)\
                   .withColumn("dfr_from_rms", col("dfr_rms_amnt").cast('decimal(25,3)')).na.fill(0.00) \
                   .withColumn("dfr_variable_disc", (col("var_amnt_off_inv")+ col("var_amnt_cr_dr_hist")+(col("line_vol_mix")*col("var_amnt_cr_dr_memos_cur"))+col("var_disc_rms_amt")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_pickup_disc",(col("pickup_amnt_off_inv")+col("pickup_amnt_cr_dr_hist")+(col("line_vol_mix")*col("pickup_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')) \
                   .withColumn("dfr_promo_disc",(col("promo_amnt_off_inv")+col("promo_amnt_cr_dr_hist")+(col("line_vol_mix")*col("promo_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_del_pen_disc",(col("del_pen_amnt_off_inv")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_fallout",(col("dfr_fallout_amnt_off_inv")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_dmi",(col("dmi_amnt_off_inv")-col("dmi_rms_amt")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_dmi_fallout",col("dmi_fallout_amnt_off_inv").cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_ctm", (col("amount_gl_ctm")*-1).cast('decimal(25,2)')).na.fill(0.00) \
                   .withColumn("dfr_sales_curve", (col('dfr_curve_amnt') * (-1)).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("total_disc_offinv", (col('dfr_fixed_disc')-col('dfr_from_rms')+col('dfr_variable_disc')+col('dfr_pickup_disc')+col('dfr_promo_disc')+col('dfr_del_pen_disc')+col('dfr_fallout')+col('dfr_dmi')+col('dfr_dmi_fallout')+col('dfr_ctm')+col('dfr_sales_curve'))).na.fill(0.00)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Other Revenue

# COMMAND ----------

df_final = df_final.withColumn("ipp_equip_rental",col("sum_other_surcharge").cast('decimal(25,2)')) \
                   .withColumn("ipp_freight_charge",col("sum_fuel_surcharge").cast('decimal(25,2)')) \
                   .withColumn("other_rev_cust", (col("ipp_equip_rental") + col("ipp_freight_charge")).cast('decimal(25,2)'))\
                   .withColumn("other_rev", (col('amount_gl_other_rev')*-1).cast('decimal(25,2)')) \
                   .withColumn("other_rev_non_cust", (col('other_rev')-col('other_rev_cust')).cast('decimal(25,2)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discounts

# COMMAND ----------

df_final = df_final.withColumn("total_discounts", (col("total_disc_oninv")+col("total_disc_offinv")).cast('decimal(25,2)'))\
                   .withColumn("nsr_after_curve", (col("gross_revenue")-col("total_discounts")).cast('decimal(25,2)'))\
                   .withColumn("nsr_before_curve", (col("gross_revenue")-col("total_discounts")+col("dfr_sales_curve")).cast('decimal(25,2)'))\
                   .withColumn("revenue_after_curve", (col("gross_revenue")-col("total_discounts")+col("other_rev")).cast('decimal(25,2)'))\
                   .withColumn("revenue_before_curve", (col("revenue_after_curve")-col("dfr_sales_curve")))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## COGS for Profitability P&L

# COMMAND ----------

## Haulage Costs ##
df_final = df_final.join(df_haulage_rates, (df_final["plant"]==df_haulage_rates["del_loc_code"])&(df_final["Package"]==df_haulage_rates["pack"])&(df_final["PackSize"]==upper(df_haulage_rates["pack_size"]))&(df_final["Quantity"]==df_haulage_rates["quantity_haulage"])&(df_final["fisc_year"]==df_haulage_rates["fisc_year"]), how = 'left_outer').drop(df_haulage_rates["fisc_year"])

# COMMAND ----------

df_vol_mix_mat = spark.sql(f""" select *, financial_vol_pc as financial_vol_pc_vmm, financial_vol_uc as financial_vol_uc_vmm from {gold_schema}.profitability.vol_mix_mat""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

df_final = df_final.join(df_vol_mix_mat, (df_final["fisc_per"]==df_vol_mix_mat["fisc_per"])&(df_final["material"]==df_vol_mix_mat["material"]), how = 'left_outer').drop(df_vol_mix_mat["fisc_per"], df_vol_mix_mat["material"],df_vol_mix_mat["financial_vol_pc"], df_vol_mix_mat["financial_vol_uc"])

# COMMAND ----------



# COMMAND ----------

df_haulage = spark.sql(f"""
                       with haulage_costs as(
                           select h.fisc_per, sum(i.balance) haulage_balance from
                           {silver_schema}.ecc.gl_items i
                           inner join {silver_schema}.ecc.gl_header h
                           on i.fisc_year = h.fisc_year and i.acc_docnum = h.acc_docnum
                           where h.fisc_per >= {v_start_fisc_per} and h.fisc_per <= {v_end_fisc_per}
                           and i.gl_account in {df_haulage_gl_accounts} and i.comp_code = '{df_haulage_comp_code}'
                           group by h.fisc_per
                       ),
                       T1_opex as (
                         select h.fisc_per, sum(i.balance) t1_opex_balance from
                         {silver_schema}.ecc.gl_items i
                         inner join {silver_schema}.ecc.gl_header h
                         ON 
                         i.fisc_year = h.fisc_year 
                         AND i.acc_docnum = h.acc_docnum
                         where h.fisc_per >= {v_start_fisc_per} and h.fisc_per <= {v_end_fisc_per}
                         and cast(i.gl_account as int) >= 800000000
                         and cast(i.gl_account as int) <= 900000000
                         and i.gl_account not in {df_t1_opex_gl_accounts} and i.func_area = 'B32'
                         and i.profit_center in {df_t1_opex_profit_centres}
                         and h.comp_code = '{df_t1_opex_comp_code}'
                         group by h.fisc_per
                       )
                       select haulage_costs.fisc_per, (haulage_costs.haulage_balance + T1_opex.t1_opex_balance) as total_costs
                       from haulage_costs
                       inner join T1_opex
                       on haulage_costs.fisc_per = T1_opex.fisc_per
                       """)

# COMMAND ----------

df_final = df_final\
                    .withColumn("nka_discount", when(col("FISC_PER") < '2024001',when(col("RSM_MTMCutDesc").contains("NKA"),col("total_disc_oninv") - col("dfr_fixed_disc") - col("dfr_variable_disc") - col("dfr_pickup_disc") - col("dfr_promo_disc")).otherwise(0)).otherwise(0))\
                    .withColumn("nsr_base_incidence",when(col("FISC_PER") < '2024001',col("gross_revenue") - col("Excise_Tax_Std") - (0.093 * (col("gross_revenue") - col("Excise_Tax_Std"))) - col("nka_discount")) 
                    .otherwise(col("gross_revenue") - col("Excise_Tax_Std") - when(col("total_disc_oninv") < 0, -col("total_disc_oninv")).otherwise(col("total_disc_oninv")) - when(col("dfr_fixed_disc") < 0, -col("dfr_fixed_disc")).otherwise(col("dfr_fixed_disc")) - when(col("dfr_variable_disc") < 0, -col("dfr_variable_disc")).otherwise(col("dfr_variable_disc")) - when(col("dfr_pickup_disc") < 0, -col("dfr_pickup_disc")).otherwise(col("dfr_pickup_disc"))- when(col("dfr_promo_disc") < 0, -col("dfr_promo_disc")).otherwise(col("dfr_promo_disc"))).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate", (when((col("incidence_rate_pc")==0),(col("concentrate_pc")+col("management_fee_pc"))*col("financial_vol_pc")).otherwise(col("nsr_base_incidence")*col("incidence_rate_pc"))).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_tccc", col("sum_cogs_conctr_tccc").cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_nontccc", col("sum_cogs_conctr_nontccc").cast('decimal(38,18)'))\
                    .withColumn("Commodities_Std_Cost",(col("commodities_cogs_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))\
                    .withColumn("Conversion_Std_Cost", (col("conversion_cost_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_under_over", (df_vol_mix_mat.total_cogs_over_under_rate * df_final.financial_vol_pc).cast('decimal(38,18)')).na.fill(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Haulage Std Cost

# COMMAND ----------

Window_spec_hlv_dlv_whse_total = Window.partitionBy('fisc_per')

# COMMAND ----------

df_final = df_final.join(df_haulage, (df_final["fisc_per"]==df_haulage["fisc_per"]), how='left_outer').drop(df_haulage["fisc_per"])

# COMMAND ----------

df_final = df_final.withColumn("haulage_std", (col("mktg_vol_pc")*col("haulage_cost_pc")).cast('decimal(38,18)'))\
                    .withColumn("total_haulage_std", (sum("haulage_std").over(Window_spec_hlv_dlv_whse_total)))\
                    .withColumn("haulage_mix", col("haulage_std")/col("total_haulage_std"))\
                    .withColumn("haulage_total", (col("total_costs")*col("haulage_mix")).cast('decimal(38,18)'))\
                    .withColumn("total_cogs", (col("cogs_concentrate")+col("Commodities_Std_Cost")+col("Excise_Tax_Std")+col("Conversion_Std_Cost")+col("cogs_under_over")+col("haulage_total")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delivery 

# COMMAND ----------

df_delivery_rates = spark.sql(f"""
                              select fisc_year,del_loc_code,pack,pack_size,quantity as quantity_delivery,delivery_cost_pc from dev_silver.sharepoint.delivery_costs where fisc_year in(select distinct year from df_per_v)
                              """)

# COMMAND ----------

# Delivery Rates
df_final = df_final.join(df_delivery_rates, (df_final["plant"]==df_delivery_rates["del_loc_code"])&(df_final["Package"]==df_delivery_rates["pack"])&(df_final["PackSize"]==upper(df_delivery_rates["pack_size"]))&(df_final["Quantity"]==df_delivery_rates["quantity_delivery"])&(df_final["fisc_year"]==df_delivery_rates["fisc_year"]), how = 'left_outer').drop(df_delivery_rates["del_loc_code"],df_delivery_rates["pack"],df_delivery_rates["pack_size"],df_delivery_rates["quantity_delivery"],df_delivery_rates["fisc_year"])

# COMMAND ----------

df_delivery = df_gl_balances.filter(col("FS_Items")=='Transport').groupBy("fisc_per").agg(sum("balance").alias("total_costs_dlv"))

# COMMAND ----------

df_final = df_final.join(df_delivery, (df_final["fisc_per"]==df_delivery["fisc_per"]), how = 'left_outer').drop(df_delivery["fisc_per"])

# COMMAND ----------

df_final = df_final.withColumn("delivery_std", (col("mktg_vol_pc")*col("delivery_cost_pc"))*(-1)) \
                   .withColumn("total_delivery_std", (sum("delivery_std").over(Window_spec_hlv_dlv_whse_total)))\
                   .withColumn("delivery_mix", (col("delivery_std")/col("total_delivery_std")))\
                   .withColumn("delivery_total", (col("total_costs_dlv")*col("delivery_mix"))*(-1))\
                   .withColumn("delivery_under_over", (col("delivery_total")-col("delivery_std")))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Warehouse

# COMMAND ----------

df_warehouse_rates = spark.sql(f""" select fisc_year,del_loc_code,pack,pack_size, quantity as quantity_warehouse, warehouse_cost_pc from dev_silver.sharepoint.warehouse_costs where fisc_year in(select distinct year from df_per_v)""")

# COMMAND ----------

# Warehouse Rates
df_final = df_final.join(df_warehouse_rates, (df_final["plant"]==df_warehouse_rates["del_loc_code"])&(df_final["Package"]==df_warehouse_rates["pack"])&(df_final["PackSize"]==upper(df_warehouse_rates["pack_size"]))&(df_final["Quantity"]==df_warehouse_rates["quantity_warehouse"])&(df_final["fisc_year"]==df_warehouse_rates["fisc_year"]), how = 'left_outer').drop(df_warehouse_rates["del_loc_code"],df_warehouse_rates["pack"],df_warehouse_rates["pack_size"],df_warehouse_rates["quantity_warehouse"],df_warehouse_rates["fisc_year"])

# COMMAND ----------

df_warehouse = df_gl_balances.filter(col("FS_Items")=='Logistic Operating Expenses (Whse & Distribution)').groupBy("fisc_per").agg(sum("balance").alias("total_costs_warehouse"))

# COMMAND ----------

df_final = df_final.join(broadcast(df_warehouse), (df_final["fisc_per"]==df_warehouse["fisc_per"]), how = 'left_outer').drop(df_warehouse["fisc_per"])

# COMMAND ----------

df_final = df_final.withColumn("warehouse_std", (col("mktg_vol_pc")*col("warehouse_cost_pc"))*(-1)) \
                   .withColumn("total_warehouse_std", (sum("warehouse_std").over(Window_spec_hlv_dlv_whse_total)))\
                   .withColumn("warehouse_mix", (col("warehouse_std")/col("total_warehouse_std")))\
                   .withColumn("warehouse_total", (col("total_costs_warehouse")*col("warehouse_mix"))*(-1))\
                   .withColumn("warehouse_under_over", (col("warehouse_total")-col("warehouse_std")))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Fully Delivered Margin (FDM) 

# COMMAND ----------

# DBTITLE 1,FDM BEFORE CURVE
df_final = df_final.withColumn("fdm_before_curve", col("revenue_before_curve") + col("total_cogs") + col("delivery_total") + col("warehouse_total"))

# COMMAND ----------

# DBTITLE 1,FDM AFTER CURVE
df_final = df_final.withColumn("fdm_after_curve", col("revenue_after_curve") + col("total_cogs") + col("delivery_total") + col("warehouse_total"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistics

# COMMAND ----------

df_final = df_final.withColumn("logistics_opex", (col("warehouse_total")+col("delivery_total")+col("haulage_total")).cast('decimal(38,18)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## COGS for Portfolio P&L

# COMMAND ----------

# DBTITLE 1,cogs for portfolio P&L
df_final = df_final.withColumn("cogs_concentrate_total", (col("cogs_concentrate") + ((col("total_var_concentrate") / col("financial_vol_pc_vmm")) * col("financial_vol_pc"))).cast('decimal(38,18)'))\
.withColumn("cogs_sweetener_total", (col("cogs_rate_sugar_sweetener") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_compounding_total", (col("cogs_rate_compounding") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_his_ace_k_total", (col("cogs_rate_his_ace_k") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_total", (col("cogs_rate_other_ing") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_tea_total", (col("cogs_rate_other_ing_tea") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_sweetener_total", (col("cogs_rate_other_ing_sweetner") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_dairy_total", (col("cogs_rate_other_ing_dairy") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_alcohol_total", (col("cogs_rate_other_ing_alcohol") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_ing_whiskey_total", (col("cogs_rate_other_ing_whiskey") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_resin_total", (col("cogs_rate_resin") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_preform_total", (col("cogs_rate_preform") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pet_bottles_total", (col("cogs_rate_pet_bottles") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_cans_total", (col("cogs_rate_cans") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_closures_crowns_total", (col("cogs_rate_closures_crowns") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_labels_total", (col("cogs_rate_labels") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_packaging_material_corrugate_total", (col("cogs_rate_packaging_material_corrugate") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_stretch_film_total", (col("cogs_rate_stretch_film") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_shrink_film_total", (col("cogs_rate_shrink_film") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_separator_sheets_total", (col("cogs_rate_separator_sheets") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pet_plastic_handles_total", (col("cogs_rate_pet_plastic_handles") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_bags_total", (col("cogs_rate_bags") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_packaging_material_total", (col("cogs_rate_other_packaging_material") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_other_packaging_material_straw_total", (col("cogs_rate_other_packaging_material_straw") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_co2_total", (col("cogs_rate_co2") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_primary_packaging_total", (col("cogs_rate_primary_packaging") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_repacking_total", (col("cogs_rate_repacking") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_packaging_materials_total", (col("cogs_pet_bottles_total") + col("cogs_cans_total") + col("cogs_closures_crowns_total") + col("cogs_labels_total") + col("cogs_packaging_material_corrugate_total") + col("cogs_stretch_film_total") + col("cogs_shrink_film_total") + col("cogs_separator_sheets_total") + col("cogs_pet_plastic_handles_total") + col("cogs_bags_total") + col("cogs_other_packaging_material_total") + col("cogs_other_packaging_material_straw_total") + col("cogs_co2_total") + col("cogs_primary_packaging_total") + col("cogs_repacking_total") + col("cogs_resin_total") + col("cogs_preform_total") ).cast('decimal(38,18)'))\
.withColumn("cogs_commodities_total", (col("cogs_sweetener_total") + col("cogs_compounding_total") + col("cogs_his_ace_k_total") + col("cogs_other_ing_total") + col("cogs_other_ing_tea_total") + col("cogs_other_ing_sweetener_total") + col("cogs_other_ing_dairy_total") + col("cogs_other_ing_alcohol_total") + col("cogs_other_ing_whiskey_total") + col("cogs_packaging_materials_total")).cast('decimal(38,18)'))\
.withColumn("cogs_excise_total", (col("cogs_rate_excise_tax") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_energy_total", (col("cogs_rate_energy") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_tolling_fee_toller_total", (col("cogs_rate_tolling_fee_toller") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_tolling_fee_pet_total", (col("cogs_rate_tolling_fee_pet") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_rm_specific_total", (col("cogs_rate_pack_filling_oh_rm_specific") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_rm_alloc_total", (col("cogs_rate_pack_filling_oh_rm_alloc") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_var_specific_total", (col("cogs_rate_pack_filling_oh_var_specific") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_var_alloc_total", (col("cogs_rate_pack_filling_oh_var_alloc") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_bc_pack_total", (col("cogs_rate_pack_filling_bc_pack") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_distilling_cost_total", (col("cogs_rate_distilling_cost") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_variable_total", (col("cogs_energy_total") + col("cogs_tolling_fee_toller_total") + col("cogs_tolling_fee_pet_total") + col("cogs_pack_filling_oh_rm_specific_total") + col("cogs_pack_filling_oh_rm_alloc_total") + col("cogs_pack_filling_oh_var_specific_total") + col("cogs_pack_filling_oh_var_alloc_total") + col("cogs_pack_filling_bc_pack_total") + col("cogs_distilling_cost_total")).cast('decimal(38,18)'))\
.withColumn("cogs_imp_mother_sku_upl_total", (col("cogs_rate_imp_mother_sku_upl") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_fixed_specific_total", (col("cogs_rate_pack_filling_oh_fixed_specific") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_oh_fixed_alloc_total", (col("cogs_rate_pack_filling_oh_fixed_alloc") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_dep_pack_specific_total", (col("cogs_rate_pack_filling_dep_pack_specific") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_pack_filling_dep_pack_allocated_total", (col("cogs_rate_pack_filling_dep_pack_allocated") * col("financial_vol_pc")).cast('decimal(38,18)'))\
.withColumn("cogs_fixed_total", (col("cogs_pack_filling_oh_fixed_specific_total") + col("cogs_pack_filling_oh_fixed_alloc_total") + col("cogs_pack_filling_dep_pack_specific_total") + col("cogs_pack_filling_dep_pack_allocated_total")).cast('decimal(38,18)'))\
.withColumn("cogs_conversion_cost_total", (col("cogs_variable_total") + col("cogs_fixed_total")).cast('decimal(38,18)'))\
.withColumn("haulage_under_over", (col("haulage_total") - col("haulage_std")).cast('decimal(38,18)'))\
.withColumn("total_cogs_portfolio", (col("cogs_concentrate_total") + col("cogs_commodities_total") + col("cogs_excise_total") + col("cogs_imp_mother_sku_upl_total")+ col("haulage_total") + col("cogs_conversion_cost_total")).cast('decimal(38,18)'))

# COMMAND ----------

display(df_final.groupBy("fisc_per").agg(sum("mktg_vol_pc")))

# COMMAND ----------

# DBTITLE 1,TME
# Step1
df_pc_mapping = spark.table("dev_silver.sharepoint.pc_mapping")
df_joined = df_gl_balances.alias('glb') \
    .join(df_pc_mapping.alias('pcm'), df_gl_balances.profit_center == df_pc_mapping.profit_center, how='left_outer')

df_filtered = df_joined.filter(
    (df_gl_balances.FS_Items == 'Direct Marketing Expenses') &
    (df_gl_balances.gl_account != '894502201') &
    (df_gl_balances.profit_center != 'PH12150121') &
    (df_gl_balances.cost_center != '1215038109')
)

df_tme = df_filtered.groupBy('fisc_per', 'pcm.bus_type_ext') \
    .agg(sum('balance').alias('tme_cost'))

# Step2 
df_tme_alloc = df_vol_mix_salesgrpcustmat.join(df_tme, on=["fisc_per","bus_type_ext"], how="left_outer").withColumn("tme_cost_alloc",  when(col("bus_type_ext") != 'xx', col("tme_cost") * col("mix_gtmt_mktg_uc") ) .otherwise(col("tme_cost") * col("mix_total_mktg_uc"))).select('fisc_per','customer','material','tme_cost_alloc') 


# Step3]
df_final = df_final.join(df_tme_alloc, on=["fisc_per", "customer", "material"], how="left_outer")

df_final = df_final.withColumn("tme",coalesce(col("tme_cost_alloc").cast("double") * col("line_vol_mix").cast("double"),lit(0)))

df_final = df_final.withColumn("tme", (col("tme")).cast('decimal(38,18)'))

# COMMAND ----------

# DBTITLE 1,TME Sales Curve
# Step1
df_filtered_curve = df_gl_balances.filter(
    (df_gl_balances.FS_Items == 'Direct Marketing Expenses') &
    (df_gl_balances.gl_account == '894502201') &
    (df_gl_balances.profit_center == 'PH12150121') &
    (df_gl_balances.cost_center == '1215038109')
)
df_tme_curve = df_filtered_curve.groupBy('fisc_per') \
    .agg(sum('balance').alias('tme_curve_cost'))

# Step2
gtmt_window = Window.partitionBy("fisc_per")

# Filter the data, add 'total_cost' using the window, and calculate 'gtmt_mix'
df_tme_mix = df_tme.filter(col("bus_type_ext") != "xx") \
    .withColumn("total_cost", sum("tme_cost").over(gtmt_window)) \
    .withColumn("gtmt_mix", col("tme_cost") / col("total_cost"))

# Step3
df_tme_curve = df_tme_curve.join(df_tme_mix, on="fisc_per", how="inner") \
    .withColumn("tme_curve_gtmt", col("tme_curve_cost") * col("gtmt_mix"))

# Step4
df_tme_curve_alloc = df_vol_mix_salesgrpcustmat.join(
    df_tme_curve, 
    on=["fisc_per", "bus_type_ext"], 
    how="left_outer"
) 
df_tme_curve_alloc = df_tme_curve_alloc.withColumn("tme_curve_cost_alloc", coalesce(col("tme_curve_gtmt").cast("double") * col("mix_gtmt_mktg_uc").cast("double"),lit(0))).select('fisc_per','customer','material','tme_curve_cost_alloc') 

df_tme_curve_alloc = df_tme_curve_alloc.withColumn("tme_curve_cost_alloc", (col("tme_curve_cost_alloc")).cast('decimal(38,18)'))

df_final = df_final.join(
    df_tme_curve_alloc,
    on=["fisc_per", "customer", "material"],
    how="left_outer"
    )
df_final = df_final.withColumn("tme_curve", coalesce(col("tme_curve_cost_alloc").cast("double") * col("line_vol_mix").cast("double"),lit(0)))
df_final = df_final.withColumn("tme_curve", (col("tme_curve")).cast('decimal(38,18)'))

#df_final.display()

# COMMAND ----------

# DBTITLE 1,Marketing Expense TME
# Create the 'tme_incl_curve' column as the sum of 'tme' and 'tme_curve'
df_final = df_final.withColumn("tme_incl_curve", col("tme") + col("tme_curve"))

# COMMAND ----------

display(df_final.groupBy("fisc_per").agg(sum("mktg_vol_pc")))

# COMMAND ----------

# DBTITLE 1,Selling Expense
# Perform the left outer join between df_gl_balances (glb), cc_mapping (ccm), and pc_mapping (pcm)
df_sellexp = df_gl_balances.alias("glb") \
    .join(spark.table("dev_silver.sharepoint.cc_mapping").alias("ccm"), "cost_center", "left_outer") \
    .join(spark.table("dev_silver.sharepoint.pc_mapping").alias("pcm"), "profit_center", "left_outer") \
    .filter(
        (col("glb.FS_Items") == "Commercial Operating Expenses (Commercial)") &
        (col("glb.cdo") == "no")
    ) \
    .select(
        "glb.fisc_per",
        when(col("glb.cost_center").isNull(), col("pcm.bus_type_ext")).otherwise(col("ccm.bus_type_ext")).alias("bus_type_ext"),
        when(col("glb.cost_center").isNull(), col("pcm.trade_grp")).otherwise(col("ccm.trade_grp")).alias("trade_grp"),
        when(col("glb.cost_center").isNull(), col("pcm.sales_grp")).otherwise(col("ccm.sales_grp")).alias("sales_grp"),
        col("glb.balance")
    ) \
    .groupBy(
        "glb.fisc_per", 
        "bus_type_ext", 
        "trade_grp", 
        "sales_grp"
    ) \
    .agg(
        sum("glb.balance").alias("sell_exp")  # Ensure the aggregation for the sum of the balance
    )

# Display the result
#df_sellexp.display()

# COMMAND ----------

# DBTITLE 1,Selling Expense joins
# 1. when we have a valid sales group
df_sellexp_alloc_1 = df_vol_mix_salesgrpcustmat \
    .join(df_sellexp.filter(col("sales_grp") != "xx"), on=["fisc_per", "sales_grp"], how="left_outer") \
    .withColumn("sellexp_alloc", col("sell_exp") * col("mix_salesgrp_mktg_uc")) \
    .drop("bus_type_ext", "trade_grp", "sales_grp")

# 2. when we have a valid trade group but sales group is xx
df_sellexp_alloc_2 = df_vol_mix_salesgrpcustmat \
    .join(df_sellexp.filter((col("trade_grp") != "xx") & (col("sales_grp") == "xx")), on=["fisc_per", "trade_grp"], how="left_outer") \
    .withColumn("sellexp_alloc", col("sell_exp") * col("mix_tradegrp_mktg_uc")) \
    .drop("bus_type_ext", "trade_grp", "sales_grp")

# 3. when we have a value of GT or MT for business type extension but trade group and sales group are xx
df_sellexp_alloc_3 = df_vol_mix_salesgrpcustmat \
    .join(df_sellexp.filter((col("bus_type_ext") != "xx") & (col("trade_grp") == "xx") & (col("sales_grp") == "xx")), 
          on=["fisc_per", "bus_type_ext"], how="left_outer") \
    .withColumn("sellexp_alloc", col("sell_exp") * col("mix_gtmt_mktg_uc")) \
    .drop("bus_type_ext", "trade_grp", "sales_grp")

# 4. when business type extension is xx (HO) and trade group and sales group are xx
df_sellexp_alloc_4 = df_vol_mix_salesgrpcustmat \
    .join(df_sellexp.filter((col("bus_type_ext") == "xx") & (col("trade_grp") == "xx") & (col("sales_grp") == "xx")), 
          on=["fisc_per"], how="left_outer") \
    .withColumn("sellexp_alloc", col("sell_exp") * col("mix_total_mktg_uc")) \
    .drop("bus_type_ext", "trade_grp", "sales_grp")


# Union the DataFrames df_sellexp_alloc_1, df_sellexp_alloc_2, df_sellexp_alloc_3, df_sellexp_alloc_4
df_sellexp_alloc = df_sellexp_alloc_1 \
    .union(df_sellexp_alloc_2) \
    .union(df_sellexp_alloc_3) \
    .union(df_sellexp_alloc_4) \
    .groupBy("fisc_per", "customer", "material") \
    .agg(sum("sellexp_alloc").alias("sellexp_alloc"))

# Join df_sellexp_alloc with df_final on ["fisc_per", "customer", "material"]
df_final = df_final.join(df_sellexp_alloc, on=["fisc_per", "customer", "material"], how="left_outer")
 
df_final = df_final.withColumn("selling_expenses", coalesce(col("sellexp_alloc").cast("double") * col("line_vol_mix").cast("double"),lit(0)))

df_final = df_final.withColumn("selling_expenses", (col("selling_expenses")).cast('decimal(38,18)'))

# COMMAND ----------

# DBTITLE 1,operating_profit
# support_services needs to be added

# df_final = df_final.withColumn("operating_profit", col("fdm_after_curve") - (col("tme") + col("selling_expenses") +col("cold_drink_operations") + col("support_services")).cast('decimal(38,18)')
df_final = df_final.withColumn("operating_profit", col("fdm_after_curve") - (col("tme") + col("selling_expenses") +col("cold_drink_operations")))

# COMMAND ----------

# df_final_copa_items = df_final.select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type","reserve_92", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc", "base_price_adj", "base_price_excl_excise", "excise_from_copa", "excise_from_cogs", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "promo_disc_oninv", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout", "ipp_equip_rental", "ipp_freight_charge", "other_rev_cust","cogs_concentrate", "cogs_commodities", "cogs_conversion", "cogs_sweetener", "cogs_compounding", "cogs_his_ace_k", "other_ing", "other_ing_tea", "other_ing_sweetner", "other_ing_dairy", "other_ing_alcohol", "other_ing_whiskey", "resin", "preform", "pet_bottels", "cans", "closures_crows", "labels", "packaging_material_corrugate", "stretch_film", "shrink_film", "separator_sheets_layer_pads", "pet_plastic_handles", "bags", "other_pack_material", "other_pack_material_straw", "co2", "primary_packaging", "repacking", "management_fee", "distilling_cost", "energy", "tolling_fee_toller", "tolling_fee_pet", "imp_mother_sku_upl", "pack_filling_oh_rm_specific", "pack_filling_oh_rm_alloc", "pack_filling_oh_var_specific", "pack_filling_oh_var_alloc", "pack_filling_dep_pack_specific", "pack_filling_bc_pack", "pack_filling_oh_fixed_alloc", "pack_filling_oh_fixed_specific", "pack_filling_dep_pack_allocated", "haulage", "dfr_ctm", "dfr_sales_curve", "total_disc_offinv", "other_rev", "other_rev_non_cust", "nsr_base_incidence", "cogs_insidence", "cogs_under_over", "cogs_concentrate_tccc", "cogs_concentrate_nontccc", "cogs_incidence_adj")

# COMMAND ----------

df_final_copa_items = df_final.select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","plant","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc","free_goods_vol_fpc","financial_vol_uc","financial_vol_pc","financial_vol_fpc", "revenue_adj","base_price_adj", "base_price_excl_excise", "excise_from_copa", "Excise_Tax_Std", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "gross_wholesale_price","total_discounts","nsr_after_curve","nsr_before_curve","revenue_after_curve","revenue_before_curve", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "promo_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout", "dfr_ctm", "dfr_sales_curve", "total_disc_offinv", "trade_grp", "ipp_equip_rental", "ipp_freight_charge", "other_rev_cust", "other_rev", "other_rev_non_cust", "misc_revenue", "discounts_not_used","nka_discount","nsr_base_incidence","cogs_concentrate","cogs_concentrate_tccc","cogs_concentrate_nontccc","Commodities_Std_Cost","Conversion_Std_Cost", "cogs_under_over","haulage_std","haulage_total","total_cogs","delivery_std","delivery_total","delivery_under_over","warehouse_std","warehouse_total","warehouse_under_over","logistics_opex", "cogs_concentrate_total", "cogs_sweetener_total", "cogs_compounding_total", "cogs_his_ace_k_total", "cogs_other_ing_total", "cogs_other_ing_tea_total", "cogs_other_ing_sweetener_total", "cogs_other_ing_dairy_total", "cogs_other_ing_alcohol_total", "cogs_other_ing_whiskey_total", "cogs_resin_total", "cogs_preform_total", "cogs_pet_bottles_total", "cogs_cans_total", "cogs_closures_crowns_total", "cogs_labels_total", "cogs_packaging_material_corrugate_total", "cogs_stretch_film_total", "cogs_shrink_film_total", "cogs_separator_sheets_total", "cogs_pet_plastic_handles_total", "cogs_bags_total", "cogs_other_packaging_material_total", "cogs_other_packaging_material_straw_total", "cogs_co2_total", "cogs_primary_packaging_total", "cogs_repacking_total", "cogs_packaging_materials_total", "cogs_commodities_total", "cogs_excise_total", "cogs_energy_total", "cogs_tolling_fee_toller_total", "cogs_tolling_fee_pet_total","cogs_pack_filling_oh_rm_specific_total", "cogs_pack_filling_oh_rm_alloc_total", "cogs_pack_filling_oh_var_specific_total", "cogs_pack_filling_oh_var_alloc_total", "cogs_pack_filling_bc_pack_total", "cogs_distilling_cost_total", "cogs_variable_total", "cogs_imp_mother_sku_upl_total", "cogs_pack_filling_oh_fixed_specific_total", "cogs_pack_filling_oh_fixed_alloc_total", "cogs_pack_filling_dep_pack_specific_total", "cogs_pack_filling_dep_pack_allocated_total", "cogs_fixed_total", "cogs_conversion_cost_total", "haulage_under_over", "total_cogs_portfolio","tme","tme_curve","tme_incl_curve","selling_expenses","fdm_after_curve","fdm_before_curve","cold_drink_operations","operating_profit")

# COMMAND ----------

df_final_copa_items.write.format("delta").partitionBy("fisc_per").mode("append").saveAsTable(f"""{gold_schema}.fact.copa_items""" )

# COMMAND ----------

# MAGIC %sql
# MAGIC select fisc_per, sum(mktg_vol_pc) from dev_gold.fact.copa_items
# MAGIC group by fisc_per

# COMMAND ----------

# %sql

# select fisc_per,sum(tme) from dev_gold.fact.copa_items  group by fisc_per

# COMMAND ----------

# %sql

# select fisc_per,sum(total_disc_offinv),sum(dfr_cr_dr_memos),sum(dfr_ctm),sum(dfr_del_pen_disc),sum(dfr_dmi),sum(dfr_dmi_fallout),sum(dfr_fallout),sum(dfr_fixed_disc),sum(dfr_from_rms),sum(dfr_pickup_disc),sum(dfr_promo_disc),sum(dfr_sales_curve),sum(dfr_variable_disc) from dev_gold.fact.copa_items  group by fisc_per
