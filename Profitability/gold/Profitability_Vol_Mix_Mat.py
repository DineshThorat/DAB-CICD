# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

import os
from datetime import datetime,date, timedelta
from pyspark.sql.functions import *
import builtins as py
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

# MAGIC %md
# MAGIC ### initial config

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# MAGIC %run 
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/std_cogs_latest_rec_per_period

# COMMAND ----------

# spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss",False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### get and set variables for environment

# COMMAND ----------

dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")
print(Env)

# COMMAND ----------

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### get vol_mix_mat specific config

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/cogs_variance_reusable_obj

# COMMAND ----------

# MAGIC %md
# MAGIC ### get and set variables for fisc_per
# MAGIC set default fisc_per if none provided

# COMMAND ----------

current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d")

df_per = spark.table(f'{gold_schema}.common_dim.fisc_per')
df_default_fisc_per = df_per.filter((lit(formatted_date).cast("date") >= col("PeriodStart")) & 
    (lit(formatted_date).cast("date") <= col("PeriodEnd"))).select('FiscalPeriod')

start_fisc_per = df_default_fisc_per.collect()[0][0]
end_fisc_per = df_default_fisc_per.collect()[0][0]

dbutils.widgets.text("start_fisc_per", start_fisc_per, 'start_fisc_per')
dbutils.widgets.text("end_fisc_per", end_fisc_per, 'end_fisc_per')

v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")

if not v_start_fisc_per:
    v_start_fisc_per = start_fisc_per
if not v_end_fisc_per:
    v_end_fisc_per = end_fisc_per

print(f"start_fisc_per: {v_start_fisc_per}")
print(f"end_fisc_per: {v_end_fisc_per}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### get COGS Variances

# COMMAND ----------

df_ppv, df_uv, df_srmv, df_abs, df_tolling, df_brand_adj = fn_cogs_variance(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)

# COMMAND ----------

# display(df_ppv)

# COMMAND ----------

# DBTITLE 1,validate variance totals
# # # Get all columns and their types
# decimal_columns = [
#     field.name
#     for field in df_srmv.schema.fields
#     if isinstance(field.dataType, DecimalType)
# ]

# # Dynamically create a sum expression for all decimal columns
# sum_expr = py.sum([col(column) for column in decimal_columns], lit(0))

# # Add the sum as a new column
# display(df_srmv.withColumn("total_var", sum_expr).select("total_var"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### get initial data selections

# COMMAND ----------

# DBTITLE 1,fiscal period
df_per = df_per.filter((col("FiscalPeriod") >= v_start_fisc_per ) & (col("FiscalPeriod") <= v_end_fisc_per))
v_fisc_year = df_per.select("Year").collect()[0][0]
# print(v_fisc_year)

# COMMAND ----------

# DBTITLE 1,get data from copa_items
export_skus = list(export_skus)
CO2_skus = list(CO2_skus)

#Reading copa table
df_copa_items = spark.table(f'{silver_schema}.ecc.copa_items') \
    .filter(((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per)) & (col("material_type") == finished_goods_mat_type) \
& (~col("material").isin(export_skus)) \
& (~col("material").isin(CO2_skus))) \
.select("fisc_per","fisc_year","customer","material", \
when(~col("sales_doc_type").isin("YDME","ZMKT"), col("physical_case_qty")).otherwise(0).alias("financial_vol_pc"), \
col("physical_case_qty").alias("marketing_vol_pc"))

window_spec_per = Window.partitionBy("fisc_per")

# COMMAND ----------

# DBTITLE 1,sum volume by material and period
df_mat_per_vol = df_copa_items.groupBy("fisc_per","fisc_year","material") \
    .agg(sum("financial_vol_pc").alias("financial_vol_pc"), sum("marketing_vol_pc").alias("marketing_vol_pc"))

# COMMAND ----------

# display(df_mat_per_vol.filter(col("material").isin('101671','101670','101672')).groupBy("material").agg(sum(col("marketing_vol_pc"))))

# COMMAND ----------

# DBTITLE 1,get mapping tables
#Reading standard cogs table
df_std_cogs = std_cogs_call(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)

#Reading Material table
df_material = spark.table(f'{gold_schema}.common_dim.current_materials')\
                   .withColumn("MaterialNo",col("MaterialNo").cast('string'))

# Reading vol_mix_brandmat table
df_vol_mix_brandmat = spark.table(f'{gold_schema}.profitability.vol_mix_brandmat') \
    .filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

# display(df_vol_mix_brandmat.filter((col("material") == '101650') & (col("fisc_per") == '2024010')))

# COMMAND ----------

# DBTITLE 1,join volume with std COGS
# Joining copa with std. cogs
df_copa_cogs = df_mat_per_vol \
     .join(df_std_cogs, ["fisc_per", "material"],'left_outer') \
    .fillna(0.0)

# COMMAND ----------

# DBTITLE 1,check if std_cogs has duplicated materials
# display(df_copa_cogs.groupBy("fisc_per", "material").agg(count("*").alias("count")).filter(col("count") > 1))

# COMMAND ----------

# DBTITLE 1,join with variances and other mapping tables
df_copa_cogs_variance = df_copa_cogs \
    .join(df_ppv, "fisc_per", how="left_outer")\
    .join(df_uv, "fisc_per", how="left_outer")\
    .join(df_srmv, "fisc_per", how="left_outer")\
    .join(df_abs, "fisc_per", how="left_outer")\
    .join(df_tolling, "fisc_per", how="left_outer")\
    .join(df_material, df_copa_cogs["material"] == df_material["MaterialNo"], how="left_outer")\
    .join(df_brand_adj.alias("badj"), \
        (df_copa_cogs["fisc_per"] == df_brand_adj["fisc_per"]) 
        & (df_material["MajorBrand"] == df_brand_adj["MajorBrand"]), how="left_outer")\
    .join(df_vol_mix_brandmat.alias("bmix"), (df_copa_cogs["fisc_per"] == df_vol_mix_brandmat["fisc_per"]) & (df_copa_cogs["material"] == df_vol_mix_brandmat["material"]), how="left_outer") \
    .drop(df_vol_mix_brandmat['fisc_per'],df_vol_mix_brandmat['material'],df_brand_adj['fisc_per'])

# COMMAND ----------

# df_vol_mix_brandmat.filter('material = "103962"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### calculate COGS Components

# COMMAND ----------

# DBTITLE 1,main COGS calculations
df_vol_mix_mat = df_copa_cogs_variance \
    .withColumn("financial_vol_uc", 
                            (col("financial_vol_pc") * col("conv_full_case") * col("conv_to_unit_case")))\
    .withColumn("total_qty_uc",sum("financial_vol_uc").over(window_spec_per))\
    .withColumn("mix_total_uc",col("financial_vol_uc")/col("total_qty_uc"))\
    .withColumn("marketing_vol_uc",(col("marketing_vol_pc") * col("conv_full_case") *col("conv_to_unit_case")))\
    .withColumn("total_marketing_qty_uc",sum("marketing_vol_uc").over(window_spec_per))\
    .withColumn("mix_marketing_uc",col("marketing_vol_uc")/col("total_marketing_qty_uc"))\
    .withColumn("std_dolphine_support",col("financial_vol_pc")*col("dolphine_support_pc"))\
    .withColumn("mix_dolphine_support",(col("std_dolphine_support")/sum("std_dolphine_support").over(window_spec_per)).cast("decimal(38,18)"))\
    .withColumn("std_concentrate",col("financial_vol_pc")*col("concentrate_pc"))\
    .withColumn("total_concentrate_pc", sum("std_concentrate").over(window_spec_per)) \
    .withColumn("mix_concentrate", (col("std_concentrate")/col("total_concentrate_pc")).cast("decimal(38,18)"))\
    .withColumn("uv_concentrate",col("mix_concentrate") * df_uv.total_uv_concentrate)\
    .withColumn("ppv_concentrate",when(col("fisc_per")<lit('2024001'), col("mix_concentrate") * df_ppv.total_ppv_concentrate) \
        .otherwise(col("mix_dolphine_support") * df_ppv.total_ppv_concentrate))\
    .withColumn("srmv_concentrate", col("mix_concentrate") * df_srmv.total_srmv_concentrate)\
    .withColumn("badj_concentrate",df_brand_adj.adj_amount * df_vol_mix_brandmat.mix_brand_uc_ytd)\
    .withColumn("total_var_concentrate",col("uv_concentrate")+col("ppv_concentrate")+col("srmv_concentrate")+col("badj_concentrate"))\
    .withColumn("cogs_concentrate",col("std_concentrate")+col("total_var_concentrate"))\
    .withColumn("cogs_rate_concentrate",col("cogs_concentrate")/col("financial_vol_pc"))\
    .withColumn("std_sugar_sweetner",col("financial_vol_pc")*col("sugar_sweetner_pc"))\
    .withColumn("mix_sugar_sweetener", col("std_sugar_sweetner")/sum("std_sugar_sweetner").over(window_spec_per))\
    .withColumn("uv_sugar_sweetener",col("mix_sugar_sweetener") * df_uv.total_uv_sugar_sweetner)\
    .withColumn("ppv_sugar_sweetener",col("mix_sugar_sweetener") * df_ppv.total_ppv_sugar_sweetner)\
    .withColumn("srmv_sugar_sweetener",col("mix_sugar_sweetener")*df_srmv.total_srmv_sugar_sweetner)\
    .withColumn("total_var_sugar_sweetener",col("uv_sugar_sweetener")+col("ppv_sugar_sweetener")+col("srmv_sugar_sweetener"))\
    .withColumn("cogs_sugar_sweetener",col("std_sugar_sweetner")+col("total_var_sugar_sweetener"))\
    .withColumn("cogs_rate_sugar_sweetener",col("cogs_sugar_sweetener")/col("financial_vol_pc"))\
    .withColumn("std_compounding",col("financial_vol_pc")*col("compounding_pc"))\
    .withColumn("mix_compounding", col("std_compounding")/sum("std_compounding").over(window_spec_per))\
    .withColumn("uv_compounding",col("mix_compounding") * df_uv.total_uv_compounding)\
    .withColumn("ppv_compounding",col("mix_compounding") * df_ppv.total_ppv_compounding)\
    .withColumn("srmv_compounding",col("mix_compounding") * df_srmv.total_srmv_compounding)\
    .withColumn("total_var_compounding",col("uv_compounding")+col("ppv_compounding")+col("srmv_compounding"))\
    .withColumn("cogs_compounding",col("std_compounding")+col("total_var_compounding"))\
    .withColumn("cogs_rate_compounding",col("cogs_compounding")/col("financial_vol_pc"))\
    .withColumn("std_his_ace_k",col("financial_vol_pc")*col("his_ace_k_pc"))\
    .withColumn("mix_his_ace_k", col("std_his_ace_k")/sum("std_his_ace_k").over(window_spec_per))\
    .withColumn("uv_his_ace_k",col("mix_his_ace_k") * df_uv.total_uv_his_ace_k)\
    .withColumn("ppv_his_ace_k",col("mix_his_ace_k") * df_ppv.total_ppv_his_ace_k)\
    .withColumn("srmv_his_ace_k",col("mix_his_ace_k") * df_srmv.total_srmv_his_ace_k)\
    .withColumn("total_var_his_ace_k",col("uv_his_ace_k")+col("ppv_his_ace_k")+col("srmv_his_ace_k"))\
    .withColumn("cogs_his_ace_k",col("std_his_ace_k")+col("total_var_his_ace_k"))\
    .withColumn("cogs_rate_his_ace_k",col("cogs_his_ace_k")/col("financial_vol_pc"))\
    .withColumn("std_other_ing",col("financial_vol_pc")*col("other_ing_pc"))\
    .withColumn("mix_other_ing", col("std_other_ing")/sum("std_other_ing").over(window_spec_per))\
    .withColumn("uv_other_ing",col("mix_other_ing") * df_uv.total_uv_other_ing)\
    .withColumn("ppv_other_ing",col("mix_other_ing") * df_ppv.total_ppv_other_ing)\
    .withColumn("srmv_other_ing",col("mix_other_ing") * df_srmv.total_srmv_other_ing)\
    .withColumn("total_var_other_ing",col("uv_other_ing")+col("ppv_other_ing")+col("srmv_other_ing"))\
    .withColumn("cogs_other_ing",col("std_other_ing")+col("total_var_other_ing"))\
    .withColumn("cogs_rate_other_ing",col("cogs_other_ing")/col("financial_vol_pc"))\
    .withColumn("std_other_ing_tea",col("financial_vol_pc")*col("other_ing_tea_pc"))\
    .withColumn("mix_other_ing_tea", col("std_other_ing_tea")/sum("std_other_ing_tea").over(window_spec_per))\
    .withColumn("uv_other_ing_tea",col("mix_other_ing_tea") * df_uv.total_uv_other_ing_tea)\
    .withColumn("ppv_other_ing_tea",col("mix_other_ing_tea") * df_ppv.total_ppv_other_ing_tea)\
    .withColumn("srmv_other_ing_tea",col("mix_other_ing_tea") * df_srmv.total_srmv_other_ing_tea)\
    .withColumn("total_var_other_ing_tea",col("uv_other_ing_tea")+col("ppv_other_ing_tea")+col("srmv_other_ing_tea"))\
    .withColumn("cogs_other_ing_tea",col("std_other_ing_tea")+col("total_var_other_ing_tea"))\
    .withColumn("cogs_rate_other_ing_tea",col("cogs_other_ing_tea")/col("financial_vol_pc"))\
    .withColumn("std_other_ing_sweetner",col("financial_vol_pc")*col("other_ing_sweetner_pc"))\
    .withColumn("mix_other_ing_sweetner", col("std_other_ing_sweetner")/sum("std_other_ing_sweetner").over(window_spec_per))\
    .withColumn("uv_other_ing_sweetner",col("mix_other_ing_sweetner") * df_uv.total_uv_other_ing_sweetner)\
    .withColumn("ppv_other_ing_sweetner",col("mix_other_ing_sweetner") * df_ppv.total_ppv_other_ing_sweetner)\
    .withColumn("srmv_other_ing_sweetner",col("mix_other_ing_sweetner") * df_srmv.total_srmv_other_ing_sweetner)\
    .withColumn("total_var_other_ing_sweetner",col("uv_other_ing_sweetner")+col("ppv_other_ing_sweetner")+col("srmv_other_ing_sweetner"))\
    .withColumn("cogs_other_ing_sweetner",col("std_other_ing_sweetner")+col("total_var_other_ing_sweetner"))\
    .withColumn("cogs_rate_other_ing_sweetner",col("cogs_other_ing_sweetner")/col("financial_vol_pc"))\
    .withColumn("std_other_ing_dairy",col("financial_vol_pc")*col("other_ing_dairy_pc"))\
    .withColumn("mix_other_ing_dairy", col("std_other_ing_dairy")/sum("std_other_ing_dairy").over(window_spec_per))\
    .withColumn("uv_other_ing_dairy",col("mix_other_ing_dairy") * df_uv.total_uv_other_ing_dairy)\
    .withColumn("ppv_other_ing_dairy",col("mix_other_ing_dairy") *df_ppv.total_ppv_other_ing_dairy)\
    .withColumn("srmv_other_ing_dairy",col("mix_other_ing_dairy") * df_srmv.total_srmv_other_ing_dairy)\
    .withColumn("total_var_other_ing_dairy",col("uv_other_ing_dairy")+col("ppv_other_ing_dairy")+col("srmv_other_ing_dairy"))\
    .withColumn("cogs_other_ing_dairy",col("std_other_ing_dairy")+col("total_var_other_ing_dairy"))\
    .withColumn("cogs_rate_other_ing_dairy",col("cogs_other_ing_dairy")/col("financial_vol_pc"))\
    .withColumn("std_other_ing_alcohol",col("financial_vol_pc")*col("other_ing_alcohol_pc"))\
    .withColumn("mix_other_ing_alcohol", col("std_other_ing_alcohol")/sum("std_other_ing_alcohol").over(window_spec_per))\
    .withColumn("uv_other_ing_alcohol",col("mix_other_ing_alcohol") * df_uv.total_uv_other_ing_alchol)\
    .withColumn("ppv_other_ing_alcohol",col("mix_other_ing_alcohol") * df_ppv.total_ppv_other_ing_alchol)\
    .withColumn("srmv_other_ing_alcohol",col("mix_other_ing_alcohol") * df_srmv.total_srmv_other_ing_alchol)\
    .withColumn("total_var_other_ing_alcohol",col("uv_other_ing_alcohol")+col("ppv_other_ing_alcohol")+col("srmv_other_ing_alcohol"))\
    .withColumn("cogs_other_ing_alcohol",col("std_other_ing_alcohol")+col("total_var_other_ing_alcohol"))\
    .withColumn("cogs_rate_other_ing_alcohol",col("cogs_other_ing_alcohol")/col("financial_vol_pc"))\
    .withColumn("std_other_ing_whiskey",col("financial_vol_pc")*col("other_ing_whiskey_pc"))\
    .withColumn("mix_other_ing_whiskey", col("std_other_ing_whiskey")/sum("std_other_ing_whiskey").over(window_spec_per))\
    .withColumn("uv_other_ing_whiskey",col("mix_other_ing_whiskey") * df_uv.total_uv_other_ing_whiskey)\
    .withColumn("ppv_other_ing_whiskey",col("mix_other_ing_whiskey") * df_ppv.total_ppv_other_ing_whiskey)\
    .withColumn("srmv_other_ing_whiskey",col("mix_other_ing_whiskey") * df_srmv.total_srmv_other_ing_whiskey)\
    .withColumn("total_var_other_ing_whiskey",col("uv_other_ing_whiskey")+col("ppv_other_ing_whiskey")+col("srmv_other_ing_whiskey"))\
    .withColumn("cogs_other_ing_whiskey",col("std_other_ing_whiskey")+col("total_var_other_ing_whiskey"))\
    .withColumn("cogs_rate_other_ing_whiskey",col("cogs_other_ing_whiskey")/col("financial_vol_pc"))\
    .withColumn("std_resin",col("financial_vol_pc")*col("resin_pc"))\
    .withColumn("mix_resin", col("std_resin")/sum("std_resin").over(window_spec_per))\
    .withColumn("uv_resin",col("mix_resin") * df_uv.total_uv_resin)\
    .withColumn("ppv_resin",col("mix_resin") * df_ppv.total_ppv_resin)\
    .withColumn("srmv_resin",col("mix_resin") * df_srmv.total_srmv_resin)\
    .withColumn("total_var_resin",col("uv_resin")+col("ppv_resin")+col("srmv_resin"))\
    .withColumn("cogs_resin",col("std_resin")+col("total_var_resin"))\
    .withColumn("cogs_rate_resin",col("cogs_resin")/col("financial_vol_pc"))\
    .withColumn("std_preform",col("financial_vol_pc")*col("preform_pc"))\
    .withColumn("mix_preform", col("std_preform")/sum("std_preform").over(window_spec_per))\
    .withColumn("uv_preform",col("mix_preform") * df_uv.total_uv_preform)\
    .withColumn("ppv_preform",col("mix_preform") * df_ppv.total_ppv_preform)\
    .withColumn("srmv_preform",col("mix_preform") * df_srmv.total_srmv_preform)\
    .withColumn("total_var_preform",col("uv_preform")+col("ppv_preform")+col("srmv_preform"))\
    .withColumn("cogs_preform",col("std_preform")+col("total_var_preform"))\
    .withColumn("cogs_rate_preform",col("cogs_preform")/col("financial_vol_pc"))\
    .withColumn("std_pet_bottles",col("financial_vol_pc")*col("pet_bottles_pc"))\
    .withColumn("mix_pet_bottles", col("std_pet_bottles")/sum("std_pet_bottles").over(window_spec_per))\
    .withColumn("uv_pet_bottles",col("mix_pet_bottles") * df_uv.total_uv_pet_bottles)\
    .withColumn("ppv_pet_bottles",col("mix_pet_bottles") * df_ppv.total_ppv_pet_bottles)\
    .withColumn("srmv_pet_bottles",col("mix_pet_bottles") * df_srmv.total_srmv_pet_bottles)\
    .withColumn("total_var_pet_bottles",col("uv_pet_bottles")+col("ppv_pet_bottles")+col("srmv_pet_bottles"))\
    .withColumn("cogs_pet_bottles",col("std_pet_bottles")+col("total_var_pet_bottles"))\
    .withColumn("cogs_rate_pet_bottles",col("cogs_pet_bottles")/col("financial_vol_pc"))\
    .withColumn("std_cans",col("financial_vol_pc")*col("cans_pc"))\
    .withColumn("mix_cans", col("std_cans")/sum("std_cans").over(window_spec_per))\
    .withColumn("uv_cans",col("mix_cans") * df_uv.total_uv_can)\
    .withColumn("ppv_cans",col("mix_cans") * df_ppv.total_ppv_can)\
    .withColumn("srmv_cans",col("mix_cans") * df_srmv.total_srmv_can)\
    .withColumn("total_var_cans",col("uv_cans")+col("ppv_cans")+col("srmv_cans"))\
    .withColumn("cogs_cans",col("std_cans")+col("total_var_cans"))\
    .withColumn("cogs_rate_cans",col("cogs_cans")/col("financial_vol_pc"))\
    .withColumn("std_closures_crowns",col("financial_vol_pc")*col("closures_crowns_pc"))\
    .withColumn("mix_closures_crowns", col("std_closures_crowns")/sum("std_closures_crowns").over(window_spec_per))\
    .withColumn("uv_closures_crowns",col("mix_closures_crowns") * df_uv.total_uv_closures_crowns)\
    .withColumn("ppv_closures_crowns",col("mix_closures_crowns") *df_ppv.total_ppv_closures_crowns)\
    .withColumn("srmv_closures_crowns",col("mix_closures_crowns") * df_srmv.total_srmv_closures_crowns)\
    .withColumn("total_var_closures_crowns",col("uv_closures_crowns")+col("ppv_closures_crowns")+col("srmv_closures_crowns"))\
    .withColumn("cogs_closures_crowns",col("std_closures_crowns")+col("total_var_closures_crowns"))\
    .withColumn("cogs_rate_closures_crowns",col("cogs_closures_crowns")/col("financial_vol_pc"))\
    .withColumn("std_labels",col("financial_vol_pc")*col("labels_pc"))\
    .withColumn("mix_labels", col("std_labels")/sum("std_labels").over(window_spec_per))\
    .withColumn("uv_labels",col("mix_labels") * df_uv.total_uv_labels)\
    .withColumn("ppv_labels",col("mix_labels") * df_ppv.total_ppv_labels)\
    .withColumn("srmv_labels",col("mix_labels") * df_srmv.total_srmv_labels)\
    .withColumn("total_var_labels",col("uv_labels")+col("ppv_labels")+col("srmv_labels"))\
    .withColumn("cogs_labels",col("std_labels")+col("total_var_labels"))\
    .withColumn("cogs_rate_labels",col("cogs_labels")/col("financial_vol_pc"))\
    .withColumn("std_packaging_material_corrugate",col("financial_vol_pc")*col("packaging_material_corrugate_pc"))\
    .withColumn("mix_packaging_material_corrugate", col("std_packaging_material_corrugate")/sum("std_packaging_material_corrugate").over(window_spec_per))\
    .withColumn("uv_packaging_material_corrugate",col("mix_packaging_material_corrugate") * df_uv.total_uv_packaging_material_corrugate)\
    .withColumn("ppv_packaging_material_corrugate",col("mix_packaging_material_corrugate") * df_ppv.total_ppv_packaging_material_corrugate)\
    .withColumn("srmv_packaging_material_corrugate",col("mix_packaging_material_corrugate") * df_srmv.total_srmv_packaging_material_corrugate)\
    .withColumn("total_var_packaging_material_corrugate",col("uv_packaging_material_corrugate")+col("ppv_packaging_material_corrugate")+col("srmv_packaging_material_corrugate"))\
    .withColumn("cogs_packaging_material_corrugate",col("std_packaging_material_corrugate")+col("total_var_packaging_material_corrugate"))\
    .withColumn("cogs_rate_packaging_material_corrugate",col("cogs_packaging_material_corrugate")/col("financial_vol_pc"))\
    .withColumn("std_stretch_film",col("financial_vol_pc")*col("stretch_film_pc"))\
    .withColumn("mix_stretch_film", col("std_stretch_film")/sum("std_stretch_film").over(window_spec_per))\
    .withColumn("uv_stretch_film",col("mix_stretch_film") * df_uv.total_uv_stretch_film)\
    .withColumn("ppv_stretch_film",col("mix_stretch_film") * df_ppv.total_ppv_stretch_film)\
    .withColumn("srmv_stretch_film",col("mix_stretch_film") * df_srmv.total_srmv_stretch_film)\
    .withColumn("total_var_stretch_film",col("uv_stretch_film")+col("ppv_stretch_film")+col("srmv_stretch_film"))\
    .withColumn("cogs_stretch_film",col("std_stretch_film") + col("total_var_stretch_film"))\
    .withColumn("cogs_rate_stretch_film",col("cogs_stretch_film")/ col("financial_vol_pc"))\
    .withColumn("std_shrink_film",col("financial_vol_pc")*col("shrink_film_pc"))\
    .withColumn("mix_shrink_film", col("std_shrink_film")/sum("std_shrink_film").over(window_spec_per))\
    .withColumn("uv_shrink_film",col("mix_shrink_film") * df_uv.total_uv_shrink_film)\
    .withColumn("ppv_shrink_film",col("mix_shrink_film") * df_ppv.total_ppv_shrink_film)\
    .withColumn("srmv_shrink_film",col("mix_shrink_film") * df_srmv.total_srmv_shrink_film)\
    .withColumn("total_var_shrink_film",col("uv_shrink_film")+col("ppv_shrink_film")+col("srmv_shrink_film"))\
    .withColumn("cogs_shrink_film",col("std_shrink_film")+col("total_var_shrink_film"))\
    .withColumn("cogs_rate_shrink_film",col("cogs_shrink_film")/col("financial_vol_pc"))\
    .withColumn("std_separator_sheets",col("financial_vol_pc")* col("separator_sheets_pc"))\
    .withColumn("mix_separator_sheets", col("std_separator_sheets")/sum("std_separator_sheets").over(window_spec_per))\
    .withColumn("uv_separator_sheets",col("mix_separator_sheets") * df_uv.total_uv_separator_sheets)\
    .withColumn("ppv_separator_sheets",col("mix_separator_sheets") * df_ppv.total_ppv_separator_sheets)\
    .withColumn("srmv_separator_sheets",col("mix_separator_sheets") * df_srmv.total_srmv_separator_sheets)\
    .withColumn("total_var_separator_sheets",col("uv_separator_sheets")+col("ppv_separator_sheets")+col("srmv_separator_sheets"))\
    .withColumn("cogs_separator_sheets",col("std_separator_sheets")+col("total_var_separator_sheets"))\
    .withColumn("cogs_rate_separator_sheets",col("cogs_separator_sheets")/col("financial_vol_pc"))\
    .withColumn("std_pet_plastic_handles",col("financial_vol_pc")*col("pet_plastic_handles_pc"))\
    .withColumn("mix_pet_plastic_handles", col("std_pet_plastic_handles")/sum("std_pet_plastic_handles").over(window_spec_per))\
    .withColumn("uv_pet_plastic_handles",col("mix_pet_plastic_handles") * df_uv.total_uv_pet_plastic_handles)\
    .withColumn("ppv_pet_plastic_handles",col("mix_pet_plastic_handles") * df_ppv.total_ppv_pet_plastic_handles)\
    .withColumn("srmv_pet_plastic_handles",col("mix_pet_plastic_handles") * df_srmv.total_srmv_pet_plastic_handles)\
    .withColumn("total_var_pet_plastic_handles",col("uv_pet_plastic_handles")+col("ppv_pet_plastic_handles")+col("srmv_pet_plastic_handles"))\
    .withColumn("cogs_pet_plastic_handles",col("std_pet_plastic_handles")+col("total_var_pet_plastic_handles"))\
    .withColumn("cogs_rate_pet_plastic_handles",col("cogs_pet_plastic_handles")/col("financial_vol_pc"))\
    .withColumn("std_bags",col("financial_vol_pc")*col("bags_pc"))\
    .withColumn("mix_bags", col("std_bags")/sum("std_bags").over(window_spec_per))\
    .withColumn("uv_bags",col("mix_bags")* df_uv.total_uv_bags)\
    .withColumn("ppv_bags",col("mix_bags") * df_ppv.total_ppv_bags)\
    .withColumn("srmv_bags",col("mix_bags") * df_srmv.total_srmv_bags)\
    .withColumn("total_var_bags",col("uv_bags")+col("ppv_bags")+col("srmv_bags"))\
    .withColumn("cogs_bags",col("std_bags")+col("total_var_bags"))\
    .withColumn("cogs_rate_bags",col("cogs_bags")/col("financial_vol_pc"))\
    .withColumn("std_other_packaging_material",col("financial_vol_pc")*col("other_packaging_material_pc"))\
    .withColumn("mix_other_packaging_material", col("std_other_packaging_material")/sum("std_other_packaging_material").over(window_spec_per))\
    .withColumn("uv_other_packaging_material",col("mix_other_packaging_material") * df_uv.total_uv_other_pack_material)\
    .withColumn("ppv_other_packaging_material",col("mix_other_packaging_material") * df_ppv.total_ppv_other_pack_material)\
    .withColumn("srmv_other_packaging_material",col("mix_other_packaging_material") * df_srmv.total_srmv_other_pack_material)\
    .withColumn("total_var_other_packaging_material",col("uv_other_packaging_material")+col("ppv_other_packaging_material")+col("srmv_other_packaging_material"))\
    .withColumn("cogs_other_packaging_material",col("std_other_packaging_material")+col("total_var_other_packaging_material"))\
    .withColumn("cogs_rate_other_packaging_material",col("cogs_other_packaging_material")/col("financial_vol_pc"))\
    .withColumn("std_other_packaging_material_straw",col("financial_vol_pc")*col("other_packaging_material_straw_pc"))\
    .withColumn("mix_other_packaging_material_straw", col("std_other_packaging_material_straw")/sum("std_other_packaging_material_straw").over(window_spec_per))\
    .withColumn("uv_other_packaging_material_straw",col("mix_other_packaging_material_straw") * df_uv.total_uv_other_pack_material_straw)\
    .withColumn("ppv_other_packaging_material_straw",col("mix_other_packaging_material_straw") * df_ppv.total_ppv_other_pack_material_straw)\
    .withColumn("srmv_other_packaging_material_straw", col("mix_other_packaging_material_straw") * df_srmv.total_srmv_other_pack_material_straw)\
    .withColumn("total_var_other_packaging_material_straw",col("uv_other_packaging_material_straw")+col("ppv_other_packaging_material_straw")+col("srmv_other_packaging_material_straw"))\
    .withColumn("cogs_other_packaging_material_straw",col("std_other_packaging_material_straw")+col("total_var_other_packaging_material_straw"))\
    .withColumn("cogs_rate_other_packaging_material_straw",col("cogs_other_packaging_material_straw")/col("financial_vol_pc"))\
    .withColumn("std_co2",col("financial_vol_pc")*col("co2_pc"))\
    .withColumn("mix_co2", col("std_co2")/sum("std_co2").over(window_spec_per))\
    .withColumn("uv_co2",col("mix_co2") * df_uv.total_uv_co2)\
    .withColumn("ppv_co2",col("mix_co2") * df_ppv.total_ppv_co2)\
    .withColumn("srmv_co2",col("mix_co2") * df_srmv.total_srmv_co2)\
    .withColumn("total_var_co2",col("uv_co2")+col("ppv_co2")+col("srmv_co2"))\
    .withColumn("cogs_co2",col("std_co2")+col("total_var_co2"))\
    .withColumn("cogs_rate_co2",col("cogs_co2")/col("financial_vol_pc"))\
    .withColumn("std_primary_packaging",col("financial_vol_pc")*col("primary_packaging_pc"))\
    .withColumn("mix_primary_packaging", col("std_primary_packaging")/sum("std_primary_packaging").over(window_spec_per))\
    .withColumn("uv_primary_packaging",col("mix_primary_packaging") * df_uv.total_uv_primary_packaging)\
    .withColumn("ppv_primary_packaging",col("mix_primary_packaging") * df_ppv.total_ppv_primary_packaging)\
    .withColumn("srmv_primary_packaging",col("mix_primary_packaging") * df_srmv.total_srmv_primary_packaging)\
    .withColumn("total_var_primary_packaging",col("uv_primary_packaging")+col("ppv_primary_packaging")+col("srmv_primary_packaging"))\
    .withColumn("cogs_primary_packaging",col("std_primary_packaging")+col("total_var_primary_packaging"))\
    .withColumn("cogs_rate_primary_packaging",col("cogs_primary_packaging")/col("financial_vol_pc"))\
    .withColumn("std_repacking",col("financial_vol_pc")*col("repacking_pc"))\
    .withColumn("mix_repacking", col("std_repacking")/sum("std_repacking").over(window_spec_per))\
    .withColumn("cogs_repacking",col("std_repacking"))\
    .withColumn("cogs_rate_repacking",col("cogs_repacking")/col("financial_vol_pc"))\
    .withColumn("std_excise_tax",col("financial_vol_pc")*col("excise_tax_pc"))\
    .withColumn("mix_excise_tax", col("std_excise_tax")/sum("std_excise_tax").over(window_spec_per))\
    .withColumn("abs_excise_tax",col("mix_excise_tax") * df_abs.total_abs_excise_tax)\
    .withColumn("cogs_excise_tax",col("std_excise_tax")+col("abs_excise_tax"))\
    .withColumn("cogs_rate_excise_tax",col("cogs_excise_tax")/col("financial_vol_pc"))\
    .withColumn("std_energy",col("financial_vol_pc")*col("energy_pc"))\
    .withColumn("mix_energy", col("std_energy")/sum("std_energy").over(window_spec_per))\
    .withColumn("cogs_energy",col("std_energy"))\
    .withColumn("cogs_rate_energy",col("cogs_energy")/col("financial_vol_pc"))\
    .withColumn("std_tolling_fee_toller",col("financial_vol_pc")*col("tolling_fee_toller_pc"))\
    .withColumn("mix_tolling_fee_toller", col("std_tolling_fee_toller")/sum("std_tolling_fee_toller").over(window_spec_per))\
    .withColumn("abs_tolling_fee_toller",col("mix_tolling_fee_toller") * df_tolling.total_abs_tolling_fee_toller)\
    .withColumn("cogs_tolling_fee_toller",col("std_tolling_fee_toller")+col("abs_tolling_fee_toller"))\
    .withColumn("cogs_rate_tolling_fee_toller",col("cogs_tolling_fee_toller")/col("financial_vol_pc"))\
    .withColumn("std_tolling_fee_pet",col("financial_vol_pc")*col("tolling_fee_pet_pc"))\
    .withColumn("mix_tolling_fee_pet", col("std_tolling_fee_pet")/sum("std_tolling_fee_pet").over(window_spec_per))\
    .withColumn("abs_tolling_fee_pet",col("mix_tolling_fee_pet") *df_tolling.total_abs_tolling_fee_pet)\
    .withColumn("cogs_tolling_fee_pet",col("std_tolling_fee_pet")+col("abs_tolling_fee_pet"))\
    .withColumn("cogs_rate_tolling_fee_pet",col("cogs_tolling_fee_pet")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_rm_specific",col("financial_vol_pc")*col("pack_filling_oh_rm_specific_pc"))\
    .withColumn("mix_pack_filling_oh_rm_specific", col("std_pack_filling_oh_rm_specific")/sum("std_pack_filling_oh_rm_specific").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_rm_specific",col("mix_pack_filling_oh_rm_specific") * df_abs.total_abs_pack_filling_oh_rm_specific)\
    .withColumn("cogs_pack_filling_oh_rm_specific",col("std_pack_filling_oh_rm_specific")+col("abs_pack_filling_oh_rm_specific"))\
    .withColumn("cogs_rate_pack_filling_oh_rm_specific",col("cogs_pack_filling_oh_rm_specific")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_rm_alloc",col("financial_vol_pc")*col("pack_filling_oh_rm_alloc_pc"))\
    .withColumn("mix_pack_filling_oh_rm_alloc", col("std_pack_filling_oh_rm_alloc")/sum("std_pack_filling_oh_rm_alloc").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_rm_alloc",col("mix_pack_filling_oh_rm_alloc") * df_abs.total_abs_pack_filling_oh_rm_alloc)\
    .withColumn("cogs_pack_filling_oh_rm_alloc",col("std_pack_filling_oh_rm_alloc")+col("abs_pack_filling_oh_rm_alloc"))\
    .withColumn("cogs_rate_pack_filling_oh_rm_alloc",col("cogs_pack_filling_oh_rm_alloc")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_var_specific",col("financial_vol_pc")*col("pack_filling_oh_var_specific_pc"))\
    .withColumn("mix_pack_filling_oh_var_specific", col("std_pack_filling_oh_var_specific")/sum("std_pack_filling_oh_var_specific").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_var_specific", col("mix_pack_filling_oh_var_specific") * df_abs.total_abs_pack_filling_oh_var_specific)\
    .withColumn("cogs_pack_filling_oh_var_specific",col("std_pack_filling_oh_var_specific")+col("abs_pack_filling_oh_var_specific"))\
    .withColumn("cogs_rate_pack_filling_oh_var_specific",col("cogs_pack_filling_oh_var_specific")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_var_alloc",col("financial_vol_pc")*col("pack_filling_oh_var_alloc_pc"))\
    .withColumn("mix_pack_filling_oh_var_alloc", col("std_pack_filling_oh_var_alloc")/sum("std_pack_filling_oh_var_alloc").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_var_alloc",col("mix_pack_filling_oh_var_alloc") * df_abs.total_abs_pack_filling_oh_var_alloc)\
    .withColumn("cogs_pack_filling_oh_var_alloc",col("std_pack_filling_oh_var_alloc")+col("abs_pack_filling_oh_var_alloc"))\
    .withColumn("cogs_rate_pack_filling_oh_var_alloc",col("cogs_pack_filling_oh_var_alloc")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_fixed_specific",col("financial_vol_pc")*col("pack_filling_oh_fixed_specific_pc"))\
    .withColumn("mix_pack_filling_oh_fixed_specific", col("std_pack_filling_oh_fixed_specific")/sum("std_pack_filling_oh_fixed_specific").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_fixed_specific",col("mix_pack_filling_oh_fixed_specific") * df_abs.total_abs_pack_filling_oh_fixed_specific)\
    .withColumn("cogs_pack_filling_oh_fixed_specific",col("std_pack_filling_oh_fixed_specific")+col("abs_pack_filling_oh_fixed_specific"))\
    .withColumn("cogs_rate_pack_filling_oh_fixed_specific",col("cogs_pack_filling_oh_fixed_specific")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_oh_fixed_alloc",col("financial_vol_pc")*col("pack_filling_oh_fixed_alloc_pc"))\
    .withColumn("mix_pack_filling_oh_fixed_alloc", col("std_pack_filling_oh_fixed_alloc")/sum("std_pack_filling_oh_fixed_alloc").over(window_spec_per))\
    .withColumn("abs_pack_filling_oh_fixed_alloc",col("mix_pack_filling_oh_fixed_alloc") * df_abs.total_abs_pack_filling_oh_fixed_alloc)\
    .withColumn("cogs_pack_filling_oh_fixed_alloc",col("std_pack_filling_oh_fixed_alloc")+col("abs_pack_filling_oh_fixed_alloc"))\
    .withColumn("cogs_rate_pack_filling_oh_fixed_alloc",col("cogs_pack_filling_oh_fixed_alloc")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_bc_pack",col("financial_vol_pc")*col("pack_filling_bc_pack_pc"))\
    .withColumn("mix_pack_filling_bc_pack", col("std_pack_filling_bc_pack")/sum("std_pack_filling_bc_pack").over(window_spec_per))\
    .withColumn("abs_pack_filling_bc_pack",col("mix_pack_filling_bc_pack") * df_abs.total_abs_pack_filling_bc_pack)\
    .withColumn("cogs_pack_filling_bc_pack",col("std_pack_filling_bc_pack")+col("abs_pack_filling_bc_pack"))\
    .withColumn("cogs_rate_pack_filling_bc_pack",col("cogs_pack_filling_bc_pack")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_dep_pack_specific",col("financial_vol_pc")*col("pack_filling_dep_pack_specific_pc"))\
    .withColumn("mix_pack_filling_dep_pack_specific", col("std_pack_filling_dep_pack_specific")/sum("std_pack_filling_dep_pack_specific").over(window_spec_per))\
    .withColumn("abs_pack_filling_dep_pack_specific",col("mix_pack_filling_dep_pack_specific") * df_abs.total_abs_pack_filling_dep_pack_specific)\
    .withColumn("cogs_pack_filling_dep_pack_specific",col("std_pack_filling_dep_pack_specific")+col("abs_pack_filling_dep_pack_specific"))\
    .withColumn("cogs_rate_pack_filling_dep_pack_specific",col("cogs_pack_filling_dep_pack_specific")/col("financial_vol_pc"))\
    .withColumn("std_pack_filling_dep_pack_allocated",col("financial_vol_pc")*col("pack_filling_dep_pack_allocated_pc"))\
    .withColumn("mix_pack_filling_dep_pack_allocated", col("std_pack_filling_dep_pack_allocated")/sum("std_pack_filling_dep_pack_allocated").over(window_spec_per))\
    .withColumn("abs_pack_filling_dep_pack_allocated",col("mix_pack_filling_dep_pack_allocated") * df_abs.total_abs_pack_filling_dep_pack_allocated)\
    .withColumn("cogs_pack_filling_dep_pack_allocated",col("std_pack_filling_dep_pack_allocated")+col("abs_pack_filling_dep_pack_allocated"))\
    .withColumn("cogs_rate_pack_filling_dep_pack_allocated",col("cogs_pack_filling_dep_pack_allocated")/col("financial_vol_pc"))\
    .withColumn("std_distilling_cost",col("financial_vol_pc")*col("distilling_cost_pc"))\
    .withColumn("mix_distilling_cost", col("std_distilling_cost")/sum("std_distilling_cost").over(window_spec_per))\
    .withColumn("cogs_distilling_cost",col("std_distilling_cost"))\
    .withColumn("cogs_rate_distilling_cost",col("cogs_distilling_cost")/col("financial_vol_pc"))\
    .withColumn("std_imp_mother_sku_upl",col("financial_vol_pc")*col("imp_mother_sku_upl_pc"))\
    .withColumn("mix_imp_mother_sku_upl", col("std_imp_mother_sku_upl")/sum("std_imp_mother_sku_upl").over(window_spec_per))\
    .withColumn("srmv_imp_mother_sku_upl",df_srmv.total_srmv_imp_mother_sku_upl * col("mix_imp_mother_sku_upl"))\
    .withColumn("cogs_imp_mother_sku_upl",col("std_imp_mother_sku_upl")+col("srmv_imp_mother_sku_upl"))\
    .withColumn("cogs_rate_imp_mother_sku_upl",col("cogs_imp_mother_sku_upl")/col("financial_vol_pc"))\
    .withColumn("std_management_fee",col("financial_vol_pc")*col("management_fee_pc"))\
    .withColumn("mix_management_fee", col("std_management_fee")/sum("std_management_fee").over(window_spec_per))\
    .withColumn("cogs_management_fee",col("std_management_fee"))\
    .withColumn("cogs_rate_management_fee",col("cogs_management_fee")/col("financial_vol_pc"))\
    .withColumn("std_haulage",col("financial_vol_pc")*col("haulage_pc"))\
    .withColumn("mix_haulage", col("std_haulage")/sum("std_haulage").over(window_spec_per))\
    .withColumn("cogs_haulage",col("std_haulage"))\
    .withColumn("cogs_rate_haulage",col("cogs_haulage")/col("financial_vol_pc"))\
    .withColumn("std_total_cogs",col("financial_vol_pc") * (col("total_cogs_pc")-col("management_fee_pc")-col("concentrate_pc")-col("haulage_pc")))\
    .withColumn("mix_total_cogs", col("std_total_cogs")/sum("std_total_cogs").over(window_spec_per)).fillna(0.0)

# COMMAND ----------

# DBTITLE 1,Total COGS calculations
df_vol_mix_mat_totals = df_vol_mix_mat \
    .withColumn("uv_total_cogs",col("uv_concentrate")+col("uv_sugar_sweetener")+col("uv_compounding") + col("uv_his_ace_k") + col("uv_other_ing") + col("uv_other_ing_tea") + col("uv_other_ing_sweetner") + col("uv_other_ing_dairy") + col("uv_other_ing_alcohol") + col("uv_other_ing_whiskey") + col("uv_resin") + col("uv_preform") + col("uv_pet_bottles") + col("uv_cans") + col("uv_closures_crowns") + col("uv_labels") + col("uv_packaging_material_corrugate") + col("uv_stretch_film") +col("uv_shrink_film") + col("uv_separator_sheets") + col("uv_pet_plastic_handles") + col("uv_bags") + col("uv_other_packaging_material") + col("uv_other_packaging_material_straw") + col("uv_co2") + col("uv_primary_packaging"))\
    .withColumn('ppv_total_cogs',col("ppv_concentrate")+col("ppv_sugar_sweetener")+col("ppv_compounding")+col("ppv_his_ace_k")+col("ppv_other_ing")+col("ppv_other_ing_tea")+col("ppv_other_ing_sweetner")+col("ppv_other_ing_dairy")+col("ppv_other_ing_alcohol")+col("ppv_other_ing_whiskey")+col("ppv_resin")+col("ppv_preform")+col("ppv_pet_bottles")+col("ppv_cans")+col("ppv_closures_crowns")+col("ppv_labels")+col("ppv_packaging_material_corrugate")+col("ppv_stretch_film")+col("ppv_shrink_film")+col("ppv_separator_sheets")+col("ppv_pet_plastic_handles")+col("ppv_bags")+col("ppv_other_packaging_material")+col("ppv_other_packaging_material_straw")+col("ppv_co2")+col("ppv_primary_packaging"))\
    .withColumn("srmv_total_cogs",col("srmv_concentrate")+col("srmv_sugar_sweetener")+col("srmv_compounding")+col("srmv_his_ace_k")+col("srmv_other_ing")+col("srmv_other_ing_tea")+col("srmv_other_ing_sweetner")+col("srmv_other_ing_dairy")+col("srmv_other_ing_alcohol")+col("srmv_other_ing_whiskey")+col("srmv_resin")+col("srmv_preform")+col("srmv_pet_bottles")+col("srmv_cans")+col("srmv_closures_crowns")+col("srmv_labels")+col("srmv_packaging_material_corrugate")+col("srmv_stretch_film")+col("srmv_shrink_film")+col("srmv_separator_sheets")+col("srmv_pet_plastic_handles")+col("srmv_bags")+col("srmv_other_packaging_material")+col("srmv_other_packaging_material_straw")+col("srmv_co2")+
                col("srmv_primary_packaging")+col("srmv_imp_mother_sku_upl"))\
    .withColumn("total_absorption",col("abs_excise_tax")+col("abs_tolling_fee_toller")+col("abs_tolling_fee_pet")+col("abs_pack_filling_oh_rm_specific")+col("abs_pack_filling_oh_rm_alloc")+col("abs_pack_filling_oh_var_specific")+col("abs_pack_filling_oh_var_alloc")+col("abs_pack_filling_oh_fixed_specific")+col("abs_pack_filling_oh_fixed_alloc")+col("abs_pack_filling_bc_pack")+col("abs_pack_filling_dep_pack_specific")+col("abs_pack_filling_dep_pack_allocated"))\
    .withColumn("total_variances",col("uv_total_cogs")+col("ppv_total_cogs")+col("srmv_total_cogs")+col("badj_concentrate"))\
    .withColumn("total_cogs_over_under",col("total_variances")+col("total_absorption"))\
    .withColumn("total_cogs_over_under_rate",col("total_cogs_over_under")/col("financial_vol_pc"))\
    .withColumn("total_cogs",col("std_total_cogs")+col("total_cogs_over_under"))\
    .withColumn("cogs_rate_total_cogs",col("total_cogs")/col("financial_vol_pc"))

# COMMAND ----------

# DBTITLE 1,define list of columns to be selected
ls = ['fisc_per', 'material', 'financial_vol_uc', 'financial_vol_pc', 'total_qty_uc', 'mix_total_uc', 'std_concentrate', 'mix_concentrate', 'uv_concentrate', 'ppv_concentrate', 'srmv_concentrate', 'total_var_concentrate', 'cogs_concentrate', 'cogs_rate_concentrate', 'std_dolphine_support', 'mix_dolphine_support', 'std_sugar_sweetner', 'mix_sugar_sweetener', 'uv_sugar_sweetener', 'ppv_sugar_sweetener', 'srmv_sugar_sweetener', 'total_var_sugar_sweetener', 'cogs_sugar_sweetener', 'cogs_rate_sugar_sweetener', 'std_compounding', 'mix_compounding', 'uv_compounding', 'ppv_compounding', 'srmv_compounding', 'total_var_compounding', 'cogs_compounding', 'cogs_rate_compounding', 'std_his_ace_k', 'mix_his_ace_k', 'uv_his_ace_k', 'ppv_his_ace_k', 'srmv_his_ace_k', 'total_var_his_ace_k', 'cogs_his_ace_k', 'cogs_rate_his_ace_k', 'std_other_ing', 'mix_other_ing', 'uv_other_ing', 'ppv_other_ing', 'srmv_other_ing', 'total_var_other_ing', 'cogs_other_ing', 'cogs_rate_other_ing', 'std_other_ing_tea', 'mix_other_ing_tea', 'uv_other_ing_tea', 'ppv_other_ing_tea', 'srmv_other_ing_tea', 'total_var_other_ing_tea', 'cogs_other_ing_tea', 'cogs_rate_other_ing_tea', 'std_other_ing_sweetner', 'mix_other_ing_sweetner', 'uv_other_ing_sweetner', 'ppv_other_ing_sweetner', 'srmv_other_ing_sweetner', 'total_var_other_ing_sweetner', 'cogs_other_ing_sweetner', 'cogs_rate_other_ing_sweetner', 'std_other_ing_dairy', 'mix_other_ing_dairy', 'uv_other_ing_dairy', 'ppv_other_ing_dairy', 'srmv_other_ing_dairy', 'total_var_other_ing_dairy', 'cogs_other_ing_dairy', 'cogs_rate_other_ing_dairy', 'std_other_ing_alcohol', 'mix_other_ing_alcohol', 'uv_other_ing_alcohol', 'ppv_other_ing_alcohol', 'srmv_other_ing_alcohol', 'total_var_other_ing_alcohol', 'cogs_other_ing_alcohol', 'cogs_rate_other_ing_alcohol', 'std_other_ing_whiskey', 'mix_other_ing_whiskey', 'uv_other_ing_whiskey', 'ppv_other_ing_whiskey', 'srmv_other_ing_whiskey', 'total_var_other_ing_whiskey', 'cogs_other_ing_whiskey', 'cogs_rate_other_ing_whiskey', 'std_resin', 'mix_resin', 'uv_resin', 'ppv_resin', 'srmv_resin', 'total_var_resin', 'cogs_resin', 'cogs_rate_resin', 'std_preform', 'mix_preform', 'uv_preform', 'ppv_preform', 'srmv_preform', 'total_var_preform', 'cogs_preform', 'cogs_rate_preform', 'std_pet_bottles', 'mix_pet_bottles', 'uv_pet_bottles', 'ppv_pet_bottles', 'srmv_pet_bottles', 'total_var_pet_bottles', 'cogs_pet_bottles', 'cogs_rate_pet_bottles', 'std_cans', 'mix_cans', 'uv_cans', 'ppv_cans', 'srmv_cans', 'total_var_cans', 'cogs_cans', 'cogs_rate_cans', 'std_closures_crowns', 'mix_closures_crowns', 'uv_closures_crowns', 'ppv_closures_crowns', 'srmv_closures_crowns', 'total_var_closures_crowns', 'cogs_closures_crowns', 'cogs_rate_closures_crowns', 'std_labels', 'mix_labels', 'uv_labels', 'ppv_labels', 'srmv_labels', 'total_var_labels', 'cogs_labels', 'cogs_rate_labels', 'std_packaging_material_corrugate', 'mix_packaging_material_corrugate', 'uv_packaging_material_corrugate', 'ppv_packaging_material_corrugate', 'srmv_packaging_material_corrugate', 'total_var_packaging_material_corrugate', 'cogs_packaging_material_corrugate', 'cogs_rate_packaging_material_corrugate', 'std_stretch_film', 'mix_stretch_film', 'uv_stretch_film', 'ppv_stretch_film', 'srmv_stretch_film', 'total_var_stretch_film', 'cogs_stretch_film', 'cogs_rate_stretch_film', 'std_shrink_film', 'mix_shrink_film', 'uv_shrink_film', 'ppv_shrink_film', 'srmv_shrink_film', 'total_var_shrink_film', 'cogs_shrink_film', 'cogs_rate_shrink_film', 'std_separator_sheets', 'mix_separator_sheets', 'uv_separator_sheets', 'ppv_separator_sheets', 'srmv_separator_sheets', 'total_var_separator_sheets', 'cogs_separator_sheets', 'cogs_rate_separator_sheets', 'std_pet_plastic_handles', 'mix_pet_plastic_handles', 'uv_pet_plastic_handles', 'ppv_pet_plastic_handles', 'srmv_pet_plastic_handles', 'total_var_pet_plastic_handles', 'cogs_pet_plastic_handles', 'cogs_rate_pet_plastic_handles', 'std_bags', 'mix_bags', 'uv_bags', 'ppv_bags', 'srmv_bags', 'total_var_bags', 'cogs_bags', 'cogs_rate_bags', 'std_other_packaging_material', 'mix_other_packaging_material', 'uv_other_packaging_material', 'ppv_other_packaging_material', 'srmv_other_packaging_material', 'total_var_other_packaging_material', 'cogs_other_packaging_material', 'cogs_rate_other_packaging_material', 'std_other_packaging_material_straw', 'mix_other_packaging_material_straw', 'uv_other_packaging_material_straw', 'ppv_other_packaging_material_straw', 'srmv_other_packaging_material_straw', 'total_var_other_packaging_material_straw', 'cogs_other_packaging_material_straw', 'cogs_rate_other_packaging_material_straw', 'std_co2', 'mix_co2', 'uv_co2', 'ppv_co2', 'srmv_co2', 'total_var_co2', 'cogs_co2', 'cogs_rate_co2', 'std_primary_packaging', 'mix_primary_packaging', 'uv_primary_packaging', 'ppv_primary_packaging', 'srmv_primary_packaging', 'total_var_primary_packaging', 'cogs_primary_packaging', 'cogs_rate_primary_packaging', 'std_repacking', 'mix_repacking', 'cogs_repacking', 'cogs_rate_repacking', 'std_excise_tax', 'mix_excise_tax', 'abs_excise_tax', 'cogs_excise_tax', 'cogs_rate_excise_tax', 'std_energy', 'mix_energy', 'cogs_energy', 'cogs_rate_energy', 'std_tolling_fee_toller', 'mix_tolling_fee_toller', 'abs_tolling_fee_toller', 'cogs_tolling_fee_toller', 'cogs_rate_tolling_fee_toller', 'std_tolling_fee_pet', 'mix_tolling_fee_pet', 'abs_tolling_fee_pet', 'cogs_tolling_fee_pet', 'cogs_rate_tolling_fee_pet', 'std_pack_filling_oh_rm_specific', 'mix_pack_filling_oh_rm_specific', 'abs_pack_filling_oh_rm_specific', 'cogs_pack_filling_oh_rm_specific', 'cogs_rate_pack_filling_oh_rm_specific', 'std_pack_filling_oh_rm_alloc', 'mix_pack_filling_oh_rm_alloc', 'abs_pack_filling_oh_rm_alloc', 'cogs_pack_filling_oh_rm_alloc', 'cogs_rate_pack_filling_oh_rm_alloc', 'std_pack_filling_oh_var_specific', 'mix_pack_filling_oh_var_specific', 'abs_pack_filling_oh_var_specific', 'cogs_pack_filling_oh_var_specific', 'cogs_rate_pack_filling_oh_var_specific', 'std_pack_filling_oh_var_alloc', 'mix_pack_filling_oh_var_alloc', 'abs_pack_filling_oh_var_alloc', 'cogs_pack_filling_oh_var_alloc', 'cogs_rate_pack_filling_oh_var_alloc', 'std_pack_filling_oh_fixed_specific', 'mix_pack_filling_oh_fixed_specific', 'abs_pack_filling_oh_fixed_specific', 'cogs_pack_filling_oh_fixed_specific', 'cogs_rate_pack_filling_oh_fixed_specific', 'std_pack_filling_oh_fixed_alloc', 'mix_pack_filling_oh_fixed_alloc', 'abs_pack_filling_oh_fixed_alloc', 'cogs_pack_filling_oh_fixed_alloc', 'cogs_rate_pack_filling_oh_fixed_alloc', 'std_pack_filling_bc_pack', 'mix_pack_filling_bc_pack', 'abs_pack_filling_bc_pack', 'cogs_pack_filling_bc_pack', 'cogs_rate_pack_filling_bc_pack', 'std_pack_filling_dep_pack_specific', 'mix_pack_filling_dep_pack_specific', 'abs_pack_filling_dep_pack_specific', 'cogs_pack_filling_dep_pack_specific', 'cogs_rate_pack_filling_dep_pack_specific', 'std_pack_filling_dep_pack_allocated', 'mix_pack_filling_dep_pack_allocated', 'abs_pack_filling_dep_pack_allocated', 'cogs_pack_filling_dep_pack_allocated', 'cogs_rate_pack_filling_dep_pack_allocated', 'std_distilling_cost', 'mix_distilling_cost', 'cogs_distilling_cost', 'cogs_rate_distilling_cost', 'std_imp_mother_sku_upl', 'mix_imp_mother_sku_upl', 'srmv_imp_mother_sku_upl', 'cogs_imp_mother_sku_upl', 'cogs_rate_imp_mother_sku_upl', 'std_management_fee', 'mix_management_fee', 'cogs_management_fee', 'cogs_rate_management_fee', 'std_haulage', 'mix_haulage', 'cogs_haulage', 'cogs_rate_haulage', 'std_total_cogs', 'mix_total_cogs', 'uv_total_cogs', 'ppv_total_cogs', 'srmv_total_cogs', 'total_absorption', 'total_variances', 'total_cogs_over_under', 'total_cogs_over_under_rate', 'total_cogs', 'cogs_rate_total_cogs']

df_vol_mix_mat_final = df_vol_mix_mat_totals.select(*ls)

# COMMAND ----------

# DBTITLE 1,display columns that contain a specific word
# # Filter columns containing specific words
# filter_columns = [col_name for col_name in df_vol_mix_mat_final.columns if "bottles" in col_name]

# # Create a dictionary of select expressions for the filtered columns
# select_exprs = {col_name: col(col_name) for col_name in filter_columns}

# # show selected columns
# display(df_vol_mix_mat_final.select("material","financial_vol_pc","marketing_vol_pc", *[col(c) for c in filter_columns]))

# COMMAND ----------

# DBTITLE 1,sum up all columns that contain a specific word
# # Filter columns containing the word "uv_"
# sum_columns = [col_name for col_name in df_vol_mix_mat_final.columns if "abs_" in col_name or "absorption" in col_name]

# # Create a dictionary of aggregate expressions for the filtered columns
# agg_exprs = {col_name: sum(col(col_name)).alias(f"sum_{col_name}") for col_name in sum_columns}

# # Perform the aggregation and display the result
# display(df_vol_mix_mat_final.agg(*[sum(col(c)).alias(f"sum_{c}") for c in sum_columns]))

# COMMAND ----------

# DBTITLE 1,apply consistent precision and scale to decimal types
# Define the desired precision and scale
target_precision = 38
target_scale = 18
target_decimal_type = DecimalType(target_precision, target_scale)



# # Get all columns and their types
decimal_columns = [
    field.name
    for field in df_vol_mix_mat_final.schema.fields
    if isinstance(field.dataType, DecimalType)
]

# Construct the expressions for casting decimal columns
select_expr = [
    f"CAST({col} AS DECIMAL({target_precision}, {target_scale})) AS {col}"
    if col in decimal_columns else col
    for col in df_vol_mix_mat_final.columns
]

# Apply the changes using selectExpr
df_vol_mix_mat_final = df_vol_mix_mat_final.selectExpr(*select_expr)

# COMMAND ----------

display(df_vol_mix_mat_final.filter(col("material") == '101677'))

# COMMAND ----------

# DBTITLE 1,check COGS Variances totals after allocation
# from pyspark.sql.functions import sum, round, col
# display(df_vol_mix_mat_final.groupBy("fisc_per").agg( \
#     sum("mix_total_uc"), round(sum("uv_concentrate"),2).alias("uv_concentrate"), round(sum("ppv_concentrate"),2).alias("ppv_concentrate"), round(sum("srmv_concentrate"),2).alias("srmv_concentrate")) \
#     .join(df_ppv.select("fisc_per", col("concentrate").alias("ppv_total")), "fisc_per", "inner") \
#     .join(df_uv.select("fisc_per", col("concentrate").alias("uv_total")), "fisc_per", "inner") \
#     .join(df_srmv.select("fisc_per", col("concentrate").alias("srmv_total")), "fisc_per", "inner") \
#     .withColumn("ppv_diff", col("ppv_total") - col("ppv_concentrate")) \
#     .withColumn("uv_diff", col("uv_total") - col("uv_concentrate")) \
#     .withColumn("srmv_diff", col("srmv_total") - col("srmv_concentrate"))
#         )

# COMMAND ----------

# MAGIC %md
# MAGIC ### create target table if not exists

# COMMAND ----------

create_query = f"""
create table if not exists {gold_schema}.profitability.vol_mix_mat
(fisc_per string COMMENT 'Fiscal Year and Period in SAP Database Format YYYYPPP',
material string COMMENT 'SAP Material Code',
financial_vol_uc DECIMAL(38,18),
financial_vol_pc DECIMAL(38,18),
total_qty_uc DECIMAL(38,18),
mix_total_uc DECIMAL(38,18),
std_concentrate DECIMAL(38,18),
mix_concentrate DECIMAL(38,18),
uv_concentrate DECIMAL(38,18),
ppv_concentrate DECIMAL(38,18),
srmv_concentrate DECIMAL(38,18),
total_var_concentrate DECIMAL(38,18),
cogs_concentrate DECIMAL(38,18),
cogs_rate_concentrate DECIMAL(38,18),
std_dolphine_support DECIMAL(38,18),
mix_dolphine_support DECIMAL(38,18),
std_sugar_sweetner DECIMAL(38,18),
mix_sugar_sweetener DECIMAL(38,18),
uv_sugar_sweetener DECIMAL(38,18),
ppv_sugar_sweetener DECIMAL(38,18),
srmv_sugar_sweetener DECIMAL(38,18),
total_var_sugar_sweetener DECIMAL(38,18),
cogs_sugar_sweetener DECIMAL(38,18),
cogs_rate_sugar_sweetener DECIMAL(38,18),
std_compounding DECIMAL(38,18),
mix_compounding DECIMAL(38,18),
uv_compounding DECIMAL(38,18),
ppv_compounding DECIMAL(38,18),
srmv_compounding DECIMAL(38,18),
total_var_compounding DECIMAL(38,18),
cogs_compounding DECIMAL(38,18),
cogs_rate_compounding DECIMAL(38,18),
std_his_ace_k DECIMAL(38,18),
mix_his_ace_k DECIMAL(38,18),
uv_his_ace_k DECIMAL(38,18),
ppv_his_ace_k DECIMAL(38,18),
srmv_his_ace_k DECIMAL(38,18),
total_var_his_ace_k DECIMAL(38,18),
cogs_his_ace_k DECIMAL(38,18),
cogs_rate_his_ace_k DECIMAL(38,18),
std_other_ing DECIMAL(38,18),
mix_other_ing DECIMAL(38,18),
uv_other_ing DECIMAL(38,18),
ppv_other_ing DECIMAL(38,18),
srmv_other_ing DECIMAL(38,18),
total_var_other_ing DECIMAL(38,18),
cogs_other_ing DECIMAL(38,18),
cogs_rate_other_ing DECIMAL(38,18),
std_other_ing_tea DECIMAL(38,18),
mix_other_ing_tea DECIMAL(38,18),
uv_other_ing_tea DECIMAL(38,18),
ppv_other_ing_tea DECIMAL(38,18),
srmv_other_ing_tea DECIMAL(38,18),
total_var_other_ing_tea DECIMAL(38,18),
cogs_other_ing_tea DECIMAL(38,18),
cogs_rate_other_ing_tea DECIMAL(38,18),
std_other_ing_sweetner DECIMAL(38,18),
mix_other_ing_sweetner DECIMAL(38,18),
uv_other_ing_sweetner DECIMAL(38,18),
ppv_other_ing_sweetner DECIMAL(38,18),
srmv_other_ing_sweetner DECIMAL(38,18),
total_var_other_ing_sweetner DECIMAL(38,18),
cogs_other_ing_sweetner DECIMAL(38,18),
cogs_rate_other_ing_sweetner DECIMAL(38,18),
std_other_ing_dairy DECIMAL(38,18),
mix_other_ing_dairy DECIMAL(38,18),
uv_other_ing_dairy DECIMAL(38,18),
ppv_other_ing_dairy DECIMAL(38,18),
srmv_other_ing_dairy DECIMAL(38,18),
total_var_other_ing_dairy DECIMAL(38,18),
cogs_other_ing_dairy DECIMAL(38,18),
cogs_rate_other_ing_dairy DECIMAL(38,18),
std_other_ing_alcohol DECIMAL(38,18),
mix_other_ing_alcohol DECIMAL(38,18),
uv_other_ing_alcohol DECIMAL(38,18),
ppv_other_ing_alcohol DECIMAL(38,18),
srmv_other_ing_alcohol DECIMAL(38,18),
total_var_other_ing_alcohol DECIMAL(38,18),
cogs_other_ing_alcohol DECIMAL(38,18),
cogs_rate_other_ing_alcohol DECIMAL(38,18),
std_other_ing_whiskey DECIMAL(38,18),
mix_other_ing_whiskey DECIMAL(38,18),
uv_other_ing_whiskey DECIMAL(38,18),
ppv_other_ing_whiskey DECIMAL(38,18),
srmv_other_ing_whiskey DECIMAL(38,18),
total_var_other_ing_whiskey DECIMAL(38,18),
cogs_other_ing_whiskey DECIMAL(38,18),
cogs_rate_other_ing_whiskey DECIMAL(38,18),
std_resin DECIMAL(38,18),
mix_resin DECIMAL(38,18),
uv_resin DECIMAL(38,18),
ppv_resin DECIMAL(38,18),
srmv_resin DECIMAL(38,18),
total_var_resin DECIMAL(38,18),
cogs_resin DECIMAL(38,18),
cogs_rate_resin DECIMAL(38,18),
std_preform DECIMAL(38,18),
mix_preform DECIMAL(38,18),
uv_preform DECIMAL(38,18),
ppv_preform DECIMAL(38,18),
srmv_preform DECIMAL(38,18),
total_var_preform DECIMAL(38,18),
cogs_preform DECIMAL(38,18),
cogs_rate_preform DECIMAL(38,18),
std_pet_bottles DECIMAL(38,18),
mix_pet_bottles DECIMAL(38,18),
uv_pet_bottles DECIMAL(38,18),
ppv_pet_bottles DECIMAL(38,18),
srmv_pet_bottles DECIMAL(38,18),
total_var_pet_bottles DECIMAL(38,18),
cogs_pet_bottles DECIMAL(38,18),
cogs_rate_pet_bottles DECIMAL(38,18),
std_cans DECIMAL(38,18),
mix_cans DECIMAL(38,18),
uv_cans DECIMAL(38,18),
ppv_cans DECIMAL(38,18),
srmv_cans DECIMAL(38,18),
total_var_cans DECIMAL(38,18),
cogs_cans DECIMAL(38,18),
cogs_rate_cans DECIMAL(38,18),
std_closures_crowns DECIMAL(38,18),
mix_closures_crowns DECIMAL(38,18),
uv_closures_crowns DECIMAL(38,18),
ppv_closures_crowns DECIMAL(38,18),
srmv_closures_crowns DECIMAL(38,18),
total_var_closures_crowns DECIMAL(38,18),
cogs_closures_crowns DECIMAL(38,18),
cogs_rate_closures_crowns DECIMAL(38,18),
std_labels DECIMAL(38,18),
mix_labels DECIMAL(38,18),
uv_labels DECIMAL(38,18),
ppv_labels DECIMAL(38,18),
srmv_labels DECIMAL(38,18),
total_var_labels DECIMAL(38,18),
cogs_labels DECIMAL(38,18),
cogs_rate_labels DECIMAL(38,18),
std_packaging_material_corrugate DECIMAL(38,18),
mix_packaging_material_corrugate DECIMAL(38,18),
uv_packaging_material_corrugate DECIMAL(38,18),
ppv_packaging_material_corrugate DECIMAL(38,18),
srmv_packaging_material_corrugate DECIMAL(38,18),
total_var_packaging_material_corrugate DECIMAL(38,18),
cogs_packaging_material_corrugate DECIMAL(38,18),
cogs_rate_packaging_material_corrugate DECIMAL(38,18),
std_stretch_film DECIMAL(38,18),
mix_stretch_film DECIMAL(38,18),
uv_stretch_film DECIMAL(38,18),
ppv_stretch_film DECIMAL(38,18),
srmv_stretch_film DECIMAL(38,18),
total_var_stretch_film DECIMAL(38,18),
cogs_stretch_film DECIMAL(38,18),
cogs_rate_stretch_film DECIMAL(38,18),
std_shrink_film DECIMAL(38,18),
mix_shrink_film DECIMAL(38,18),
uv_shrink_film DECIMAL(38,18),
ppv_shrink_film DECIMAL(38,18),
srmv_shrink_film DECIMAL(38,18),
total_var_shrink_film DECIMAL(38,18),
cogs_shrink_film DECIMAL(38,18),
cogs_rate_shrink_film DECIMAL(38,18),
std_separator_sheets DECIMAL(38,18),
mix_separator_sheets DECIMAL(38,18),
uv_separator_sheets DECIMAL(38,18),
ppv_separator_sheets DECIMAL(38,18),
srmv_separator_sheets DECIMAL(38,18),
total_var_separator_sheets DECIMAL(38,18),
cogs_separator_sheets DECIMAL(38,18),
cogs_rate_separator_sheets DECIMAL(38,18),
std_pet_plastic_handles DECIMAL(38,18),
mix_pet_plastic_handles DECIMAL(38,18),
uv_pet_plastic_handles DECIMAL(38,18),
ppv_pet_plastic_handles DECIMAL(38,18),
srmv_pet_plastic_handles DECIMAL(38,18),
total_var_pet_plastic_handles DECIMAL(38,18),
cogs_pet_plastic_handles DECIMAL(38,18),
cogs_rate_pet_plastic_handles DECIMAL(38,18),
std_bags DECIMAL(38,18),
mix_bags DECIMAL(38,18),
uv_bags DECIMAL(38,18),
ppv_bags DECIMAL(38,18),
srmv_bags DECIMAL(38,18),
total_var_bags DECIMAL(38,18),
cogs_bags DECIMAL(38,18),
cogs_rate_bags DECIMAL(38,18),
std_other_packaging_material DECIMAL(38,18),
mix_other_packaging_material DECIMAL(38,18),
uv_other_packaging_material DECIMAL(38,18),
ppv_other_packaging_material DECIMAL(38,18),
srmv_other_packaging_material DECIMAL(38,18),
total_var_other_packaging_material DECIMAL(38,18),
cogs_other_packaging_material DECIMAL(38,18),
cogs_rate_other_packaging_material DECIMAL(38,18),
std_other_packaging_material_straw DECIMAL(38,18),
mix_other_packaging_material_straw DECIMAL(38,18),
uv_other_packaging_material_straw DECIMAL(38,18),
ppv_other_packaging_material_straw DECIMAL(38,18),
srmv_other_packaging_material_straw DECIMAL(38,18),
total_var_other_packaging_material_straw DECIMAL(38,18),
cogs_other_packaging_material_straw DECIMAL(38,18),
cogs_rate_other_packaging_material_straw DECIMAL(38,18),
std_co2 DECIMAL(38,18),
mix_co2 DECIMAL(38,18),
uv_co2 DECIMAL(38,18),
ppv_co2 DECIMAL(38,18),
srmv_co2 DECIMAL(38,18),
total_var_co2 DECIMAL(38,18),
cogs_co2 DECIMAL(38,18),
cogs_rate_co2 DECIMAL(38,18),
std_primary_packaging DECIMAL(38,18),
mix_primary_packaging DECIMAL(38,18),
uv_primary_packaging DECIMAL(38,18),
ppv_primary_packaging DECIMAL(38,18),
srmv_primary_packaging DECIMAL(38,18),
total_var_primary_packaging DECIMAL(38,18),
cogs_primary_packaging DECIMAL(38,18),
cogs_rate_primary_packaging DECIMAL(38,18),
std_repacking DECIMAL(38,18),
mix_repacking DECIMAL(38,18),
cogs_repacking DECIMAL(38,18),
cogs_rate_repacking DECIMAL(38,18),
std_excise_tax DECIMAL(38,18),
mix_excise_tax DECIMAL(38,18),
abs_excise_tax DECIMAL(38,18),
cogs_excise_tax DECIMAL(38,18),
cogs_rate_excise_tax DECIMAL(38,18),
std_energy DECIMAL(38,18),
mix_energy DECIMAL(38,18),
cogs_energy DECIMAL(38,18),
cogs_rate_energy DECIMAL(38,18),
std_tolling_fee_toller DECIMAL(38,18),
mix_tolling_fee_toller DECIMAL(38,18),
abs_tolling_fee_toller DECIMAL(38,18),
cogs_tolling_fee_toller DECIMAL(38,18),
cogs_rate_tolling_fee_toller DECIMAL(38,18),
std_tolling_fee_pet DECIMAL(38,18),
mix_tolling_fee_pet DECIMAL(38,18),
abs_tolling_fee_pet DECIMAL(38,18),
cogs_tolling_fee_pet DECIMAL(38,18),
cogs_rate_tolling_fee_pet DECIMAL(38,18),
std_pack_filling_oh_rm_specific DECIMAL(38,18),
mix_pack_filling_oh_rm_specific DECIMAL(38,18),
abs_pack_filling_oh_rm_specific DECIMAL(38,18),
cogs_pack_filling_oh_rm_specific DECIMAL(38,18),
cogs_rate_pack_filling_oh_rm_specific DECIMAL(38,18),
std_pack_filling_oh_rm_alloc DECIMAL(38,18),
mix_pack_filling_oh_rm_alloc DECIMAL(38,18),
abs_pack_filling_oh_rm_alloc DECIMAL(38,18),
cogs_pack_filling_oh_rm_alloc DECIMAL(38,18),
cogs_rate_pack_filling_oh_rm_alloc DECIMAL(38,18),
std_pack_filling_oh_var_specific DECIMAL(38,18),
mix_pack_filling_oh_var_specific DECIMAL(38,18),
abs_pack_filling_oh_var_specific DECIMAL(38,18),
cogs_pack_filling_oh_var_specific DECIMAL(38,18),
cogs_rate_pack_filling_oh_var_specific DECIMAL(38,18),
std_pack_filling_oh_var_alloc DECIMAL(38,18),
mix_pack_filling_oh_var_alloc DECIMAL(38,18),
abs_pack_filling_oh_var_alloc DECIMAL(38,18),
cogs_pack_filling_oh_var_alloc DECIMAL(38,18),
cogs_rate_pack_filling_oh_var_alloc DECIMAL(38,18),
std_pack_filling_oh_fixed_specific DECIMAL(38,18),
mix_pack_filling_oh_fixed_specific DECIMAL(38,18),
abs_pack_filling_oh_fixed_specific DECIMAL(38,18),
cogs_pack_filling_oh_fixed_specific DECIMAL(38,18),
cogs_rate_pack_filling_oh_fixed_specific DECIMAL(38,18),
std_pack_filling_oh_fixed_alloc DECIMAL(38,18),
mix_pack_filling_oh_fixed_alloc DECIMAL(38,18),
abs_pack_filling_oh_fixed_alloc DECIMAL(38,18),
cogs_pack_filling_oh_fixed_alloc DECIMAL(38,18),
cogs_rate_pack_filling_oh_fixed_alloc DECIMAL(38,18),
std_pack_filling_bc_pack DECIMAL(38,18),
mix_pack_filling_bc_pack DECIMAL(38,18),
abs_pack_filling_bc_pack DECIMAL(38,18),
cogs_pack_filling_bc_pack DECIMAL(38,18),
cogs_rate_pack_filling_bc_pack DECIMAL(38,18),
std_pack_filling_dep_pack_specific DECIMAL(38,18),
mix_pack_filling_dep_pack_specific DECIMAL(38,18),
abs_pack_filling_dep_pack_specific DECIMAL(38,18),
cogs_pack_filling_dep_pack_specific DECIMAL(38,18),
cogs_rate_pack_filling_dep_pack_specific DECIMAL(38,18),
std_pack_filling_dep_pack_allocated DECIMAL(38,18),
mix_pack_filling_dep_pack_allocated DECIMAL(38,18),
abs_pack_filling_dep_pack_allocated DECIMAL(38,18),
cogs_pack_filling_dep_pack_allocated DECIMAL(38,18),
cogs_rate_pack_filling_dep_pack_allocated DECIMAL(38,18),
std_distilling_cost DECIMAL(38,18),
mix_distilling_cost DECIMAL(38,18),
cogs_distilling_cost DECIMAL(38,18),
cogs_rate_distilling_cost DECIMAL(38,18),
std_imp_mother_sku_upl DECIMAL(38,18),
mix_imp_mother_sku_upl DECIMAL(38,18),
srmv_imp_mother_sku_upl DECIMAL(38,18),
cogs_imp_mother_sku_upl DECIMAL(38,18),
cogs_rate_imp_mother_sku_upl DECIMAL(38,18),
std_management_fee DECIMAL(38,18),
mix_management_fee DECIMAL(38,18),
cogs_management_fee DECIMAL(38,18),
cogs_rate_management_fee DECIMAL(38,18),
std_haulage DECIMAL(38,18),
mix_haulage DECIMAL(38,18),
cogs_haulage DECIMAL(38,18),
cogs_rate_haulage DECIMAL(38,18),
std_total_cogs DECIMAL(38,18),
mix_total_cogs DECIMAL(38,18),
uv_total_cogs DECIMAL(38,18),
ppv_total_cogs DECIMAL(38,18),
srmv_total_cogs DECIMAL(38,18),
total_absorption DECIMAL(38,18),
total_variances DECIMAL(38,18),
total_cogs_over_under DECIMAL(38,18),
total_cogs_over_under_rate DECIMAL(38,18),
total_cogs DECIMAL(38,18),
cogs_rate_total_cogs DECIMAL(38,18))
COMMENT 'contains the volume, volume mix and all the calculated COGS components by material in each period.'
CLUSTER BY (fisc_per)
"""

spark.sql(create_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### delete existing data for specified periods

# COMMAND ----------

delete_query = f"""
DELETE FROM {gold_schema}.profitability.vol_mix_mat
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(delete_query)

# COMMAND ----------

# display(df_vol_mix_mat_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write newly calculated data for specified periods

# COMMAND ----------

if df_vol_mix_mat_final.groupBy("fisc_per","material").count().filter("count > 1").count() > 0:
    raise ValueError("Duplicate values found in 'id' column. Cannot proceed.")
else:
    df_vol_mix_mat_final.write \
        .mode('append') \
        .option("mergeSchema", "true") \
        .saveAsTable(f'{gold_schema}.profitability.vol_mix_mat')
