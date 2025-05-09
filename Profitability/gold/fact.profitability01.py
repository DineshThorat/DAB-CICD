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

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

query = f"""
DELETE FROM {gold_schema}.fact.profitability01
WHERE FiscalPeriod >= '{v_start_fisc_per}'
  AND FiscalPeriod <= '{v_end_fisc_per}'
"""

spark.sql(query)

# COMMAND ----------


#################################################################################################################################
############################################## Import Liabraries   ##############################################################
#################################################################################################################################
import pandas as pd
import os
from datetime import datetime,date
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, expr, when, lit
from pyspark.sql.types import StructType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DateType,DoubleType,BooleanType,DecimalType
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

df_copa_items = spark.table(f'{gold_schema}.fact.copa_items').filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

df_final = df_copa_items.select("fisc_per",col("mktg_vol_pc"),"mktg_vol_uc","mktg_vol_fpc",col("free_goods_vol_pc").cast('decimal(38,18)'),"free_goods_vol_uc","free_goods_vol_fpc","financial_vol_uc",col("financial_vol_pc").cast('decimal(38,18)'),col("financial_vol_fpc").cast('decimal(38,18)'),col("revenue_adj").cast('decimal(38,18)'),col("base_price_adj").cast('decimal(38,18)'),col("excise_from_copa").cast('decimal(38,18)'),col("Excise_Tax_Std").cast('decimal(38,18)'),col("gross_revenue").cast('decimal(38,18)'),col("base_price").cast('decimal(38,18)'),col("base_price_excl_excise").cast('decimal(38,18)'),col("base_price_adj_w_ipp").cast('decimal(38,18)'),"ipp_discount",col("gross_wholesale_price").cast('decimal(38,18)'),col("promo_disc_oninv").cast('decimal(38,18)'),col("fixed_disc_oninv").cast('decimal(38,18)'),col("variable_disc_oninv").cast('decimal(38,18)'),col("pickup_disc_oninv").cast('decimal(38,18)'),col("total_disc_oninv").cast('decimal(38,18)'),col("total_disc_offinv").cast('decimal(38,18)'),col("dfr_cr_dr_memos").cast('decimal(38,18)'),col("dfr_fixed_disc").cast('decimal(38,18)'),col("dfr_from_rms").cast('decimal(38,18)'),col("dfr_variable_disc").cast('decimal(38,18)'),col("dfr_pickup_disc").cast('decimal(38,18)'),col("dfr_promo_disc").cast('decimal(38,18)'),col("dfr_del_pen_disc").cast('decimal(38,18)'),col("dfr_fallout").cast('decimal(38,18)'),col("dfr_dmi").cast('decimal(38,18)'),col("dfr_dmi_fallout").cast('decimal(38,18)'),col("dfr_ctm").cast('decimal(38,18)'),"dfr_sales_curve",col("ipp_equip_rental").cast('decimal(38,18)'),col("ipp_freight_charge").cast('decimal(38,18)'),col("other_rev_cust").cast('decimal(38,18)'),col("other_rev").cast('decimal(38,18)'),col("other_rev_non_cust").cast('decimal(38,18)'),col("misc_revenue").cast('decimal(38,18)'),col("discounts_not_used").cast('decimal(38,18)'),col("total_discounts").cast('decimal(38,18)'),col("nsr_after_curve").cast('decimal(38,18)'),col("nsr_before_curve").cast('decimal(38,18)'),col("revenue_after_curve").cast('decimal(38,18)'),col("revenue_before_curve").cast('decimal(38,18)'),"nka_discount","nsr_base_incidence","cogs_concentrate","cogs_concentrate_tccc","cogs_concentrate_nontccc","Commodities_Std_Cost","Conversion_Std_Cost","cogs_under_over","haulage_std","haulage_total","total_cogs",col("delivery_std").cast('decimal(38,18)'),col("delivery_total").cast('decimal(38,18)'),col("delivery_under_over").cast('decimal(38,18)'),col("warehouse_std").cast('decimal(38,18)'),col("warehouse_total").cast('decimal(38,18)'),col("warehouse_under_over").cast('decimal(38,18)'),"logistics_opex","cogs_concentrate_total", "cogs_sweetener_total", "cogs_compounding_total", "cogs_his_ace_k_total", "cogs_other_ing_total", "cogs_other_ing_tea_total", "cogs_other_ing_sweetener_total", "cogs_other_ing_dairy_total", "cogs_other_ing_alcohol_total", "cogs_other_ing_whiskey_total", "cogs_resin_total", "cogs_preform_total", "cogs_pet_bottles_total", "cogs_cans_total", "cogs_closures_crowns_total", "cogs_labels_total", "cogs_packaging_material_corrugate_total", "cogs_stretch_film_total", "cogs_shrink_film_total", "cogs_separator_sheets_total", "cogs_pet_plastic_handles_total", "cogs_bags_total", "cogs_other_packaging_material_total", "cogs_other_packaging_material_straw_total", "cogs_co2_total", "cogs_primary_packaging_total", "cogs_repacking_total", "cogs_packaging_materials_total", "cogs_commodities_total", "cogs_excise_total", "cogs_energy_total", "cogs_tolling_fee_toller_total", "cogs_tolling_fee_pet_total","cogs_pack_filling_oh_rm_specific_total", "cogs_pack_filling_oh_rm_alloc_total", "cogs_pack_filling_oh_var_specific_total", "cogs_pack_filling_oh_var_alloc_total", "cogs_pack_filling_bc_pack_total", "cogs_distilling_cost_total", "cogs_variable_total", "cogs_imp_mother_sku_upl_total", "cogs_pack_filling_oh_fixed_specific_total", "cogs_pack_filling_oh_fixed_alloc_total", "cogs_pack_filling_dep_pack_specific_total", "cogs_pack_filling_dep_pack_allocated_total", "cogs_fixed_total", "cogs_conversion_cost_total", "haulage_under_over", "total_cogs_portfolio",col("tme").cast('decimal(38,18)'),col("tme_curve").cast('decimal(38,18)'),col("tme_incl_curve").cast('decimal(38,18)'),col("selling_expenses").cast('decimal(38,18)'),"operating_profit","customer","material").withColumn("scenario", lit("Actuals"))#.withColumn("TotalRevenueUC",((col("gross_revenue") - (col("total_disc_oninv") + col("total_disc_offinv"))+ col("other_rev"))/col("mktg_vol_uc")).cast('decimal(38,18)'))

# COMMAND ----------

# existing_columns = df_final.columns
# missing_columns = [col for col in selected_columns if col not in existing_columns]

# COMMAND ----------

# for col in missing_columns:
#     df_final = df_final.withColumn(col, lit(0).cast(DecimalType(38, 18)))

# COMMAND ----------

# df_final = df_copa_items.select("fisc_per",col("mktg_vol_pc").cast('decimal(38,18)'),"mktg_vol_uc",col("free_goods_vol_pc").cast('decimal(38,18)'),"free_goods_vol_uc",col("excise_from_copa").cast('decimal(38,18)'),col("excise_from_cogs").cast('decimal(38,18)'),col("gross_revenue").cast('decimal(38,18)'),col("base_price").cast('decimal(38,18)'),col("base_price_excl_excise").cast('decimal(38,18)'),col("base_price_adj_w_ipp").cast('decimal(38,18)'),col("ipp_discount").cast('decimal(38,18)'),col("promo_disc_oninv").cast('decimal(38,18)'),col("fixed_disc_oninv").cast('decimal(38,18)'),col("variable_disc_oninv").cast('decimal(38,18)'),col("pickup_disc_oninv").cast('decimal(38,18)'),col("total_disc_oninv").cast('decimal(38,18)'),col("dfr_cr_dr_memos").cast('decimal(38,18)'),col("dfr_fixed_disc").cast('decimal(38,18)'),col("dfr_from_rms").cast('decimal(38,18)'),col("dfr_variable_disc").cast('decimal(38,18)'),col("dfr_promo_disc").cast('decimal(38,18)'),col("dfr_del_pen_disc").cast('decimal(38,18)'),col("dfr_fallout").cast('decimal(38,18)'),col("dfr_dmi").cast('decimal(38,18)'),col("dfr_dmi_fallout").cast('decimal(38,18)'),col("ipp_equip_rental").cast('decimal(38,18)'),col("ipp_freight_charge").cast('decimal(38,18)'),col("other_rev_cust").cast('decimal(38,18)'),col("cogs_concentrate"),col("cogs_commodities"),"cogs_conversion", "cogs_sweetener", "cogs_compounding", "cogs_his_ace_k", "other_ing", "other_ing_tea", "other_ing_sweetner", "other_ing_dairy", "other_ing_alcohol", "other_ing_whiskey", "resin", "preform", "pet_bottels", "cans", "closures_crows", "labels", "packaging_material_corrugate", "stretch_film", col("shrink_film").cast('decimal(38,18)'), "separator_sheets_layer_pads", "pet_plastic_handles", "bags", "other_pack_material", "other_pack_material_straw", "co2", "primary_packaging", "repacking", "management_fee", "distilling_cost", "energy", "tolling_fee_toller", "tolling_fee_pet", "imp_mother_sku_upl", "pack_filling_oh_rm_specific", "pack_filling_oh_rm_alloc", "pack_filling_oh_var_specific", "pack_filling_oh_var_alloc", "pack_filling_dep_pack_specific", "pack_filling_bc_pack", "pack_filling_oh_fixed_alloc", "pack_filling_oh_fixed_specific", "pack_filling_dep_pack_allocated", "haulage",col("dfr_ctm").cast('decimal(38,18)'),"dfr_sales_curve", "total_disc_offinv", col("other_rev").cast('decimal(38,18)'), col("other_rev_non_cust").cast('decimal(38,18)'), col("nsr_base_incidence").cast('decimal(38,18)'), "cogs_insidence", "cogs_under_over", col("cogs_concentrate_tccc").cast('decimal(38,18)'), col("cogs_concentrate_nontccc").cast('decimal(38,18)'), col("cogs_incidence_adj").cast('decimal(38,18)'),"customer","material").withColumn("scenario", lit("Actuals"))

# COMMAND ----------

selected_columns = ['mktg_vol_pc', 'mktg_vol_uc', 'mktg_vol_fpc', 'free_goods_vol_pc', 'free_goods_vol_uc', 'free_goods_vol_fpc', 'financial_vol_uc', 'financial_vol_pc', 'financial_vol_fpc', 'revenue_adj', 'base_price_adj', 'excise_from_copa', 'Excise_Tax_Std', 'gross_revenue', 'base_price', 'base_price_excl_excise', 'base_price_adj_w_ipp', 'ipp_discount', 'gross_wholesale_price', 'promo_disc_oninv', 'fixed_disc_oninv', 'variable_disc_oninv', 'pickup_disc_oninv', 'total_disc_oninv', 'total_disc_offinv', 'dfr_cr_dr_memos', 'dfr_fixed_disc', 'dfr_from_rms', 'dfr_variable_disc', 'dfr_pickup_disc', 'dfr_promo_disc', 'dfr_del_pen_disc', 'dfr_fallout', 'dfr_dmi', 'dfr_dmi_fallout','dfr_ctm','dfr_sales_curve', 'ipp_equip_rental', 'ipp_freight_charge', 'other_rev_cust', 'other_rev', 'other_rev_non_cust', 'misc_revenue', 'discounts_not_used', 'total_discounts', 'nsr_after_curve', 'nsr_before_curve', 'revenue_after_curve', 'revenue_before_curve', 'nka_discount', 'nsr_base_incidence', 'cogs_concentrate', 'cogs_concentrate_tccc', 'cogs_concentrate_nontccc', 'Commodities_Std_Cost', 'Conversion_Std_Cost', 'cogs_under_over', 'haulage_std', 'haulage_total', 'total_cogs', 'delivery_std', 'delivery_total', 'delivery_under_over', 'warehouse_std', 'warehouse_total', 'warehouse_under_over', 'logistics_opex', 'cogs_concentrate_total', 'cogs_sweetener_total', 'cogs_compounding_total', 'cogs_his_ace_k_total', 'cogs_other_ing_total', 'cogs_other_ing_tea_total', 'cogs_other_ing_sweetener_total', 'cogs_other_ing_dairy_total', 'cogs_other_ing_alcohol_total', 'cogs_other_ing_whiskey_total', 'cogs_resin_total', 'cogs_preform_total', 'cogs_pet_bottles_total', 'cogs_cans_total', 'cogs_closures_crowns_total', 'cogs_labels_total', 'cogs_packaging_material_corrugate_total', 'cogs_stretch_film_total', 'cogs_shrink_film_total', 'cogs_separator_sheets_total', 'cogs_pet_plastic_handles_total', 'cogs_bags_total', 'cogs_other_packaging_material_total', 'cogs_other_packaging_material_straw_total', 'cogs_co2_total', 'cogs_primary_packaging_total', 'cogs_repacking_total', 'cogs_packaging_materials_total', 'cogs_commodities_total', 'cogs_excise_total', 'cogs_energy_total', 'cogs_tolling_fee_toller_total', 'cogs_tolling_fee_pet_total', 'cogs_pack_filling_oh_rm_specific_total', 'cogs_pack_filling_oh_rm_alloc_total', 'cogs_pack_filling_oh_var_specific_total', 'cogs_pack_filling_oh_var_alloc_total', 'cogs_pack_filling_bc_pack_total', 'cogs_distilling_cost_total', 'cogs_variable_total', 'cogs_imp_mother_sku_upl_total', 'cogs_pack_filling_oh_fixed_specific_total', 'cogs_pack_filling_oh_fixed_alloc_total', 'cogs_pack_filling_dep_pack_specific_total', 'cogs_pack_filling_dep_pack_allocated_total', 'cogs_fixed_total', 'cogs_conversion_cost_total', 'haulage_under_over', 'total_cogs_portfolio','tme','tme_curve','tme_incl_curve','selling_expenses','operating_profit']

# COMMAND ----------

num_columns = len(selected_columns)
stack_expr = f"stack({num_columns}, " + ", ".join([f"'{col}', {col}" for col in selected_columns]) + ") as (profit_loss_item, amount)"

# COMMAND ----------

df_exchange_rates  = spark.sql("""select exchange_rate
 from dev_silver.ecc.exchange_rates where valid_date = (select PeriodStart from dev_gold.common_dim.fisc_per where FiscalPeriod >= '{v_start_fisc_per}' AND FiscalPeriod <= '{v_end_fisc_per}') and from_currency ="PHP" and to_currency = "EUR" and exchange_rate_type = "EURX" """)

# v_exchange_rate = df_exchange_rates.collect()[0]['exchange_rate']
# print(v_exchange_rate)

# COMMAND ----------

from pyspark.sql.functions import col, expr, when, lit

# COMMAND ----------

transposed_df = df_final.select(col("fisc_per").alias("FiscalPeriod"),"scenario", expr(stack_expr),col("customer").alias("CustomerNo"),col("material").alias("MaterialNo"))\
    .withColumn("profit_loss_component",
                when(col("profit_loss_item").isin('mktg_vol_pc','mktg_vol_uc','mktg_vol_fpc','free_goods_vol_pc','free_goods_vol_uc','free_goods_vol_fpc','financial_vol_pc','financial_vol_fpc','financial_vol_uc'), lit("Volume"))\
                .when(col("profit_loss_item").isin("excise_from_copa","excise_from_cogs","gross_revenue","base_price","base_price_excl_excise","base_price_adj_w_ipp","ipp_discount","gross_wholesale_price"), lit("Gross Revenue")))\
    .withColumn("volume",when(col("profit_loss_item").isin('mktg_vol_pc','mktg_vol_uc','mktg_vol_fpc','free_goods_vol_pc','free_goods_vol_uc','free_goods_vol_fpc','financial_vol_pc','financial_vol_fpc','financial_vol_uc'), col("amount")))\
    .withColumn("amount_lc",when(~col("profit_loss_item").isin('mktg_vol_pc','mktg_vol_uc','mktg_vol_fpc','free_goods_vol_pc','free_goods_vol_uc','free_goods_vol_fpc','financial_vol_pc','financial_vol_fpc','financial_vol_uc'), col("amount")))\
    .withColumn("currency_lc",when(~col("profit_loss_item").isin('mktg_vol_pc','mktg_vol_uc','mktg_vol_fpc','free_goods_vol_pc','free_goods_vol_uc','free_goods_vol_fpc','financial_vol_pc','financial_vol_fpc','financial_vol_uc'), lit("PHP")))\
    .withColumn("amount_gc",col("amount_lc") )\
    .withColumn("currency_gc",when(~col("profit_loss_item").isin('mktg_vol_pc','mktg_vol_uc','mktg_vol_fpc','free_goods_vol_pc','free_goods_vol_uc','free_goods_vol_fpc','financial_vol_pc','financial_vol_fpc','financial_vol_uc'), lit("EUR"))).drop("amount")

# COMMAND ----------

transposed_df.write.partitionBy('FiscalPeriod').mode('append').saveAsTable('dev_gold.fact.profitability01')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select FiscalPeriod,count(*) from dev_gold.fact.profitability01
# MAGIC group by FiscalPeriod

# COMMAND ----------

# #using csv file

# df = spark.read.csv('/FileStore/pl_hierarchy_2.csv', header=True)
# df.display()

# COMMAND ----------

# df.write.mode('overwrite').saveAsTable('dev_gold.common_dim.pl_hierarchy')
