# Databricks notebook source
# MAGIC %md
# MAGIC ## Define Widgets

# COMMAND ----------

dbutils.widgets.text("start_fisc_per", "", "")
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
dbutils.widgets.text("end_fisc_per", "", "")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("year from fisc_per", "", "")
v_fisc_year = dbutils.widgets.get("year from fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")
print(Env)

# COMMAND ----------

# MAGIC %run
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/common_parameters

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

# MAGIC %md
# MAGIC ## Create Copa Items, Result, Vol Mix Custmat, Standard Cogs DF

# COMMAND ----------

df_copa_items = spark.table(f"""{silver_schema}.ecc.copa_items""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per) & (col('material_type') != 'ZEMP')).select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","unit_case_qty","physical_case_qty","revenue","reserve_92","trade_promo", "rebates_incl", "accruals_incl","other_surcharge","fuel_surcharge", "net_share", "cogs_conctr_tccc", "cogs_conctr_nontccc", "plant")

df_result = df_copa_items.groupBy("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel", "physical_case_qty", "plant").agg(sum("unit_case_qty").alias("sum_unit_case_qty_per_line").cast('decimal(38,18)'),sum(when(~col("material").isin('101209', '101210', '109800'), col("physical_case_qty").cast('decimal(38,18)'))
.otherwise(0.0)).alias("sum_physical_case_qty_per_line").cast('decimal(38,18)'),sum(when(col("sales_doc_type").isin('YDME','ZMKT'), col("unit_case_qty")).otherwise(0)).alias("free_goods_sum_unit_case_qty_per_line"),sum(when(col("sales_doc_type").isin('YDME','ZMKT'), col("physical_case_qty")).otherwise(0)).alias("free_goods_sum_physical_case_qty_per_line"),sum("revenue").alias("sum_revenue_per_line").cast('decimal(25,2)'),sum("reserve_92").alias("sum_reserve_92").cast('decimal(20,5)'),sum("trade_promo").alias("sum_trade_promo").cast('decimal(20,5)'), (sum("rebates_incl") + sum("accruals_incl")).alias("rebates_accruals_total").cast('decimal(25,3)'),sum("other_surcharge").alias("sum_other_surcharge").cast('decimal(25,3)'),sum("fuel_surcharge").alias("sum_fuel_surcharge").cast('decimal(25,3)'), sum("net_share").alias("sum_net_share"), sum("cogs_conctr_tccc").alias("sum_cogs_conctr_tccc"), sum("cogs_conctr_nontccc").alias("sum_cogs_conctr_nontccc"))

df_vol_mix_custmat = spark.table(f"""{gold_schema}.profitability.vol_mix_custmat""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

df_std_cogs = spark.table(f"""{silver_schema}.sharepoint.standard_cogs""").filter((col("fisc_year") == v_fisc_year))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### ylp2 Dataframe

# COMMAND ----------

df_ylp2 = spark.sql(f"""
                    select cond.bill_docnum, cond.material, sum(cast(cond.condition_val as decimal(38, 10))) as ylp2_amount
                    from {silver_schema}.bw.billing_conditions cond
                    inner join {silver_schema}.bw.billing_items items on cond.bill_docnum = items.bill_docnum and cond.bill_itemnum = items.bill_itemnum
                    where items.posting_status = '{df_ylp2_items_posting_status}' and cond.condition_type = '{df_ylp2_cond_condition_type}'
                    and items.fisc_per >= {v_start_fisc_per} and items.fisc_per <= {v_end_fisc_per}
                    group by cond.bill_docnum, cond.material
                    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### On Invoice Discounts Dataframe

# COMMAND ----------

df_on_inv_disc = spark.sql(f"""
                           SELECT bi.fisc_per, bi.bill_docnum, bi.material, bi.outlet, bc.condition_type as condition_type_oninv, sum(CAST(bc.condition_val AS decimal(25,3))) AS total_condition_val
                           from {silver_schema}.bw.billing_conditions bc
                           INNER JOIN {silver_schema}.bw.billing_items bi
                           ON
                           bc.bill_docnum = bi.bill_docnum AND
                           bc.bill_itemnum = bi.bill_itemnum
                           WHERE bc.condition_type IN {df_on_inv_disc_bc_condition_type} AND
                           bi.posting_status = '{df_on_inv_disc_bc_posting_status}' AND
                           bi.fisc_per >={v_start_fisc_per} and bi.fisc_per <={v_end_fisc_per}
                           GROUP BY bi.fisc_per, bi.bill_docnum, bi.material, bc.condition_type, bi.outlet
                           """)

# COMMAND ----------

# Fixed Discount
df_fixed_disc_oninv = df_on_inv_disc.groupBy("fisc_per", "bill_docnum", "material").agg(sum(when(col("condition_type_oninv").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type_oninv").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))

# Variable Discount
df_var_disc_oninv = df_on_inv_disc.filter(col("condition_type_oninv")=='ZA01').groupBy('fisc_per','outlet').agg(sum('total_condition_val').alias('var_disc_amount'))

# Pickup Discount
df_pickup_disc = df_on_inv_disc.groupBy("fisc_per", "bill_docnum", "material").agg(sum(when(col("condition_type_oninv").isin("YLP9"), col("total_condition_val") / vat).otherwise(0)).alias('pickup_disc_amount'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### IPP Dataframe

# COMMAND ----------

df_ipp = spark.sql(f"""
                  select * from {silver_schema}.files.ipp
                  where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Off Invoice Dataframe

# COMMAND ----------

df_off_inv_disc = spark.sql(f"""
    SELECT 
        fisc_per, 
        customer, 
        material, 
        SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN amount ELSE 0 END) AS fixed_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN amount ELSE 0 END) AS var_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN amount ELSE 0 END) AS pickup_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Promo' THEN amount ELSE 0 END) AS promo_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Delivery Penalty' THEN amount ELSE 0 END) AS del_pen_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR Fallout' THEN amount ELSE 0 END) AS dfr_fallout_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR DMI' THEN amount ELSE 0 END) AS dmi_amnt_off_inv,
        SUM(CASE WHEN disc_type = 'DFR DMI Fallout' THEN amount ELSE 0 END) AS dmi_fallout_amnt_off_inv
    FROM
        {silver_schema}.files.off_invoice_discounts
    WHERE 
        fisc_per >= {v_start_fisc_per} 
        AND fisc_per <= {v_end_fisc_per}
    GROUP BY
        fisc_per, customer, material
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cr Dr Memos Dataframe

# COMMAND ----------

df_cr_dr_memos_hist = spark.sql(f"""
                           select fisc_per, customer, material,
                           SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN amount ELSE 0 END) AS fixed_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN amount ELSE 0 END) AS var_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN amount ELSE 0 END) AS pickup_amnt_cr_dr_hist,
                           SUM(CASE WHEN disc_type = 'DFR Promo' THEN amount ELSE 0 END) AS promo_amnt_cr_dr_hist,
                           SUM(amount) AS total_cr_dr_amnt_hist
                           from
                           {silver_schema}.files.cr_dr_memos
                           where fisc_per >= {v_start_fisc_per} 
                           AND fisc_per <= {v_end_fisc_per}
                           group by fisc_per, customer, material
                           """)

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
# MAGIC ### Miscellaneous_revnue Dataframe 

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
                             select del_loc_code,pack,pack_size,quantity as quantity_haulage,haulage_cost_pc from dev_silver.sharepoint.haulage_costs
                             where fisc_year = {v_fisc_year}
                             """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calucation Methods

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
                   select adj.fisc_per, pc.trade_grp, sum(adj.amnt_gross_rev) as revenue_adj
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
    SUM(CASE WHEN disc_type = 'DFR Fixed Discount' THEN adj.amnt_total_disc ELSE 0 END) AS dfr_fixed_disc,
    SUM(CASE WHEN disc_type = 'DFR Variable Discount' THEN adj.amnt_total_disc ELSE 0 END) AS dfr_variable_disc,
    SUM(CASE WHEN disc_type = 'DFR Pick-up Discount' THEN adj.amnt_total_disc ELSE 0 END) AS dfr_pickup_disc,
    SUM(CASE WHEN disc_type = 'DFR Promo' THEN adj.amnt_total_disc ELSE 0 END) AS dfr_promo_disc,
    SUM(CASE WHEN disc_type = 'DFR DMI' THEN adj.amnt_total_disc ELSE 0 END) AS dfr_dmi
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
# MAGIC ## Initial Processiong for Reusable Measures

# COMMAND ----------

#Joining condition
cond_copa_custmat = [df_result.fisc_per == df_vol_mix_custmat.fisc_per,df_result.customer == df_vol_mix_custmat.customer, df_result.material == df_vol_mix_custmat.material]

# COMMAND ----------

Window_spec_LVM = Window.partitionBy('fisc_per', 'customer', 'material')

# COMMAND ----------

df_result_custmat = df_result.join(df_vol_mix_custmat, cond_copa_custmat ,'left').drop(df_vol_mix_custmat["material"],df_vol_mix_custmat["customer"],df_vol_mix_custmat["fisc_per"])
## STD COGS ##                           
df_final_1 = df_result_custmat.join(df_std_cogs, df_result_custmat.material == df_std_cogs.material ,how='left_outer').filter(df_result_custmat['material'] != '109580').drop(df_std_cogs["material"], df_std_cogs["fisc_year"]).withColumn("custmat_vol_uc", sum(col("physical_case_qty")*col("conv_to_unit_case") * col("conv_full_case")).over(Window_spec_LVM)).withColumn("line_vol_mix", ((col("physical_case_qty")*col("conv_to_unit_case") * col("conv_full_case"))/ col("custmat_vol_uc")).cast('decimal(38,18)'))

## IPP ##
df_final_2 = df_final_1.join(df_ipp, (df_final_1["fisc_per"] == df_ipp["fisc_per"]) & (df_final_1["customer"] == df_ipp["customer"]) &
(df_final_1["material"] == df_ipp["material"]),how='left_outer')

col_drop_frm_ipp = ['fisc_per', 'material', 'fisc_year', 'customer', 'fisc_per_num', 'material_desc', 'loaded_dt', 'source_file_name', 'input_file_modification_time']
df_final_2 = df_final_2.drop(*[df_ipp[col] for col in col_drop_frm_ipp])

## YLP2 ##
df_final_3 = df_final_2.join(df_ylp2, (df_final_2["ref_docnum"] == df_ylp2["bill_docnum"]) & (df_final_2["material"] == df_ylp2["material"]),how='left_outer').drop(df_ylp2["material"], df_ylp2["bill_docnum"])

## FIXED DISC ON INV ##

df_final_4 = df_final_3.join(df_fixed_disc_oninv, (df_final_3["ref_docnum"]==df_fixed_disc_oninv["bill_docnum"])&(df_final_3["material"]==df_fixed_disc_oninv["material"]), how = 'left_outer').drop(df_fixed_disc_oninv["bill_docnum"], df_fixed_disc_oninv["material"], df_fixed_disc_oninv["fisc_per"])

## VAR DISC ON INV ##
df_final_5 = df_final_4.join(df_var_disc_oninv, (df_final_4["fisc_per"]==df_var_disc_oninv["fisc_per"])&(df_final_4["customer"]==df_var_disc_oninv["outlet"]), how='left_outer').drop(df_var_disc_oninv["fisc_per"], df_var_disc_oninv["outlet"])

## PICKUP DISC ON INV ##

df_final_6 = df_final_5.join(df_pickup_disc, (df_final_5["ref_docnum"]==df_pickup_disc["bill_docnum"])&(df_final_5["material"]==df_pickup_disc["material"]), how='left_outer').drop(df_pickup_disc["bill_docnum"], df_pickup_disc["material"], df_pickup_disc["fisc_per"])

## OFF INV DISC ##

df_final_7 = df_final_6.join(df_off_inv_disc, (df_final_6["material"] == df_off_inv_disc["material"]) & (df_final_6["fisc_per"] == df_off_inv_disc["fisc_per"]) & (df_final_6["customer"] == df_off_inv_disc["customer"]), how = 'left_outer')
col_drop_frm_off_inv_disc = ['fisc_per', 'material', 'customer']
df_final_7 = df_final_7.drop(*[df_off_inv_disc[col] for col in col_drop_frm_off_inv_disc])

## CR DR MEMOS HIST ##

df_final_8 = df_final_7.join(df_cr_dr_memos_hist, (df_final_7["fisc_per"]==df_cr_dr_memos_hist["fisc_per"])&(df_final_7["customer"]==df_cr_dr_memos_hist["customer"])&(df_final_7["material"]==df_cr_dr_memos_hist["material"]), how='left_outer').drop(df_cr_dr_memos_hist["fisc_per"], df_cr_dr_memos_hist["customer"], df_cr_dr_memos_hist["material"])

## CR DR MEMOS CUR ##

df_final_9 = df_final_8.join(df_cr_dr_memos_cur,[(df_final_8["fisc_per"]== df_cr_dr_memos_cur["fisc_per"]) & (df_final_8["customer"]== df_cr_dr_memos_cur["customer"])], how='left_outer').drop(df_cr_dr_memos_cur["fisc_per"], df_cr_dr_memos_cur["customer"])

# GL DFR CURVE ##
df_final_10 = df_final_9.join(df_gl_dfr_curve, df_final_9["fisc_per"]==df_gl_dfr_curve["fisc_per"], how = 'left_outer').drop(df_gl_dfr_curve["fisc_per"])

## DFR RMS TOTAL ##
df_final_11 = df_final_10.join(dfr_rms_total, (df_final_10["ref_docnum"]==dfr_rms_total["bill_docnum"])&(df_final_10["material"]==dfr_rms_total["material"]), how = 'left_outer').drop(dfr_rms_total["bill_docnum"], dfr_rms_total["material"])


## VAR DISC RMS ##
df_final_12 = df_final_11.join(df_var_disc_rms , (df_final_11["ref_docnum"]==df_var_disc_rms["bill_docnum"])&(df_final_11["material"]==df_var_disc_rms["material"]), how = 'left_outer').drop(df_var_disc_rms["bill_docnum"], df_var_disc_rms["material"])


## DMI RMS ##
df_final_13 = df_final_12.join(df_dmi_rms, (df_final_12["ref_docnum"]==df_dmi_rms["bill_docnum"])&(df_final_12["material"]==df_dmi_rms["material"]), how = 'left_outer').drop(df_dmi_rms["bill_docnum"], df_dmi_rms["material"])

## Current Outlet ##
df_final_14 = df_final_13.join(df_curr_outlet, (df_final_13["customer"] == df_curr_outlet["CustomerNo"]), how = 'left_outer').drop(df_curr_outlet["CustomerNo"])

## Material ##
df_final = df_final_14.join(df_material, (df_final_14["material"] == df_material["MaterialNo"]), how='left_outer').drop(df_material["MaterialNo"])


# COMMAND ----------

df_final = df_final.unionByName(df_rev_adj, allowMissingColumns=True).unionByName(df_adj, allowMissingColumns=True).unionByName(df_gl_other_rev,  allowMissingColumns=True).unionByName(df_gl_ctm, allowMissingColumns=True).unionByName(df_dnu,allowMissingColumns=True).unionByName(df_misc_rev, allowMissingColumns=True)

# COMMAND ----------

# Read 
df_delivery_rates = select del_loc_code, package, pack_size, bottles_per_case, delivery_cost_pc 
from dev_silver.sharepoint.delivery_costs 
where fisc_year = {{ year from end_fisc_per }} 

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

df_final = df_final.withColumn("base_price_adj", (col("ylp2_amount") / vat).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price_excl_excise", (col("sum_net_share") - col("base_price_adj")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("excise_from_copa", col("sum_reserve_92").cast('decimal(25,2)')).na.fill(0.00) \
                   .withColumn("excise_from_cogs", (col("financial_vol_pc") * col("excise_tax_pc")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price", ((col("base_price_excl_excise")) + col("excise_from_copa") - col("revenue_adj")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn('ipp_discount', (col('line_vol_mix') * col("amount")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("base_price_adj_w_ipp", (col("base_price_adj") + col("ipp_discount")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("gross_revenue", (col("base_price") + col("base_price_adj_w_ipp")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("gross_wholesale_price", (col("gross_revenue")*vat).cast('decimal(25,5)')).na.fill(0.00)

# COMMAND ----------

# MAGIC %md
# MAGIC ## On Invoice Discounts

# COMMAND ----------

df_final = df_final.withColumn("fixed_disc_oninv", col("total_amount").cast('decimal(38,18)')) \
                   .withColumn("variable_disc_oninv",((col('var_disc_amount')*col('line_vol_mix'))/vat).cast('decimal(25,3)')) \
                   .withColumn("pickup_disc_oninv", (col("pickup_disc_amount")).cast('decimal(25,3)')) \
                   .withColumn("promo_disc_oninv", col("sum_trade_promo").cast('decimal(25,3)')) \
                   .withColumn("total_disc_oninv",(col('fixed_disc_oninv')+col('variable_disc_oninv')+col('promo_disc_oninv')+col('pickup_disc_oninv')).cast('decimal(25,3)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Off Invoice Discounts

# COMMAND ----------

## LATEST
df_final = df_final.withColumn("dfr_cr_dr_memos", ((col("line_vol_mix")*col("total_cr_dr_amnt_hist"))+(col("mix_cust_total_uc")*col("line_vol_mix")*col("total_cr_dr_amnt_cur"))).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_fixed_disc", ((col("line_vol_mix")*col("fixed_amnt_off_inv"))+(col("line_vol_mix")*col("fixed_amnt_cr_dr_hist"))+(col("mix_cust_total_uc")*col("line_vol_mix")*col("fixed_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')).na.fill(0.00)\
                   .withColumn("dfr_from_rms", col("dfr_rms_amnt").cast('decimal(25,3)')) \
                   .withColumn("dfr_variable_disc", ((col("line_vol_mix")*col("var_amnt_off_inv"))+(col("line_vol_mix")*col("var_amnt_cr_dr_hist"))+(col("mix_cust_total_uc")*col("line_vol_mix")*col("var_amnt_cr_dr_memos_cur"))+col("var_disc_rms_amt")).cast('decimal(38,18)')).na.fill(0.00) \
                   .withColumn("dfr_pickup_disc",((col("line_vol_mix")*col("pickup_amnt_off_inv"))+(col("line_vol_mix")*col("pickup_amnt_cr_dr_hist"))+(col("mix_cust_total_uc")*col("line_vol_mix")*col("pickup_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')) \
                   .withColumn("dfr_promo_disc",((col("line_vol_mix")*col("promo_amnt_off_inv"))+(col("line_vol_mix")*col("promo_amnt_cr_dr_hist"))+(col("mix_cust_total_uc")*col("line_vol_mix")*col("promo_amnt_cr_dr_memos_cur"))).cast('decimal(38,18)')) \
                   .withColumn("dfr_del_pen_disc",(col("line_vol_mix")*col("del_pen_amnt_off_inv")).cast('decimal(38,18)')) \
                   .withColumn("dfr_fallout",(col("line_vol_mix")*col("dfr_fallout_amnt_off_inv")).cast('decimal(38,18)')) \
                   .withColumn("dfr_dmi",((col("line_vol_mix")*col("dmi_amnt_off_inv"))+col("dmi_rms_amt")).cast('decimal(38,18)')) \
                   .withColumn("dfr_dmi_fallout",(col("line_vol_mix")*col("dmi_fallout_amnt_off_inv")).cast('decimal(38,18)')) \
                   .withColumn("dfr_ctm", col("amount_gl_ctm").cast('decimal(25,2)')) \
                   .withColumn("dfr_sales_curve", (col('line_vol_mix')*col('dfr_curve_amnt')).cast('decimal(38,18)')) \
                   .withColumn("total_disc_offinv", col('dfr_fixed_disc')+col('dfr_from_rms')+col('dfr_variable_disc')+col('dfr_pickup_disc')+col('dfr_promo_disc')+col('dfr_del_pen_disc')+col('dfr_fallout')+col('dfr_dmi')+col('dfr_dmi_fallout')+col('dfr_ctm')+col('dfr_sales_curve')).na.fill(0.00)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Other Revenue

# COMMAND ----------

df_final = df_final.withColumn("ipp_equip_rental",col("sum_other_surcharge").cast('decimal(25,2)')) \
                   .withColumn("ipp_freight_charge",col("sum_fuel_surcharge").cast('decimal(25,2)')) \
                   .withColumn("other_rev_cust", (col("ipp_equip_rental") + col("ipp_freight_charge")).cast('decimal(25,2)'))\
                   .withColumn("other_rev", col('amount_gl_other_rev').cast('decimal(25,2)')) \
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

# MAGIC %md
# MAGIC ## COGS for Profitability P&L

# COMMAND ----------

## Haulage Costs ##
df_final = df_final.join(df_haulage_rates, (df_final["plant"]==df_haulage_rates["del_loc_code"])&(df_final["Package"]==df_haulage_rates["pack"])&(df_final["PackSize"]==upper(df_haulage_rates["pack_size"]))&(df_final["Quantity"]==df_haulage_rates["quantity_haulage"]), how = 'left_outer')

# COMMAND ----------

df_vol_mix_mat = spark.sql(f""" select *, financial_vol_pc as financial_vol_pc_vmm from {gold_schema}.profitability.vol_mix_mat""").filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

df_final = df_final.join(df_vol_mix_mat, (df_final["fisc_per"]==df_vol_mix_mat["fisc_per"])&(df_final["material"]==df_vol_mix_mat["material"]), how = 'left_outer').drop(df_vol_mix_mat["fisc_per"], df_vol_mix_mat["material"],df_vol_mix_mat["financial_vol_pc"])

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

df_final = df_final.join(df_haulage, (df_final["fisc_per"]==df_haulage["fisc_per"]), how='left_outer').drop(df_haulage["fisc_per"])

# COMMAND ----------

df_final = df_final\
                    .withColumn("nka_discount", when(col("FISC_PER") < '2024001',when(col("RSM_MTMCutDesc").contains("NKA"),col("total_disc_oninv") - col("dfr_fixed_disc") - col("dfr_variable_disc") - col("dfr_pickup_disc") - col("dfr_promo_disc")).otherwise(0)).otherwise(0))\
                    .withColumn("nsr_base_incidence",when(col("FISC_PER") < '2024001',col("gross_revenue") - col("excise_from_cogs") - (0.093 * (col("gross_revenue") - col("excise_from_cogs"))) - col("nka_discount")).otherwise( col("gross_revenue") - col("excise_from_cogs") - col("total_disc_oninv") - col("dfr_fixed_disc") - col("dfr_variable_disc") - col("dfr_pickup_disc") - col("dfr_promo_disc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate", (when((col("incidence_rate_pc")==0),(col("concentrate_pc")+col("management_fee_pc"))*col("financial_vol_pc")).otherwise(col("nsr_base_incidence")*col("incidence_rate_pc"))).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_tccc", col("sum_cogs_conctr_tccc").cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_nontccc", col("sum_cogs_conctr_nontccc").cast('decimal(38,18)'))\
                    .withColumn("cogs_commodities",(col("commodities_cogs_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_conversion", (col("conversion_cost_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))

# COMMAND ----------

Window_spec_haulage_total = Window.partitionBy('fisc_per')

# COMMAND ----------

df_final = df_final.withColumn("nka_discount", (when((col("fisc_per") < '2024001') & (col("RSM_MTMCutDesc").contains("NKA")),(col("total_disc_oninv") - col("fixed_disc_oninv") - col("variable_disc_oninv") - col("pickup_disc_oninv") - col("promo_disc_oninv"))).otherwise(0)).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("nsr_base_incidence", (when(col("fisc_per") >= '2024001', (col("gross_revenue") - col("excise_from_cogs") - col("total_disc_oninv") - col("fixed_disc_oninv") - col("variable_disc_oninv") - col("pickup_disc_oninv") - col("promo_disc_oninv"))).otherwise((col("gross_revenue") - col("excise_from_cogs") - 0.093 * (col("gross_revenue") - col("excise_from_cogs")) - col("nka_discount")))).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_concentrate", (when((col("incidence_rate_pc")==0),(col("concentrate_pc")+col("management_fee_pc"))*df_final.financial_vol_pc).otherwise(col("nsr_base_incidence")*col("incidence_rate_pc"))).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_concentrate_tccc", col("sum_cogs_conctr_tccc").cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_concentrate_nontccc", col("sum_cogs_conctr_nontccc").cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_commodities",(col("commodities_cogs_pc")*df_final.financial_vol_pc).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_conversion", (col("conversion_cost_pc")*df_final.financial_vol_pc).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("cogs_under_over", (df_vol_mix_mat.total_cogs_over_under_rate * df_final.financial_vol_pc).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("haulage_std", (col("mktg_vol_pc")*col("haulage_cost_pc")).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("total_haulage_std", (sum("haulage_std").over(Window_spec_haulage_total))).na.fill(0)\
                    .withColumn("haulage_mix", col("haulage_std")/col("total_haulage_std")).na.fill(0)\
                    .withColumn("haulage_total", (col("total_costs")*col("haulage_mix")).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("total_cogs", (col("cogs_concentrate")+col("cogs_commodities")+col("excise_from_cogs")+col("cogs_conversion")+col("cogs_under_over")+col("haulage_total")).cast('decimal(38,18)')).na.fill(0)

# COMMAND ----------

# MAGIC %md <b><h1> Delivery Std Cost 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dev_silver.sharepoint.delivery_costs
# MAGIC df_delivery_rates = select del_loc_code
# MAGIC , pack, pack_size, quantity as quantity_delivery, delivery_cost_pc 
# MAGIC from silver.sharepoint.delivery_costs 
# MAGIC where fisc_year = {v_fisc_year}

# COMMAND ----------

# MAGIC %md
# MAGIC ## COGS for Portfolio P&L

# COMMAND ----------

# df_final = df_final.withColumn("cogs_concentrate", (col("mktg_vol_pc")*df_std_cogs.concentrate_pc).cast('decimal(38,18)')) \
#                   .withColumn("cogs_compounding",(col("mktg_vol_pc")*df_std_cogs.compounding_pc).cast('decimal(38,18)')) \
#                   .withColumn("cogs_sweetener",(col("mktg_vol_pc")*df_std_cogs.sugar_sweetner_pc).cast('decimal(38,18)')) \
#                   .withColumn("cogs_his_ace_k",(col("mktg_vol_pc")*df_std_cogs.his_ace_k_pc).cast('decimal(38,18)')) \
#                   .withColumn("other_ing",(col("mktg_vol_pc")*df_std_cogs.other_ing_pc).cast('decimal(38,18)')) \
#                   .withColumn("other_ing_tea",(col("mktg_vol_pc")*df_std_cogs.other_ing_tea_pc).cast('decimal(38,18)')) \
#                   .withColumn("other_ing_sweetner",(col("mktg_vol_pc")*df_std_cogs.other_ing_sweetner_pc).cast('decimal(38,18)'))\
#                   .withColumn("other_ing_dairy",(col("mktg_vol_pc")*df_std_cogs.other_ing_dairy_pc).cast('decimal(38,18)')) \
#                   .withColumn("other_ing_alcohol",(col("mktg_vol_pc")*df_std_cogs.other_ing_alcohol_pc).cast('decimal(38,18)')) \
#                   .withColumn("other_ing_whiskey",(col("mktg_vol_pc")*df_std_cogs.other_ing_whiskey_pc).cast('decimal(38,18)')) \
#                   .withColumn("resin",(col("mktg_vol_pc")*df_std_cogs.resin_pc).cast('decimal(38,18)')) \
#                   .withColumn("preform",(col("mktg_vol_pc")*df_std_cogs.preform_pc).cast('decimal(38,18)')) \
#                   .withColumn("pet_bottels",(col("mktg_vol_pc")*df_std_cogs.pet_bottles_pc).cast('decimal(38,18)')) \
#                   .withColumn("cans",(col("mktg_vol_pc")*df_std_cogs.cans_pc).cast('decimal(38,18)')) \
#                   .withColumn("closures_crows",(col("mktg_vol_pc")*df_std_cogs.closures_crowns_pc).cast('decimal(38,18)')) \
#                   .withColumn("labels",(col("mktg_vol_pc")*df_std_cogs.labels_pc).cast('decimal(38,18)')) \
#                   .withColumn("packaging_material_corrugate",(col("mktg_vol_pc")*df_std_cogs.packaging_material_corrugate_pc).cast('decimal(38,18)')) \
#                   .withColumn("stretch_film",(col("mktg_vol_pc")*df_std_cogs.stretch_film_pc).cast('decimal(38,18)')) \
#                 .withColumn("shrink_film",(col("mktg_vol_pc")*df_std_cogs.shrink_film_pc).cast('decimal(38,18)')) \
#                 .withColumn("separator_sheets_layer_pads",(col("mktg_vol_pc")*df_std_cogs.separator_sheets_pc).cast('decimal(38,18)')) \
#                 .withColumn("pet_plastic_handles",(col("mktg_vol_pc")*df_std_cogs.pet_plastic_handles_pc).cast('decimal(38,18)')) \
#                 .withColumn("bags",(col("mktg_vol_pc")*df_std_cogs.bags_pc).cast('decimal(38,18)')) \
#                 .withColumn("other_pack_material",(col("mktg_vol_pc")*df_std_cogs.other_packaging_material_pc).cast('decimal(38,18)')) \
#                 .withColumn("other_pack_material_straw",(col("mktg_vol_pc")*df_std_cogs.other_packaging_material_straw_pc).cast('decimal(38,18)')) \
#                 .withColumn("co2",(col("mktg_vol_pc")*df_std_cogs.co2_pc).cast('decimal(38,18)')) \
#                 .withColumn("primary_packaging",(col("mktg_vol_pc")*df_std_cogs.primary_packaging_pc).cast('decimal(38,18)')) \
#                 .withColumn("repacking",(col("mktg_vol_pc")*df_std_cogs.repacking_pc).cast('decimal(38,18)'))\
#                 .withColumn("management_fee",(col("mktg_vol_pc")*df_std_cogs.management_fee_pc).cast('decimal(38,18)')) \
#                 .withColumn("distilling_cost",(col("mktg_vol_pc")*df_std_cogs.distilling_cost_pc).cast('decimal(38,18)')) \
#                 .withColumn("energy",(col("mktg_vol_pc")*df_std_cogs.energy_pc).cast('decimal(38,18)')) \
#                 .withColumn("tolling_fee_toller",(col("mktg_vol_pc")*df_std_cogs.tolling_fee_toller_pc).cast('decimal(38,18)')) \
#                 .withColumn("tolling_fee_pet",(col("mktg_vol_pc")*df_std_cogs.tolling_fee_pet_pc).cast('decimal(38,18)')) \
#                 .withColumn("imp_mother_sku_upl",(col("mktg_vol_pc")*df_std_cogs.imp_mother_sku_upl_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_rm_specific",(col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_rm_specific_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_rm_alloc",(col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_rm_alloc_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_var_specific",(col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_var_specific_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_var_alloc",(col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_var_alloc_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_fixed_specific", (col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_fixed_specific_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_dep_pack_specific",(col("mktg_vol_pc")*df_std_cogs.pack_filling_dep_pack_specific_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_oh_fixed_alloc", (col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_fixed_alloc_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_bc_pack",(col("mktg_vol_pc")*df_std_cogs.pack_filling_bc_pack_pc).cast('decimal(38,18)')) \
#                 .withColumn("pack_filling_dep_pack_allocated",(col("mktg_vol_pc")*df_std_cogs.pack_filling_dep_pack_allocated_pc).cast('decimal(38,18)')) \
#                 .withColumn("haulage",(col("mktg_vol_pc")*df_std_cogs.haulage_pc).cast('decimal(38,18)'))

# COMMAND ----------

# df_final_copa_items = df_final.select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type","reserve_92", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc", "base_price_adj", "base_price_excl_excise", "excise_from_copa", "excise_from_cogs", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "promo_disc_oninv", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout", "ipp_equip_rental", "ipp_freight_charge", "other_rev_cust","cogs_concentrate", "cogs_commodities", "cogs_conversion", "cogs_sweetener", "cogs_compounding", "cogs_his_ace_k", "other_ing", "other_ing_tea", "other_ing_sweetner", "other_ing_dairy", "other_ing_alcohol", "other_ing_whiskey", "resin", "preform", "pet_bottels", "cans", "closures_crows", "labels", "packaging_material_corrugate", "stretch_film", "shrink_film", "separator_sheets_layer_pads", "pet_plastic_handles", "bags", "other_pack_material", "other_pack_material_straw", "co2", "primary_packaging", "repacking", "management_fee", "distilling_cost", "energy", "tolling_fee_toller", "tolling_fee_pet", "imp_mother_sku_upl", "pack_filling_oh_rm_specific", "pack_filling_oh_rm_alloc", "pack_filling_oh_var_specific", "pack_filling_oh_var_alloc", "pack_filling_dep_pack_specific", "pack_filling_bc_pack", "pack_filling_oh_fixed_alloc", "pack_filling_oh_fixed_specific", "pack_filling_dep_pack_allocated", "haulage", "dfr_ctm", "dfr_sales_curve", "total_disc_offinv", "other_rev", "other_rev_non_cust", "nsr_base_incidence", "cogs_insidence", "cogs_under_over", "cogs_concentrate_tccc", "cogs_concentrate_nontccc", "cogs_incidence_adj")

# COMMAND ----------

df_final_copa_items = df_final.select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc","free_goods_vol_fpc","financial_vol_uc","financial_vol_pc","financial_vol_fpc", "revenue_adj","base_price_adj", "base_price_excl_excise", "excise_from_copa", "excise_from_cogs", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "gross_wholesale_price","total_discounts","nsr_after_curve","nsr_before_curve","revenue_after_curve","revenue_before_curve", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "promo_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout", "dfr_ctm", "dfr_sales_curve", "total_disc_offinv", "trade_grp", "ipp_equip_rental", "ipp_freight_charge", "other_rev_cust", "other_rev", "other_rev_non_cust", "misc_revenue", "discounts_not_used","nka_discount","nsr_base_incidence","cogs_concentrate","cogs_concentrate_tccc","cogs_concentrate_nontccc","cogs_commodities","cogs_conversion")

# COMMAND ----------

if df_final_copa_items.groupBy("fisc_per").count().filter("count > 1").count() > 0:
    raise ValueError("Duplicate values found in 'fisc_per' column. Cannot proceed.")
else:
   df_final_copa_items.write.format("delta").partitionBy("fisc_per").mode("append").saveAsTable(f"""{gold_schema}.fact.copa_items""")
