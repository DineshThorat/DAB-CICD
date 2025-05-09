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

# query = f"""
# DELETE FROM {gold_schema}.fact.copa_items
# WHERE fisc_per >= '{v_start_fisc_per}'
#   AND fisc_per <= '{v_end_fisc_per}'
# """

# spark.sql(query)

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

df_fixed_disc_oninv = df_on_inv_disc.groupBy("fisc_per", "bill_docnum", "material").agg(sum(when(col("condition_type_oninv").isin("YIC4", "YIC2", "YPA2"), col("total_condition_val") / 1.12).otherwise(0)).alias("disc_amount1"), sum(when(col("condition_type_oninv").isin("YDIF"), col("total_condition_val")).otherwise(0)).alias("disc_amount2")).withColumn("total_amount", col("disc_amount1") + col("disc_amount2"))


# COMMAND ----------

df_var_disc_oninv = df_on_inv_disc.filter(col("condition_type_oninv")=='ZA01').groupBy('fisc_per','outlet').agg(sum('total_condition_val').alias('var_disc_amount'))

# COMMAND ----------

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
# MAGIC ### Cr Dr Memos Dataframe Hist

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

# MAGIC %md
# MAGIC ### Cr Dr Memos Dataframe Cur

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
# MAGIC ### Vom Mix Salesgrpcustmat Dataframe

# COMMAND ----------

df_vol_mix_salesgrpcustmat = spark.sql(f"""
                                       select * from {gold_schema}.profitability.vol_mix_salesgrpcustmat
                                       where fisc_per >= {v_start_fisc_per} and fisc_per <= {v_end_fisc_per}
                                       """)

# COMMAND ----------

# Issue 1: We do not have proper data in 
# nsr = df_nsr_insidence_hist.alias('nsr')
# vm = df_vol_mix_salesgrpcustmat.alias('vm')
# having data for different fisc_per and even sales_grp and sales_group are having different data 

# COMMAND ----------

nsr = df_nsr_insidence_hist.alias('nsr')
vm = df_vol_mix_salesgrpcustmat.alias('vm')

# COMMAND ----------

# df_nsr_insidence_vol_mix_salesgrp = nsr.join(vm, (nsr['fisc_per']== vm['fisc_per']) &(nsr['sales_grp']== vm['sales_group']) & (nsr['material']== vm['material']), how = 'left_outer').withColumn("cust_nsr", col("vm.pct_grp_total_uc") * col("nsr.amount")).drop(vm["fisc_per"], vm["customer"], vm["material"])


# df_nsr_insidence_vol_mix_salesgrp.display()

# These columns needs to be there.
# ['volume_pc', 'loaded_dt', 'pct_grp_total_uc', 'month', 'material', 'volume_uc', 'material_desc', 'pct_total_pc', 'amount', 'sales_office', 'fisc_year', 'sales_group', 'pct_total_uc', 'customer', 'fisc_per', 'pct_grp_total_pc', 'fisc_per_num', 'sales_grp']

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
df_final_1 = df_result_custmat.join(df_std_cogs, df_result_custmat.material == df_std_cogs.material ,how='left_outer').filter(df_result_custmat['material'] != '109580').drop(df_std_cogs["material"], df_std_cogs["fisc_year"]).withColumn("custmat_vol_uc", sum(col("physical_case_qty")*col("conv_to_unit_case") * col("conv_full_case")).over(Window_spec_LVM)).na.fill(0).withColumn("line_vol_mix", ((col("physical_case_qty")*col("conv_to_unit_case") * col("conv_full_case"))/ col("custmat_vol_uc")).cast('decimal(38,18)')).na.fill(0)

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

display(df_final.groupBy("fisc_per").agg(sum(col("commodities_cogs_pc")+col("conversion_cost_pc")+col("excise_tax_pc")+col("management_fee_pc")+col("concentrate_pc"))))

# COMMAND ----------

display(df_final.groupBy("fisc_per").agg(sum("amount"), sum("ipp_discount")))

# COMMAND ----------

64152055.69000 - 102082.41

# COMMAND ----------

display(df_final.groupby("fisc_per").agg(sum("ipp_discount")))

# COMMAND ----------

# display(df_final.select("fisc_per","free_goods_vol_uc", "free_goods_vol_pc","gross_revenue", "sales_doc_type").filter(col("gross_revenue").isNotNull()))
display(df_final.groupBy("fisc_per", "sales_doc_type").agg(sum("free_goods_vol_uc").alias("suc"), sum("free_goods_vol_pc").alias("spc"), sum("gross_revenue").alias("sgr")))

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

## Haulage Costs ##
df_final = df_final.join(df_haulage_rates, (df_final["plant"]==df_haulage_rates["del_loc_code"])&(df_final["Package"]==df_haulage_rates["pack"])&(df_final["PackSize"]==upper(df_haulage_rates["pack_size"]))&(df_final["Quantity"]==df_haulage_rates["quantity_haulage"]), how = 'left_outer').drop(df_haulage_rates["del_loc_code"],df_haulage_rates["pack"],df_haulage_rates["pack_size"],df_haulage_rates["quantity_haulage"])

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

# MAGIC %md
# MAGIC ## COGS for Profitability P&L

# COMMAND ----------

Window_spec_haulage_total = Window.partitionBy('fisc_per')

# COMMAND ----------

df_final = df_final .withColumn("nka_discount", when(col("FISC_PER") < '2024001',when(col("RSM_MTMCutDesc").contains("NKA"),col("total_disc_oninv") - col("dfr_fixed_disc") - col("dfr_variable_disc") - col("dfr_pickup_disc") - col("dfr_promo_disc")).otherwise(0)).otherwise(0))\
                    .withColumn("nsr_base_incidence",when(col("FISC_PER") < '2024001',col("gross_revenue") - col("excise_from_cogs") - (0.093 * (col("gross_revenue") - col("excise_from_cogs"))) - col("nka_discount")).otherwise( col("gross_revenue") - col("excise_from_cogs") - col("total_disc_oninv") - col("dfr_fixed_disc") - col("dfr_variable_disc") - col("dfr_pickup_disc") - col("dfr_promo_disc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate", (when((col("incidence_rate_pc")==0),(col("concentrate_pc")+col("management_fee_pc"))*col("financial_vol_pc")).otherwise(col("nsr_base_incidence")*col("incidence_rate_pc"))).cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_tccc", col("sum_cogs_conctr_tccc").cast('decimal(38,18)'))\
                    .withColumn("cogs_concentrate_nontccc", col("sum_cogs_conctr_nontccc").cast('decimal(38,18)'))\
                    .withColumn("cogs_commodities",(col("commodities_cogs_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_conversion", (col("conversion_cost_pc")*col("financial_vol_pc")).cast('decimal(38,18)'))\
                    .withColumn("cogs_under_over", (df_vol_mix_mat.total_cogs_over_under_rate * df_final.financial_vol_pc).cast('decimal(38,18)')).na.fill(0)

# COMMAND ----------

# %sql
# select sum(mktg_vol_pc) from dev_gold.fact.copa_items
# where fisc_per = '2023002'

# COMMAND ----------

display(df_final.filter(col("fisc_per") == '2023002').agg(sum("mktg_vol_pc")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Haulage Std Cost

# COMMAND ----------

df_final = df_final.withColumn("haulage_std", (col("mktg_vol_pc")*col("haulage_cost_pc")).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("total_haulage_std", (sum("haulage_std").over(Window_spec_haulage_total))).na.fill(0)\
                    .withColumn("haulage_mix", col("haulage_std")/col("total_haulage_std")).na.fill(0)\
                    .withColumn("haulage_total", (col("total_costs")*col("haulage_mix")).cast('decimal(38,18)')).na.fill(0)\
                    .withColumn("total_cogs", (col("cogs_concentrate")+col("cogs_commodities")+col("excise_from_cogs")+col("cogs_conversion")+col("cogs_under_over")+col("haulage_total")).cast('decimal(38,18)')).na.fill(0)

# COMMAND ----------

# %sql
# select sum(nsr_base_incidence) from dev_gold.fact.copa_items
# where fisc_per = '2023012'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delivery 

# COMMAND ----------

df_delivery_rates = spark.sql(f"""
                              select del_loc_code,pack,pack_size,quantity as quantity_delivery,delivery_cost_pc from dev_silver.sharepoint.delivery_costs
                             where fisc_year = {v_fisc_year}.
                              """)

# COMMAND ----------

# Delivery Rates
df_final = df_final.join(df_delivery_rates, (df_final["plant"]==df_delivery_rates["del_loc_code"])&(df_final["Package"]==df_delivery_rates["pack"])&(df_final["PackSize"]==upper(df_delivery_rates["pack_size"]))&(df_final["Quantity"]==df_delivery_rates["quantity_delivery"]), how = 'left_outer').drop(df_delivery_rates["del_loc_code"],df_delivery_rates["pack"],df_delivery_rates["pack_size"],df_delivery_rates["quantity_delivery"])

# COMMAND ----------

df_delivery = df_gl_balances.filter(col("FS_Items")=='Transport').groupBy("fisc_per").agg(sum("balance").alias("total_costs_dlv"))

# COMMAND ----------

df_final = df_final.join(df_delivery, (df_final["fisc_per"]==df_delivery["fisc_per"]), how = 'left_outer').drop(df_delivery["fisc_per"])

# COMMAND ----------

df_final = df_final.withColumn("delivery_std", (col("mktg_vol_pc")*col("delivery_cost_pc")).cast('decimal(38,18)')) \
                   .withColumn("total_delivery_std", (sum("delivery_std").over(Window_spec_haulage_total))).na.fill(0)\
                   .withColumn("delivery_mix", (col("delivery_std")/col("total_delivery_std"))).na.fill(0)\
                   .withColumn("delivery_total", (col("total_costs_dlv")*col("delivery_mix")).cast('decimal(38,18)'))\
                   .withColumn("delivery_under_over", (col("delivery_total")-col("delivery_std")).cast('decimal(38,18)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Warehouse

# COMMAND ----------

df_warehouse_rates = spark.sql(f""" select del_loc_code,pack,pack_size, quantity as quantity_warehouse, warehouse_cost_pc from dev_silver.sharepoint.warehouse_costs where fisc_year = {v_fisc_year}""")

# COMMAND ----------

# Warehouse Rates
df_final = df_final.join(df_warehouse_rates, (df_final["plant"]==df_warehouse_rates["del_loc_code"])&(df_final["Package"]==df_warehouse_rates["pack"])&(df_final["PackSize"]==upper(df_warehouse_rates["pack_size"]))&(df_final["Quantity"]==df_warehouse_rates["quantity_warehouse"]), how = 'left_outer').drop(df_warehouse_rates["del_loc_code"],df_warehouse_rates["pack"],df_warehouse_rates["pack_size"],df_warehouse_rates["quantity_warehouse"])

# COMMAND ----------

df_warehouse = df_gl_balances.filter(col("FS_Items")=='Logistic Operating Expenses (Whse & Distribution)').groupBy("fisc_per").agg(sum("balance").alias("total_costs_warehouse"))

# COMMAND ----------

df_final = df_final.join(df_warehouse, (df_final["fisc_per"]==df_warehouse["fisc_per"]), how = 'left_outer').drop(df_warehouse["fisc_per"])

# COMMAND ----------

df_final = df_final.withColumn("warehouse_std", (col("mktg_vol_pc")*col("warehouse_cost_pc")).cast('decimal(38,18)')) \
                   .withColumn("total_warehouse_std", (sum("warehouse_std").over(Window_spec_haulage_total))).na.fill(0)\
                   .withColumn("warehouse_mix", (col("warehouse_std")/col("total_warehouse_std"))).na.fill(0)\
                   .withColumn("warehouse_total", (col("total_costs_warehouse")*col("warehouse_mix")).cast('decimal(38,18)'))\
                   .withColumn("warehouse_under_over", (col("warehouse_total")-col("warehouse_std")).cast('decimal(38,18)'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistics

# COMMAND ----------

df_final = df_final.withColumn("logistics_opex", (col("warehouse_total")+col("delivery_total")+col("haulage_total")).cast('decimal(38,18)'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(total_costs) from (
# MAGIC select haulage_costs.fisc_per, (haulage_costs.haulage_balance + T1_opex.t1_opex_balance) as total_costs from (
# MAGIC (select h.fisc_per, sum(i.balance) haulage_balance from
# MAGIC dev_silver.ecc.gl_items i
# MAGIC inner join dev_silver.ecc.gl_header h
# MAGIC on i.fisc_year = h.fisc_year and i.acc_docnum = h.acc_docnum
# MAGIC where h.fisc_per >= '2024010' and h.fisc_per <= '2024010'
# MAGIC and i.gl_account in ('710306230', '845201220', '880103130') and i.comp_code = '1215'
# MAGIC group by h.fisc_per
# MAGIC )haulage_costs
# MAGIC inner join
# MAGIC (
# MAGIC select h.fisc_per, sum(i.balance) t1_opex_balance from
# MAGIC dev_silver.ecc.gl_items i
# MAGIC inner join dev_silver.ecc.gl_header h
# MAGIC where h.fisc_per >= '2024010' and h.fisc_per <= '2024010'
# MAGIC and cast(i.gl_account as int) >= 800000000
# MAGIC and cast(i.gl_account as int) <= 900000000
# MAGIC and i.gl_account not in ('875108241', '875108242', '875108243', '880103130', '845201200', '845201201') and i.func_area = 'B32'
# MAGIC and i.profit_center in ('PH1215A082', 'PH1215A083', 'PH1215A084', 'PH1215A085', 'PH1215A086', 'PH1215A087', 'PH1215A088', 'PH1215A089', 'PH1215A130', 'PH1215A091', 'PH1215A092', 'PH1215A093', 'PH1215A094', 'PH1215A095', 'PH1215A096', 'PH1215A085', 'PH1215A097', 'PH1215A098', 'PH1215A082', 'PH1215A099', 'PH1215A129', 'PH1215A101', 'PH1215A083', 'PH1215A102', 'PH1215A084', 'PH1215A103', 'PH1215A104', 'PH1215A105', 'PH1215A106', 'PH1215A086', 'PH1215A107', 'PH1215A108', 'PH1215A109', 'PH1215A110', 'PH1215A111', 'PH1215A087', 'PH1215A087', 'PH1215A112', 'PH1215A089', 'PH1215A130', 'PH1215A113', 'PH1215A114', 'PH1215A115', 'PH1215A116', 'PH1215A091', 'PH1215A117', 'PH1215A118', 'PH1215A092', 'PH1215A119', 'PH1215A120', 'PH1215A093', 'PH1215A121', 'PH1215A122', 'PH1215A123', 'PH1215A124', 'PH1215A125', 'PH1215A126', 'PH1215A127', 'PH1215A128', 'PH1215A133', 'PH1215A134', 'PH1215A131', 'PH1215A132', 'PH1215A137', 'PH1215A138', 'PH1215A139', 'PH1215A088', 'PH1215A073', 'PH1215A142', 'PH1215A143', 'PH1215A144', 'PH1215A145', 'PH1215A146', 'PH1215A147', 'PH1215A148', 'PH1215A149', 'PH1215A157', 'PH12150121', 'PH1215A158')
# MAGIC and h.comp_code = '1215'
# MAGIC group by h.fisc_per
# MAGIC )T1_opex
# MAGIC on haulage_costs.fisc_per = T1_opex.fisc_per
# MAGIC )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s1.haulage_mix, df_haulage.total_costs, s1.fisc_per
# MAGIC (
# MAGIC (
# MAGIC SELECT (haulage_std/(sum(haulage_std) OVER (PARTITION BY fisc_per))) haulage_mix from (
# MAGIC SELECT (mktg_vol_pc * haulage_cost_pc) as haulage_std, fisc_per  from (
# MAGIC SELECT sum(physical_case_qty) as mktg_vol_pc, haulage_cost_pc, fisc_per from (
# MAGIC ( 
# MAGIC     SELECT 
# MAGIC             c.fisc_per,
# MAGIC             c.physical_case_qty, 
# MAGIC             c.plant, 
# MAGIC             cm.PackSize, 
# MAGIC             cm.Package, 
# MAGIC             cm.Quantity
# MAGIC         FROM dev_silver.ecc.copa_items c
# MAGIC         LEFT OUTER JOIN dev_gold.common_dim.current_materials cm
# MAGIC             ON c.material = cm.MaterialNo
# MAGIC         WHERE c.fisc_per = '2024010'
# MAGIC           AND c.material_type != 'ZEMP'
# MAGIC           AND c.material NOT IN ('109580', '101209', '101210', '109800')
# MAGIC     ) ci
# MAGIC     LEFT OUTER JOIN (
# MAGIC         SELECT 
# MAGIC             del_loc_code, 
# MAGIC             pack, 
# MAGIC             pack_size, 
# MAGIC             quantity, 
# MAGIC             haulage_cost_pc
# MAGIC         FROM dev_silver.sharepoint.haulage_costs
# MAGIC         WHERE fisc_year = '2024'
# MAGIC     ) haulage
# MAGIC     ON ci.plant = haulage.del_loc_code 
# MAGIC        AND ci.Package = haulage.pack 
# MAGIC        AND ci.PackSize = UPPER(haulage.pack_size) 
# MAGIC        AND ci.Quantity = haulage.quantity
# MAGIC )
# MAGIC group by fisc_per, haulage_cost_pc
# MAGIC ) 
# MAGIC )
# MAGIC ) s1
# MAGIC LEFT OUTER JOIN(
# MAGIC select haulage_costs.fisc_per, (haulage_costs.haulage_balance + T1_opex.t1_opex_balance) as total_costs from (
# MAGIC (select h.fisc_per, sum(i.balance) haulage_balance from
# MAGIC dev_silver.ecc.gl_items i
# MAGIC inner join dev_silver.ecc.gl_header h
# MAGIC on i.fisc_year = h.fisc_year and i.acc_docnum = h.acc_docnum
# MAGIC where h.fisc_per >= '2024010' and h.fisc_per <= '2024010'
# MAGIC and i.gl_account in ('710306230', '845201220', '880103130') and i.comp_code = '1215'
# MAGIC group by h.fisc_per
# MAGIC )haulage_costs
# MAGIC inner join
# MAGIC (
# MAGIC select h.fisc_per, sum(i.balance) t1_opex_balance from
# MAGIC dev_silver.ecc.gl_items i
# MAGIC inner join dev_silver.ecc.gl_header h
# MAGIC where h.fisc_per >= '2024010' and h.fisc_per <= '2024010'
# MAGIC and cast(i.gl_account as int) >= 800000000
# MAGIC and cast(i.gl_account as int) <= 900000000
# MAGIC and i.gl_account not in ('875108241', '875108242', '875108243', '880103130', '845201200', '845201201') and i.func_area = 'B32'
# MAGIC and i.profit_center in ('PH1215A082', 'PH1215A083', 'PH1215A084', 'PH1215A085', 'PH1215A086', 'PH1215A087', 'PH1215A088', 'PH1215A089', 'PH1215A130', 'PH1215A091', 'PH1215A092', 'PH1215A093', 'PH1215A094', 'PH1215A095', 'PH1215A096', 'PH1215A085', 'PH1215A097', 'PH1215A098', 'PH1215A082', 'PH1215A099', 'PH1215A129', 'PH1215A101', 'PH1215A083', 'PH1215A102', 'PH1215A084', 'PH1215A103', 'PH1215A104', 'PH1215A105', 'PH1215A106', 'PH1215A086', 'PH1215A107', 'PH1215A108', 'PH1215A109', 'PH1215A110', 'PH1215A111', 'PH1215A087', 'PH1215A087', 'PH1215A112', 'PH1215A089', 'PH1215A130', 'PH1215A113', 'PH1215A114', 'PH1215A115', 'PH1215A116', 'PH1215A091', 'PH1215A117', 'PH1215A118', 'PH1215A092', 'PH1215A119', 'PH1215A120', 'PH1215A093', 'PH1215A121', 'PH1215A122', 'PH1215A123', 'PH1215A124', 'PH1215A125', 'PH1215A126', 'PH1215A127', 'PH1215A128', 'PH1215A133', 'PH1215A134', 'PH1215A131', 'PH1215A132', 'PH1215A137', 'PH1215A138', 'PH1215A139', 'PH1215A088', 'PH1215A073', 'PH1215A142', 'PH1215A143', 'PH1215A144', 'PH1215A145', 'PH1215A146', 'PH1215A147', 'PH1215A148', 'PH1215A149', 'PH1215A157', 'PH12150121', 'PH1215A158')
# MAGIC and h.comp_code = '1215'
# MAGIC group by h.fisc_per
# MAGIC )T1_opex
# MAGIC on haulage_costs.fisc_per = T1_opex.fisc_per
# MAGIC )
# MAGIC )df_haulage
# MAGIC on s1.fisc_per = df_haulage.fisc_per
# MAGIC )

# COMMAND ----------

# %sql
# SELECT 
#     cast(sum(s1.haulage_mix * df_haulage.total_costs) as DECIMAL(38,18))
# FROM (
#     SELECT 
#         (haulage_std / SUM(haulage_std) OVER (PARTITION BY fisc_per)) AS haulage_mix, 
#         fisc_per
#     FROM (
#         SELECT 
#             (mktg_vol_pc * haulage_cost_pc) AS haulage_std, 
#             fisc_per
#         FROM (
#             SELECT 
#                 SUM(physical_case_qty) AS mktg_vol_pc, 
#                 haulage_cost_pc, 
#                 fisc_per
#             FROM (
#                 SELECT 
#                     c.fisc_per,
#                     c.physical_case_qty, 
#                     c.plant, 
#                     cm.PackSize, 
#                     cm.Package, 
#                     cm.Quantity
#                 FROM 
#                     dev_silver.ecc.copa_items c
#                 LEFT OUTER JOIN 
#                     dev_gold.common_dim.current_materials cm
#                 ON 
#                     c.material = cm.MaterialNo
#                 WHERE 
#                     c.fisc_per = '2024010'
#                     AND c.material_type != 'ZEMP'
#                     AND c.material NOT IN ('109580', '101209', '101210', '109800')
#             ) ci
#             LEFT OUTER JOIN (
#                 SELECT 
#                     del_loc_code, 
#                     pack, 
#                     pack_size, 
#                     quantity, 
#                     haulage_cost_pc
#                 FROM 
#                     dev_silver.sharepoint.haulage_costs
#                 WHERE 
#                     fisc_year = '2024'
#             ) haulage
#             ON 
#                 ci.plant = haulage.del_loc_code 
#                 AND ci.Package = haulage.pack 
#                 AND ci.PackSize = UPPER(haulage.pack_size) 
#                 AND ci.Quantity = haulage.quantity
#             GROUP BY 
#                 fisc_per, 
#                 haulage_cost_pc
#         )
#     )
# ) s1
# LEFT OUTER JOIN (
#     SELECT 
#         haulage_costs.fisc_per, 
#         (haulage_costs.haulage_balance + T1_opex.t1_opex_balance) AS total_costs
#     FROM (
#         SELECT 
#             h.fisc_per, 
#             SUM(i.balance) AS haulage_balance
#         FROM 
#             dev_silver.ecc.gl_items i
#         INNER JOIN 
#             dev_silver.ecc.gl_header h
#         ON 
#             i.fisc_year = h.fisc_year 
#             AND i.acc_docnum = h.acc_docnum
#         WHERE 
#             h.fisc_per = '2024010' 
#             AND i.gl_account IN ('710306230', '845201220', '880103130') 
#             AND i.comp_code = '1215'
#         GROUP BY 
#             h.fisc_per
#     ) haulage_costs
#     INNER JOIN (
#         SELECT 
#             h.fisc_per, 
#             SUM(i.balance) AS t1_opex_balance
#         FROM 
#             dev_silver.ecc.gl_items i
#         INNER JOIN 
#             dev_silver.ecc.gl_header h
#         ON 
#             i.fisc_year = h.fisc_year 
#             AND i.acc_docnum = h.acc_docnum
#         WHERE 
#             h.fisc_per >= '2024010' 
#             AND h.fisc_per <= '2024010'
#             AND CAST(i.gl_account AS INT) BETWEEN 800000000 AND 900000000
#             AND i.gl_account NOT IN (
#                 '875108241', '875108242', '875108243', 
#                 '880103130', '845201200', '845201201'
#             )
#             AND i.func_area = 'B32'
#             AND i.profit_center IN (
#                 'PH1215A082', 'PH1215A083', 'PH1215A084', 'PH1215A085', 'PH1215A086', 
#                 'PH1215A087', 'PH1215A088', 'PH1215A089', 'PH1215A130', 'PH1215A091', 
#                 'PH1215A092', 'PH1215A093', 'PH1215A094', 'PH1215A095', 'PH1215A096', 
#                 'PH1215A097', 'PH1215A098', 'PH1215A099', 'PH1215A129', 'PH1215A101', 
#                 'PH1215A102', 'PH1215A103', 'PH1215A104', 'PH1215A105', 'PH1215A106', 
#                 'PH1215A107', 'PH1215A108', 'PH1215A109', 'PH1215A110', 'PH1215A111', 
#                 'PH1215A112', 'PH1215A113', 'PH1215A114', 'PH1215A115', 'PH1215A116', 
#                 'PH1215A117', 'PH1215A118', 'PH1215A119', 'PH1215A120', 'PH1215A121', 
#                 'PH1215A122', 'PH1215A123', 'PH1215A124', 'PH1215A125', 'PH1215A126', 
#                 'PH1215A127', 'PH1215A128', 'PH1215A133', 'PH1215A134', 'PH1215A131', 
#                 'PH1215A132', 'PH1215A137', 'PH1215A138', 'PH1215A139', 'PH1215A142', 
#                 'PH1215A143', 'PH1215A144', 'PH1215A145', 'PH1215A146', 'PH1215A147', 
#                 'PH1215A148', 'PH1215A149', 'PH1215A157', 'PH12150121', 'PH1215A158'
#             )
#             AND h.comp_code = '1215'
#         GROUP BY 
#             h.fisc_per
#     ) T1_opex
#     ON 
#         haulage_costs.fisc_per = T1_opex.fisc_per
# ) df_haulage
# ON 
#     s1.fisc_per = df_haulage.fisc_per;


# COMMAND ----------

# df_final(haulage_total) = 277381312.019998648598249150
# source_query = 277381312.019999999445237376

# COMMAND ----------

# from source query (0.999999999999999998 * 277381312.02 = 277381312.02)
# from df_final 0.999999999999995128 * 277381312.02 = 277381312.0199986
277381312.02 - 277381312.0199986 = 1.3709068298339844e-06

# COMMAND ----------

display(df_final.select("fisc_per","nsr_base_incidence", "cogs_concentrate", "cogs_concentrate_tccc", "cogs_concentrate_nontccc", "cogs_commodities", "cogs_conversion", "cogs_under_over", "haulage_std", "total_haulage_std", "haulage_mix", "haulage_total", "total_cogs").filter(col("fisc_per")=='2024010'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## COGS for Portfolio P&L

# COMMAND ----------

df_vol_mix_mat.printSchema()

# COMMAND ----------

cogs_concentrate_total = cogs_concentrate + (df_vol_mix_mat.total_var_concentrate / df_vol_mix_mat.financial_vol_pc) * df_result.financial_vol_pc 

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

# ipp_discount-
# df_final = df_final.withColumn("line_vol_mix", col("sum_unit_case_qty_per_line") / df_vol_mix_custmat.volume_uc) \
#                    .withColumn("mktg_vol_uc", col("sum_unit_case_qty_per_line")) \
#                    .withColumn("mktg_vol_pc", col("sum_physical_case_qty_per_line") * col("conv_full_case")) \
#                    .withColumn("free_goods_vol_uc", when(col("sales_doc_type").isin('YDME', 'ZMKT'), col("sum_unit_case_qty_per_line")).cast("decimal(38,18)")) \
#                    .withColumn("free_goods_vol_pc", when(col("sales_doc_type").isin('YDME', 'ZMKT'), col('mktg_vol_pc'))) \
#                    .withColumn("revenue", col("sum_revenue_per_line")) \
#                    .withColumn("excise_from_copa", col("sum_reserve_92")) \
#                    .withColumn("excise_from_cogs", col("mktg_vol_uc") * col("excise_tax")) \
#                    .withColumn('ipp_discount', col('line_vol_mix') * df_ipp.amount) \
#                    .withColumn("base_price_adj", col("ylp2_amount") / 1.12) \
#                    .withColumn("base_price_excl_excise", col("sum_net_share") - col("base_price_adj")) \
#                    .withColumn("base_price", col("base_price_excl_excise") + col("excise_from_copa")) \
#                    .withColumn("base_price_adj_w_ipp", col("base_price_adj") + col("ipp_discount")) \
#                    .withColumn("base_price_w_excise", (col("revenue") - col("base_price_adj")) + col("excise_from_copa")) \
#                    .withColumn("gross_revenue", col("base_price") + col("base_price_w_excise")) \
#                    .withColumn("promo_disc_oninv", col("sum_trade_promo")) \
#                    .withColumn("fixed_disc_oninv", when((col('condition_type').isin('Y1C4', 'Y1C2')), col('total_condition_val').cast('decimal(25,3)'))) \
#                    .withColumn("variable_disc_oninv", when((col('condition_type').isin('ZA01')), col('total_condition_val').cast('decimal(25,3)'))) \
#                    .withColumn("pickup_disc_oninv", when((col('condition_type').isin('YLP9')), col('total_condition_val').cast('decimal(25,3)'))) \
#                    .withColumn("total_disc_oninv",(col('fixed_disc_oninv')+col('variable_disc_oninv')+col('promo_disc_oninv')+col('pickup_disc_oninv'))) \
#                    .withColumn("dfr_cr_dr_memos", (col('line_vol_mix')*col('total_cr_dr_amnt')).cast('decimal(25,3)')) \
#                    .withColumn("dfr_fixed_disc", (col('line_vol_mix')*df_off_inv_disc.fixed_amnt)+(col('line_vol_mix')*df_cr_dr_memos.fixed_amnt)) \
#                    .withColumn("dfr_from_rms", (col("rebates_accruals_total")/1.12)) \
#                    .withColumn("dfr_variable_disc", (col("line_vol_mix")*df_off_inv_disc.var_amnt)+col("dfr_from_rms")+(col("line_vol_mix")*df_cr_dr_memos.var_amnt)) \
#                    .withColumn("dfr_pickup_disc",(col("line_vol_mix")*df_off_inv_disc.pickup_amnt)+(col(("line_vol_mix"))*df_cr_dr_memos.pickup_amnt)) \
#                    .withColumn("dfr_promo_disc",(col("line_vol_mix")*df_off_inv_disc.promo_amnt)+(col("line_vol_mix")*df_cr_dr_memos.promo_amnt)) \
#                    .withColumn("dfr_del_pen_disc",col("line_vol_mix")*df_off_inv_disc.del_pen_amnt) \
#                    .withColumn("dfr_fallout",col("line_vol_mix")*df_off_inv_disc.dfr_fallout_amnt) \
#                    .withColumn("dfr_dmi",col("line_vol_mix")*df_off_inv_disc.dmi_amnt) \
#                    .withColumn("dfr_dmi_fallout",col("line_vol_mix")*df_off_inv_disc.dmi_fallout_amnt)\
#                    .withColumn("ipp_equip_rental",col("sum_other_surcharge"))\
#                    .withColumn("ipp_freight_charge",col("sum_fuel_surcharge"))\
#                    .withColumn("other_rev_cust", col("ipp_equip_rental") + col("ipp_freight_charge"))\
#                    .withColumn("cogs_concentrate", col("mktg_vol_pc")*df_std_cogs.concentrate_pc) \
#                     .withColumn("cogs_commodities",col("mktg_vol_pc")*df_std_cogs.commodities_cogs_pc)\
#                     .withColumn("cogs_conversion",col("mktg_vol_pc")*df_std_cogs.conversion_cost_pc)\
#                     .withColumn("cogs_sweetener",col("mktg_vol_pc")*df_std_cogs.sugar_sweetner_pc)\
#                     .withColumn("cogs_compounding",col("mktg_vol_pc")*df_std_cogs.compounding_pc) \
#                     .withColumn("cogs_his_ace_k",col("mktg_vol_pc")*df_std_cogs.his_ace_k_pc)\
#                     .withColumn("other_ing",col("mktg_vol_pc")*df_std_cogs.other_ing_pc)\
#                     .withColumn("other_ing_tea",col("mktg_vol_pc")*df_std_cogs.other_ing_tea_pc)\
#                     .withColumn("other_ing_sweetner",col("mktg_vol_pc")*df_std_cogs.other_ing_sweetner_pc)\
#                     .withColumn("other_ing_dairy",col("mktg_vol_pc")*df_std_cogs.other_ing_dairy_pc)\
#                     .withColumn("other_ing_alcohol",col("mktg_vol_pc")*df_std_cogs.other_ing_alcohol_pc)\
#                     .withColumn("other_ing_whiskey",col("mktg_vol_pc")*df_std_cogs.other_ing_whiskey_pc)\
#                     .withColumn("resin",col("mktg_vol_pc")*df_std_cogs.resin_pc)\
#                     .withColumn("preform",col("mktg_vol_pc")*df_std_cogs.preform_pc)\
#                     .withColumn("pet_bottels",col("mktg_vol_pc")*df_std_cogs.pet_bottles_pc)\
#                     .withColumn("cans",col("mktg_vol_pc")*df_std_cogs.cans_pc)\
#                     .withColumn("closures_crows",col("mktg_vol_pc")*df_std_cogs.closures_crowns_pc)\
#                     .withColumn("labels",col("mktg_vol_pc")*df_std_cogs.labels_pc)\
#                     .withColumn("packaging_material_corrugate",col("mktg_vol_pc")*df_std_cogs.packaging_material_corrugate_pc)\
#                     .withColumn("stretch_film",col("mktg_vol_pc")*df_std_cogs.stretch_film_pc)\
#                     .withColumn("shrink_film ",col("mktg_vol_pc")*df_std_cogs.shrink_film_pc)\
#                     .withColumn("separator_sheets_layer_pads",col("mktg_vol_pc")*df_std_cogs.separator_sheets_pc)\
#                     .withColumn("pet_plastic_handles",col("mktg_vol_pc")*df_std_cogs.pet_plastic_handles_pc)\
#                     .withColumn("bags",col("mktg_vol_pc")*df_std_cogs.bags_pc)\
#                     .withColumn("other_pack_material",col("mktg_vol_pc")*df_std_cogs.other_packaging_material_pc)\
#                     .withColumn("other_pack_material_straw",col("mktg_vol_pc")*df_std_cogs.other_packaging_material_straw_pc)\
#                     .withColumn("co2",col("mktg_vol_pc")*df_std_cogs.co2_pc)\
#                     .withColumn("primary_packaging",col("mktg_vol_pc")*df_std_cogs.primary_packaging_uc)\
#                     .withColumn("repacking",col("mktg_vol_pc")*df_std_cogs.repacking_pc)\
#                     .withColumn("management_fee",col("mktg_vol_pc")*df_std_cogs.management_fee_pc)\
#                     .withColumn("distilling_cost",col("mktg_vol_pc")*df_std_cogs.distilling_cost_pc)\
#                     .withColumn("energy",col("mktg_vol_pc")*df_std_cogs.energy_pc)\
#                     .withColumn("tolling_fee_toller",col("mktg_vol_pc")*df_std_cogs.tolling_fee_toller_pc)\
#                     .withColumn("toling_fee_pet",col("mktg_vol_pc")*df_std_cogs.tolling_fee_pet_pc)\
#                     .withColumn("imp_mother_sku_upl",col("mktg_vol_pc")*df_std_cogs.imp_mother_sku_upl_pc)\
#                     .withColumn("pack_filling_oh_rm_specific",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_rm_specific_pc)\
#                     .withColumn("pack_filling_oh_rm_alloc",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_rm_alloc_pc)\
#                     .withColumn("pack_filling_oh_var_specific",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_var_specific_pc)\
#                     .withColumn("pack_filling_oh_var_alloc",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_var_alloc_pc)\
#                     .withColumn("pack_filling_dep_pack_specific",col("mktg_vol_pc")*df_std_cogs.pack_filling_dep_pack_specific_pc)\
#                     .withColumn("pack_filling_bc_pack",col("mktg_vol_pc")*df_std_cogs.pack_filling_bc_pack_pc)\
#                     .withColumn("pack_filling_oh_fixed_alloc",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_fixed_alloc_pc)\
#                     .withColumn("pack_filling_oh_fixed_specific",col("mktg_vol_pc")*df_std_cogs.pack_filling_oh_fixed_specific_pc)\
#                     .withColumn("pack_filling_dep_pack_allocated",col("mktg_vol_pc")*df_std_cogs.pack_filling_dep_pack_allocated_pc)\
#                     .withColumn("haulage",col("mktg_vol_pc")*df_std_cogs.haulage_pc) \
#                     .withColumn("dfr_ctm", lit(gl_ctm_amount).cast('decimal(25,2)')) \
#                     .withColumn("dfr_sales_curve", col('line_vol_mix')*col('dfr_curve_amnt')) \
#                     .withColumn("total_disc_offinv", col('dfr_fixed_disc')+col('dfr_from_rms')+col('dfr_variable_disc')+col('dfr_pickup_disc')+col('dfr_promo_disc')+col('dfr_del_pen_disc')+col('dfr_fallout')+col('dfr_dmi')+col('dfr_dmi_fallout')+col('dfr_ctm')+col('dfr_sales_curve')) \
#                     .withColumn("other_rev", lit('gl_other_rev_amount').cast('decimal(25,2)')) \
#                     .withColumn("other_rev_non_cust", (col('other_rev')-col('other_rev_cust'))) \
#                     .withColumn("nsr_base_incidence", (col("sum_unit_case_qty_per_line") / df_nsr_insidence_vol_mix_salesgrp.volume_uc) * col("cust_nsr")) \
#                     .withColumn("cogs_insidence", col('nsr_base_incidence')*df_std_cogs.incidence_rate_pc) \
#                     .withColumn("cogs_under_over", col('line_vol_mix') * ((df_vol_mix_custmat.pct_total_concentrate * df_cogs_var.concentrate_cogs_var) + (df_vol_mix_custmat.pct_total_compounding * df_cogs_var.compounding_cogs_var) + (df_vol_mix_custmat.pct_total_sugar_sweetner * df_cogs_var.sugar_sweetner_cogs_var) + (df_vol_mix_custmat.pct_total_his_ace_k * df_cogs_var.his_ace_k_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing * df_cogs_var.other_ing_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing_tea * df_cogs_var.other_ing_tea_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing_sweetner * df_cogs_var.other_ing_sweetner_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing_dairy * df_cogs_var.other_ing_dairy_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing_alchol * df_cogs_var.other_ing_alchol_cogs_var) + (df_vol_mix_custmat.pct_total_other_ing_whiskey * df_cogs_var.other_ing_whiskey_cogs_var) + (df_vol_mix_custmat.pct_total_resin * df_cogs_var.resin_cogs_var) + (df_vol_mix_custmat.pct_total_preform * df_cogs_var.preform_cogs_var) + (df_vol_mix_custmat.pct_total_pet_bottles * df_cogs_var.pet_bottles_cogs_var) + (df_vol_mix_custmat.pct_total_can * df_cogs_var.can_cogs_var) + (df_vol_mix_custmat.pct_total_closures_crowns * df_cogs_var.closures_crowns_cogs_var) + (df_vol_mix_custmat.pct_total_labels * df_cogs_var.labels_cogs_var) + (df_vol_mix_custmat.pct_total_packaging_material_corrugate * df_cogs_var.packaging_material_corrugate_cogs_var) + (df_vol_mix_custmat.pct_total_strectch_film * df_cogs_var.strectch_film_cogs_var) + (df_vol_mix_custmat.pct_total_shrink_film * df_cogs_var.shrink_film_cogs_var) + (df_vol_mix_custmat.pct_total_separator_sheets * df_cogs_var.separator_sheets_cogs_var) + (df_vol_mix_custmat.pct_total_pet_plastic_handles * df_cogs_var.pet_plastic_handles_cogs_var) + (df_vol_mix_custmat.pct_total_bags * df_cogs_var.bags_cogs_var) + (df_vol_mix_custmat.pct_total_other_pack_material * df_cogs_var.other_pack_material_cogs_var) + (df_vol_mix_custmat.pct_total_other_pack_material_straw * df_cogs_var.other_pack_material_straw_cogs_var) + (df_vol_mix_custmat.pct_total_co2 * df_cogs_var.co2_cogs_var) + (df_vol_mix_custmat.pct_total_primary_packaging * df_cogs_var.primary_packaging_cogs_var) + (df_vol_mix_custmat.pct_total_repacking * df_cogs_var.repacking_cogs_var) + (df_vol_mix_custmat.pct_total_management_fee * df_cogs_var.management_fee_cogs_var) + (df_vol_mix_custmat.pct_total_distilling_cost * df_cogs_var.distilling_cost_cogs_var) + (df_vol_mix_custmat.pct_total_excise_tax * df_cogs_var.excise_tax_cogs_var) + (df_vol_mix_custmat.pct_total_energy * df_cogs_var.energy_cogs_var) + (df_vol_mix_custmat.pct_total_tolling_fee_toller * df_cogs_var.tolling_fee_toller_cogs_var) + (df_vol_mix_custmat.pct_total_tolling_fee_pet * df_cogs_var.tolling_fee_pet_cogs_var) + (df_vol_mix_custmat.pct_total_imp_mother_sku_upl * df_cogs_var.imp_mother_sku_upl_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_rm_specific * df_cogs_var.pack_filling_oh_rm_specific_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_rm_alloc * df_cogs_var.pack_filling_oh_rm_alloc_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_var_specific * df_cogs_var.pack_filling_oh_var_specific_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_var_alloc * df_cogs_var.pack_filling_oh_var_alloc_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_fixed_specific * df_cogs_var.pack_filling_oh_fixed_specific_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_oh_fixed_alloc * df_cogs_var.pack_filling_oh_fixed_alloc_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_bc_pack * df_cogs_var.pack_filling_bc_pack_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_dep_pack_specific * df_cogs_var.pack_filling_dep_pack_specific_cogs_var) + (df_vol_mix_custmat.pct_total_pack_filling_dep_pack_allocated * df_cogs_var.pack_filling_dep_pack_allocated_cogs_var) + (df_vol_mix_custmat.pct_total_haulage * df_cogs_var.haulage_cogs_var))) \
#                    .withColumn("cogs_concentrate_tccc", col("sum_cogs_conctr_tccc")) \
#                    .withColumn("cogs_concentrate_nontccc", col("sum_cogs_conctr_tccc")) \
#                    .withColumn("cogs_incidence_adj", (col("cogs_concentrate_tccc")+col("cogs_concentrate_nontccc")) - col("cogs_insidence"))

# COMMAND ----------

df_final_copa_items = df_final.select("cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc","free_goods_vol_fpc","financial_vol_uc","financial_vol_pc","financial_vol_fpc", "revenue_adj","base_price_adj", "base_price_excl_excise", "excise_from_copa", "excise_from_cogs", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "gross_wholesale_price", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "promo_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout", "dfr_ctm", "dfr_sales_curve", "total_disc_offinv", "trade_grp", "ipp_equip_rental", "ipp_freight_charge", "other_rev_cust", "other_rev", "other_rev_non_cust", "misc_revenue", "discounts_not_used")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(profit_loss_item) from dev_gold.fact.profitability01

# COMMAND ----------

df_final_copa_items.groupBy('fisc_per').count().display()

# COMMAND ----------

col_list_check =["cur_type", "rec_type", "fisc_per", "material", "sales_doc_type", "posting_date", "comp_code", "sales_district", "inv_date", "billing_type", "fisc_year", "created_date", "business_type", "business_type_extn", "cust_trade_channel", "cust_sub_channel", "sales_docnum", "cust_grp", "sales_itemnum", "sales_office", "sales_group", "customer", "controlling_area", "price_grp", "cost_element", "cust_acc_assign_grp", "mat_acc_assign_grp", "ship_to", "material_type", "fisc_per_num", "profit_center", "wbs_element", "ref_docnum", "currency", "division", "created_by", "sales_org", "dist_channel","line_vol_mix", "mktg_vol_uc", "mktg_vol_pc","mktg_vol_fpc", "free_goods_vol_uc", "free_goods_vol_pc","free_goods_vol_fpc","financial_vol_uc","financial_vol_pc","financial_vol_fpc", "revenue_adj","base_price_adj", "base_price_excl_excise", "excise_from_copa", "excise_from_cogs", "base_price", "ipp_discount", "base_price_adj_w_ipp", "gross_revenue", "gross_wholesale_price", "promo_disc_oninv", "fixed_disc_oninv", "variable_disc_oninv", "pickup_disc_oninv", "total_disc_oninv", "dfr_cr_dr_memos", "dfr_fixed_disc", "dfr_from_rms", "dfr_variable_disc", "dfr_pickup_disc", "dfr_promo_disc", "dfr_del_pen_disc", "dfr_fallout", "dfr_dmi", "dfr_dmi_fallout","dfr_ctm", "dfr_sales_curve","total_disc_offinv", "other_rev","ipp_equip_rental", "ipp_freight_charge", "other_rev_cust","other_rev_non_cust"]
for i in range(len(col_list_check)-1):
    test_data_null = spark.sql(f"select * from {gold_schema}.fact.copa_items where {col_list_check[i]} is null ") 
display(test_data_null)

# COMMAND ----------

df_final_copa_items.write.format("delta").partitionBy("fisc_per").mode("append").saveAsTable(f"""{gold_schema}.fact.copa_items""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(reserve_92) from dev_silver.ecc.copa_items where fisc_per = '2023001' --1182015179.65

# COMMAND ----------

print(df_final_copa_items.count())
print(df_final_copa_items.filter("fisc_per = '2023010'").count())
print(df_final_copa_items.filter("fisc_per = '2024010'").count())
df_final_copa_items.display()


# COMMAND ----------

df_final_copa_items.write.format("delta").partitionBy("fisc_per").mode("append").saveAsTable(f"""{gold_schema}.fact.copa_items""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from {gold_schema}.fact.copa_itemsvariable_disc_oninv", "pickup_disc_oninv", "total_disc_oninv"

# COMMAND ----------



# COMMAND ----------

df_final_fact = df_final.select("mktg_vol_pc","mktg_vol_uc","free_goods_vol_pc","free_goods_vol_uc","gross_revenue","base_price","base_price_excl_excise","base_price_adj_w_ipp","ipp_discount","total_disc_oninv","fixed_disc_oninv","variable_disc_oninv","pickup_disc_oninv","promo_disc_oninv","total_disc_offinv","customer","material")

# COMMAND ----------

df_final_fact.display()

# COMMAND ----------

# def transpose_all_columns(df):
#     # Create an array of struct where each struct contains the column name and the column value
#     array_of_structs = array(*[
#         struct(lit(column).alias("Column"), col(column).alias("Value")) for column in df.columns
#     ])
    
#     # Explode the array to turn columns into rows
#     transposed_df = df.select(explode(array_of_structs).alias("kv")) \
#                       .select(col("kv.Column"), col("kv.Value"))
    
#     return transposed_df

# #transposed_df = transpose_all_columns(df)

# COMMAND ----------

# transposed_df = transpose_all_columns(df_final_fact)#.groupBy("Column").agg(sum("Value").alias("Value"))

# COMMAND ----------

display(transposed_df)

# COMMAND ----------

withColumn("dfr_dmi_fallout",col("line_vol_mix")*df_off_inv_disc.dfr_fallout_amnt)\ dfr/dmi

           .withColumn("cogs_incidence",col("nsr_base_incidence")*df_std_cogs.incidence_rate_pc)\
                   .withColumn("cogs_incidence_adj",col("cogs_concentrate_copa") - col("cogs_incidence"))\

# COMMAND ----------

df_final.filter(col("sales_doc_type").isin('YDME','ZMKT')).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from {bronze_schema}.bw.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_bronze.sharepoint.standard_cogs
# MAGIC where EffectivityDate = '01-01-2024'

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load("abfss://sharepoint@sagrsseaccbplanding0001.dfs.core.windows.net/standard_cogs/standard_cogs_hist_2024-12-09T18:26:48.csv")
display(df)
