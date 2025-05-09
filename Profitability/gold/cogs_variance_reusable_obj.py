# Databricks notebook source
# DBTITLE 1,business logic
# MAGIC %run
# MAGIC /Profitability/DataTransformation(GoldLayer)/reusable_object_calculation/call_reusable_business_logic

# COMMAND ----------

# DBTITLE 1,packages
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### build dataframes for ppv, uv, srmv, abs and brand adj

# COMMAND ----------

# this function groups df by groupBy_list and sums all decimal columns in df
def groupBy_and_sum_with_prefix(df, group_by_columns, prefix):
    """
    Groups a DataFrame by specified columns and sums all DecimalType columns,
    prefixing the resulting column names with the provided prefix.

    :param df: The input PySpark DataFrame.
    :param group_by_columns: List of column names to group by.
    :param prefix: The prefix string to add to the resulting summed columns.
    :return: A grouped PySpark DataFrame with summed DecimalType columns.
    """
    # Identify DecimalType columns
    decimal_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, DecimalType)]
    
    # Create aggregation expressions for DecimalType columns with the prefix
    agg_exprs = [
        sum(col(c)).alias(f"{prefix}_{c}") for c in decimal_columns
    ]
    
    # Group by the specified columns and apply the aggregations
    grouped_df = df.groupBy(group_by_columns).agg(*agg_exprs).fillna(0.0)
    
    return grouped_df



# COMMAND ----------

# DBTITLE 1,Function to build dataframes
def fn_cogs_variance(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per):
  try:
    ppv_reusable_query = df_ppv_initial_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)
    #print(ppv_resuable_query)
    df_ppv = spark.sql(ppv_reusable_query)
    df_ppv = groupBy_and_sum_with_prefix(df_ppv, "fisc_per", "total_ppv")
    # df_ppv = df_ppv.groupBy("fisc_per").agg(sum("concentrate").alias("concentrate"),sum("compounding").alias("compounding"),sum("sugar_sweetner").alias('sugar_sweetner'),sum("his_ace_k").alias("his_ace_k"),sum("other_ing").alias("other_ing"),sum("other_ing_tea").alias("other_ing_tea"),sum("other_ing_sweetner").alias("other_ing_sweetner"),sum("other_ing_dairy").alias("other_ing_dairy"),sum("other_ing_alchol").alias('other_ing_alchol'),sum("other_ing_whiskey").alias('other_ing_whiskey'),sum("resin").alias("resin"),sum("preform").alias("preform"),sum("pet_bottles").alias("pet_bottles"),sum("can").alias("can"),sum("closures_crowns").alias("closures_crowns"),sum("labels").alias("labels"),sum("packaging_material_corrugate").alias("packaging_material_corrugate"),sum("stretch_film").alias("stretch_film"),sum("shrink_film").alias("shrink_film"),sum("separator_sheets").alias("separator_sheets"),sum("pet_plastic_handles").alias("pet_plastic_handles"),sum("bags").alias("bags"),sum("other_pack_material").alias("other_pack_material"),sum("other_pack_material_straw").alias("other_pack_material_straw"),sum("co2").alias("co2"),sum("primary_packaging").alias("primary_packaging")).fillna(0.0)
    
    uv_reusable_query = df_uv_initial_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)
    df_uv = spark.sql(uv_reusable_query)
    df_uv = groupBy_and_sum_with_prefix(df_uv, "fisc_per", "total_uv")
    # df_uv = df_uv.groupBy("fisc_per").agg(sum("concentrate").alias("concentrate"),sum("compounding").alias("compounding"),sum("sugar_sweetner").alias('sugar_sweetner'),sum("his_ace_k").alias("his_ace_k"),sum("other_ing").alias("other_ing"),sum("other_ing_tea").alias("other_ing_tea"),sum("other_ing_sweetner").alias("other_ing_sweetner"),sum("other_ing_dairy").alias("other_ing_dairy"),sum("other_ing_alchol").alias('other_ing_alchol'),sum("other_ing_whiskey").alias('other_ing_whiskey'),sum("resin").alias("resin"),sum("preform").alias("preform"),sum("pet_bottles").alias("pet_bottles"),sum("can").alias("can"),sum("closures_crowns").alias("closures_crowns"),sum("labels").alias("labels"),sum("packaging_material_corrugate").alias("packaging_material_corrugate"),sum("stretch_film").alias("stretch_film"),sum("shrink_film").alias("shrink_film"),sum("separator_sheets").alias("separator_sheets"),sum("pet_plastic_handles").alias("pet_plastic_handles"),sum("bags").alias("bags"),sum("other_pack_material").alias("other_pack_material"),sum("other_pack_material_straw").alias("other_pack_material_straw"),sum("co2").alias("co2"),sum("primary_packaging").alias("primary_packaging")).fillna(0.0)

    srmv_reusable_query = df_srmv_initial_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)
    # print(srmv_resuable_query)
    df_srmv = spark.sql(srmv_reusable_query)
    df_srmv = groupBy_and_sum_with_prefix(df_srmv, "fisc_per", "total_srmv")
    # df_srm = df_srm.groupBy("fisc_per").agg(sum("concentrate").alias("concentrate"),sum("compounding").alias("compounding"),sum("sugar_sweetner").alias('sugar_sweetner'),sum("his_ace_k").alias("his_ace_k"),sum("other_ing").alias("other_ing"),sum("other_ing_tea").alias("other_ing_tea"),sum("other_ing_sweetner").alias("other_ing_sweetner"),sum("other_ing_dairy").alias("other_ing_dairy"),sum("other_ing_alchol").alias('other_ing_alchol'),sum("other_ing_whiskey").alias('other_ing_whiskey'),sum("resin").alias("resin"),sum("preform").alias("preform"),sum("pet_bottles").alias("pet_bottles"),sum("can").alias("can"),sum("closures_crowns").alias("closures_crowns"),sum("labels").alias("labels"),sum("packaging_material_corrugate").alias("packaging_material_corrugate"),sum("stretch_film").alias("stretch_film"),sum("shrink_film").alias("shrink_film"),sum("separator_sheets").alias("separator_sheets"),sum("pet_plastic_handles").alias("pet_plastic_handles"),sum("bags").alias("bags"),sum("other_pack_material").alias("other_pack_material"),sum("other_pack_material_straw").alias("other_pack_material_straw"),sum("co2").alias("co2"),sum("primary_packaging").alias("primary_packaging"),sum("imp_mother_sku_upl").alias("imp_mother_sku_upl")).fillna(0.0)

    cur_absorption_pre_query = df_cur_absorption_pre_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)
    # print(cur_absorption_query)
    df_cur_absorption_pre = spark.sql(cur_absorption_pre_query)
    df_cur_absorption_pre.createOrReplaceTempView("cur_absorption_pre_vw")

    cur_absorption_query = df_cur_absorption_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per,'cur_absorption_pre_vw')
    # print(cur_absorption_query)
    df_cur_absorption = spark.sql(cur_absorption_query)
    df_cur_absorption.createOrReplaceTempView("df_cur_absorption_vw")

    abs_reusable_query = df_absorption_initial_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per,"cur_absorption_pre_vw")
    #print(abs_resuable_query)
    df_abs = spark.sql(abs_reusable_query)
    df_abs = groupBy_and_sum_with_prefix(df_abs, "fisc_per", "total_abs")
    # df_abs = df_abs.groupBy("fisc_per").agg(sum("excise_tax").alias("excise_tax"),sum("pack_filling_oh_rm_specific").alias("pack_filling_oh_rm_specific"),sum("pack_filling_oh_rm_alloc").alias("pack_filling_oh_rm_alloc"),sum("pack_filling_oh_var_specific").alias("pack_filling_oh_var_specific"),sum("pack_filling_oh_var_alloc").alias("pack_filling_oh_var_alloc"),sum("pack_filling_oh_fixed_specific").alias("pack_filling_oh_fixed_specific"),sum("pack_filling_oh_fixed_alloc").alias("pack_filling_oh_fixed_alloc"),sum("pack_filling_bc_pack").alias("pack_filling_bc_pack"),sum("pack_filling_dep_pack_specific").alias("pack_filling_dep_pack_specific"),sum("pack_filling_dep_pack_allocated").alias("pack_filling_dep_pack_allocated")).fillna(0.0)

    tolling_reusable_query = df_tolling_initial_query(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per)
    # print(tolling_resuable_query)
    df_tolling = spark.sql(tolling_reusable_query)
    df_tolling = groupBy_and_sum_with_prefix(df_tolling, "fisc_per", "total_abs")
        # .groupBy("fisc_per").agg(sum("tolling_fee_toller").alias("tolling_fee_toller"),sum("tolling_fee_pet").alias("tolling_fee_pet")).fillna(0.0)

    # uncomment line 38 till 40 and remove 41 once sharepoint table is there 
    brand_adj_query = df_brand_adj_query(silver_schema,v_start_fisc_per,v_end_fisc_per)
    df_brand_adj = spark.sql(brand_adj_query)
    return df_ppv,df_uv,df_srmv,df_abs,df_tolling,df_brand_adj
    #return df_ppv,df_uv,df_srm,df_abs,df_tolling
  except Exception as e:
    raise Exception(e)
