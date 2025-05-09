# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up Notebook

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
from datetime import datetime,date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DateType,DoubleType,BooleanType,DecimalType
from pyspark.sql.window import Window
from decimal import Decimal


# COMMAND ----------

# DBTITLE 1,set global parameters
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,If no period set, use current period
# replace this once the 2025 calendar has been uploadted to the fisc_per dimension
current_date = datetime.now() - timedelta(days=30)
# current_date = datetime.now()

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

# DBTITLE 1,get common parameters
# MAGIC %run /Workspace/Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# DBTITLE 1,set schema's based on Environment
bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # load all required tables into dataframes

# COMMAND ----------

L1_window = Window.partitionBy("fisc_per")
L2_window = Window.partitionBy("fisc_per","bus_type_ext")
L3_window = Window.partitionBy("fisc_per","bus_type_ext","trade_group")

# COMMAND ----------

# DBTITLE 1,get all mapping tables
df_pcm = spark.table(f'{silver_schema}.sharepoint.pc_mapping')
df_glm = spark.table(f'{silver_schema}.sharepoint.gl_mapping')
df_ccm = spark.table(f'{silver_schema}.sharepoint.cc_mapping')
df_cust = spark.table(f'{gold_schema}.common_dim.current_outlets')
df_sales_grp = df_cust.where(col("RSM_MTMCut").isNotNull()).withColumnRenamed("RSM_MTMCut","sales_grp_code") \
                .groupBy("sales_grp_code", "RSM_MTMCutDesc").agg(count("*").alias("count"))
df_budget_vol = spark.table(f'{silver_schema}.files.budget_vol_nsr') \
                .filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per)) \
                .groupBy("fisc_per","bus_type_ext", "trade_group", "sales_group").agg(sum("mgsv_ucs").alias("mgsv_ucs")) \
                .join(df_sales_grp, on=col("sales_group") == df_sales_grp.RSM_MTMCutDesc, how="leftouter") \
                .drop("RSM_MTMCutDesc", "count") \
                .withColumn("sales_grp_code_nonull", when(col("sales_grp_code").isNull(),col("sales_group")) \
                    .otherwise(col("sales_grp_code"))) \
                .drop("sales_grp_code") \
                .withColumnRenamed("sales_grp_code_nonull", "sales_grp_code") \
                .withColumn("L1_vol", sum("mgsv_ucs").over(L1_window)) \
                .withColumn("L1_mix", (col("mgsv_ucs") / col("L1_vol")).cast('Decimal(38,28)')) \
                .withColumn("L2_vol", sum("mgsv_ucs").over(L2_window)) \
                .withColumn("L2_mix", (col("L2_vol") / col("L1_vol")).cast('Decimal(38,28)') * (col("mgsv_ucs") / col("L2_vol")).cast('Decimal(38,28)')) \
                .withColumn("L3_vol", sum("mgsv_ucs").over(L3_window)) \
                .withColumn("L3_mix", ((col("L3_vol") / col("L1_vol"))).cast('Decimal(38,28)') * (col("mgsv_ucs") / col("L3_vol")).cast('Decimal(38,28)'))    

# COMMAND ----------

# display(df_budget_vol)

# COMMAND ----------

# DBTITLE 1,get GL Balance for CDO
df_cdo_balances = spark.table(f'{gold_schema}.profitability.gl_balances').filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per) & (col("cdo") == "yes"))

# COMMAND ----------

# df_ccm.display()
# df_pcm.display()
# df_cdo_balances.display()

# COMMAND ----------

# DBTITLE 1,join GL with mapping tables and aggregate
df_cdo_costs = df_cdo_balances.groupBy("fisc_year", "fisc_per", "cost_center", "profit_center") \
    .agg(sum("balance").alias("cdo_cost")) \
    .join(df_ccm.select("cost_center", "cdo_class","sales_grp","sales_grp_desc","trade_grp_desc","bus_type_ext_desc"), on="cost_center", how="left") \
    .join(df_pcm.select("profit_center", col("sales_grp").alias("pc_sales_grp"),col("sales_grp_desc").alias("pc_sales_grp_desc"),col("trade_grp_desc").alias("pc_trade_grp_desc"),col("bus_type_ext_desc").alias("pc_bus_type_ext_desc"), "unit"), on="profit_center", how="left") \
    .withColumn("GTMT", when(col("cost_center").isNotNull(), col("bus_type_ext_desc")).otherwise(col("pc_bus_type_ext_desc"))) \
    .withColumn("cdo_classification", when(col("cost_center").isNotNull(), col("cdo_class")) \
        .otherwise(when(col("unit") == "VENDING",lit("Vending")).otherwise(lit("Not Vending"))))

# COMMAND ----------

# df_cdo_costs.display()

# COMMAND ----------

# display(df_cdo_balances.join(df_pcm, on="profit_center", how="left").where(col("cost_center").isNull()))

# COMMAND ----------

# display(df_cdo_costs.groupBy("cdo_classification").agg(sum("cdo_cost")))

# COMMAND ----------

# DBTITLE 1,get Equipment metrics for CDO
df_cdo_equip = spark.table(f'{silver_schema}.files.cdo_history').filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

# DBTITLE 1,join equipment data with mapping tables and aggregate
df_cdo_counts = df_cdo_equip.groupBy("fisc_year","fisc_per","object_type_group","status_group_1","ship_to","cost_center").agg(countDistinct("equipment").alias("equipment_count"))
df_cdo_counts = df_cdo_counts.join(df_ccm.select("cost_center", "cdo_class","sales_grp","sales_grp_desc"), on="cost_center", how="left") \
    .join(df_cust.select(col("CustomerNo").cast("string").alias("CustomerNo"), "RSM_MTMCut", "RSM_MTMCutDesc"), on=df_cdo_counts.ship_to == col("CustomerNo"), how="left") \
    .drop("CustomerNo")

# COMMAND ----------

display(df_cdo_counts.groupBy("object_type_group","status_group_1").agg(sum("equipment_count")).orderBy("object_type_group","status_group_1"))
# display(df_cdo_counts.groupBy("object_type_group").agg(sum("equipment_count")))

# COMMAND ----------

# MAGIC %md
# MAGIC # calculate all required metrics

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vending

# COMMAND ----------

# DBTITLE 1,rate for Vending
vending_cost = df_cdo_costs.where(col("cdo_classification") == "Vending").groupBy("fisc_per").agg(sum("cdo_cost").alias("cdo_cost"))
vending_count = df_cdo_counts.where(col("object_type_group") == "Vending").groupBy("fisc_per").agg(sum("equipment_count").alias("equipment_count"))
vending_rate = vending_cost.collect()[0][1] / vending_count.collect()[0][1]


# COMMAND ----------

# DBTITLE 1,Vending In Trade
df_vending_in_trade = df_cdo_counts.where((col("object_type_group") == "Vending") & (col("status_group_1") == "In Trade")) \
                        .select("fisc_year", "fisc_per", col("ship_to").alias("customer"), "object_type_group", "status_group_1", "equipment_count") \
                        .withColumn("cdo_cost", col("equipment_count") * vending_rate)

# COMMAND ----------

# display(df_vending_in_trade)

# COMMAND ----------

# DBTITLE 1,Vending Not in Trade
df_vending_not_in_trade = df_cdo_counts.where((col("object_type_group") == "Vending") & (col("status_group_1") == "Not In Trade")) \
                        .withColumn("customer", lit("OD")) \
                        .select("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1", "equipment_count") \
                        .groupBy("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1") \
                        .agg(sum("equipment_count").alias("equipment_count")) \
                        .withColumn("cdo_cost", col("equipment_count") * vending_rate)

# COMMAND ----------

# display(df_vending_not_in_trade)

# COMMAND ----------

# DBTITLE 1,Total Vending
df_vending = df_vending_in_trade.union(df_vending_not_in_trade)

# COMMAND ----------

# display(df_vending.agg(round(sum(col("equipment_count")),0), round(sum(col("cdo_cost")),2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Not Vending

# COMMAND ----------

# DBTITLE 1,Rate for "Not Vending"
not_vending_cost = df_cdo_costs.where(col("cdo_classification") == "Not Vending").groupBy("fisc_per").agg(sum("cdo_cost").alias("cdo_cost"))
not_vending_count = df_cdo_counts.where(col("object_type_group") == "Not Vending").groupBy("fisc_per").agg(sum("equipment_count").alias("equipment_count"))
not_vending_rate = not_vending_cost.collect()[0][1] / not_vending_count.collect()[0][1]

# COMMAND ----------

# DBTITLE 1,Not Vending In Trade
df_not_vending_in_trade = df_cdo_counts.where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "In Trade")) \
                        .select("fisc_year", "fisc_per", col("ship_to").alias("customer"), "object_type_group", "status_group_1", "equipment_count") \
                        .withColumn("cdo_cost", col("equipment_count") * not_vending_rate)

# COMMAND ----------

# display(df_not_vending_in_trade)

# COMMAND ----------

display(df_not_vending_in_trade.agg(round(sum("equipment_count"), 0).alias("equipment_count"), round(sum("cdo_cost"), 2).alias("cdo_cost")) \
    .withColumn("cdo_cost_calc", round(col("equipment_count") * not_vending_rate,2))
        )

# COMMAND ----------

# DBTITLE 1,Not Vending Not In Trade
df_not_vending_not_in_trade = df_cdo_counts \
            .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade") & (col("sales_grp").isNotNull()) & (col("sales_grp") != "xx")) \
            .select("fisc_year", "fisc_per", col("sales_grp").alias("customer"), "object_type_group", "status_group_1", "equipment_count") \
            .groupBy("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1").agg(sum("equipment_count").alias("equipment_count")) \
            .withColumn("cdo_cost", col("equipment_count") * not_vending_rate)

# COMMAND ----------

# display(df_not_vending_not_in_trade)

# COMMAND ----------

# display(df_cdo_counts \
#             .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade")) \
#             .groupBy("sales_grp", "sales_grp_desc", "RSM_MTMCut", "RSM_MTMCutDesc") \
#                 .agg(sum("equipment_count").alias("equipment_count")))

# COMMAND ----------

df_not_vending_not_in_trade_l1 = df_cdo_counts \
            .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade") & ((col("sales_grp").isNull()) | (col("sales_grp_desc") == "L1 Allocation"))) \
            .select("fisc_year", "fisc_per", "object_type_group", "status_group_1", "equipment_count") \
            .groupBy("fisc_year", "fisc_per", "object_type_group", "status_group_1").agg(sum("equipment_count").alias("equipment_count")) \
            .join(df_budget_vol, on="fisc_per", how="inner") \
            .withColumnRenamed("sales_grp_code", "customer") \
            .withColumn("equipment_count_l1", (col("equipment_count") * col("L1_mix")).cast('decimal(38,18)')) \
            .withColumn("cdo_cost", (col("equipment_count_l1") * not_vending_rate).cast('decimal(38,10)')) \
            .drop("bus_type_ext","trade_group", "mgsv_ucs", "L1_vol", "L1_mix", "L2_vol", "L2_mix", "L3_vol", "L3_mix", "equipment_count", "sales_group" ) \
            .withColumnRenamed("equipment_count_l1", "equipment_count") \
            .select("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1", "equipment_count", "cdo_cost")

# COMMAND ----------

df_not_vending_not_in_trade_l2 = df_cdo_counts \
            .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade") & (col("sales_grp_desc") == "L2 Allocation")) \
            .select("fisc_year", "fisc_per", "object_type_group", "status_group_1", "equipment_count") \
            .groupBy("fisc_year", "fisc_per", "object_type_group", "status_group_1").agg(sum("equipment_count").alias("equipment_count")) \
            .join(df_budget_vol, on="fisc_per", how="inner") \
            .withColumnRenamed("sales_grp_code", "customer") \
            .withColumn("equipment_count_l2", (col("equipment_count") * col("L2_mix")).cast('decimal(38,18)')) \
            .withColumn("cdo_cost", (col("equipment_count_l2") * not_vending_rate).cast('decimal(38,10)')) \
            .drop("bus_type_ext","trade_group", "mgsv_ucs", "L1_vol", "L1_mix", "L2_vol", "L2_mix", "L3_vol", "L3_mix", "equipment_count", "sales_group" ) \
            .withColumnRenamed("equipment_count_l2", "equipment_count") \
            .select("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1", "equipment_count", "cdo_cost")

# COMMAND ----------

df_not_vending_not_in_trade_l3 = df_cdo_counts \
            .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade") & (col("sales_grp_desc") == "L3 Allocation")) \
            .select("fisc_year", "fisc_per", "object_type_group", "status_group_1", "equipment_count") \
            .groupBy("fisc_year", "fisc_per", "object_type_group", "status_group_1").agg(sum("equipment_count").alias("equipment_count")) \
            .join(df_budget_vol, on="fisc_per", how="inner") \
            .withColumnRenamed("sales_grp_code", "customer") \
            .withColumn("equipment_count_l3", (col("equipment_count") * col("L3_mix")).cast('decimal(38,18)')) \
            .withColumn("cdo_cost", (col("equipment_count_l3") * not_vending_rate).cast('decimal(38,10)')) \
            .drop("bus_type_ext","trade_group", "mgsv_ucs", "L1_vol", "L1_mix", "L2_vol", "L2_mix", "L3_vol", "L3_mix", "equipment_count", "sales_group" ) \
            .withColumnRenamed("equipment_count_l3", "equipment_count") \
            .select("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1", "equipment_count", "cdo_cost")

# COMMAND ----------

# display(df_cdo_counts \
#             .where((col("object_type_group") == "Not Vending") & (col("status_group_1") == "Not In Trade")) \
#             .agg(sum("equipment_count").alias("equipment_count"))
# )

# COMMAND ----------

# display(df_not_vending_not_in_trade_l3)

# COMMAND ----------

# DBTITLE 1,Total Not Vending
df_not_vending = df_not_vending_in_trade \
                    .union(df_not_vending_not_in_trade) \
                    .union(df_not_vending_not_in_trade_l1) \
                    .union(df_not_vending_not_in_trade_l2) \
                    .union(df_not_vending_not_in_trade_l3) \
                    .groupBy("fisc_year", "fisc_per", "customer", "object_type_group", "status_group_1") \
                        .agg(sum("equipment_count").alias("equipment_count"), sum("cdo_cost").alias("cdo_cost"))

# COMMAND ----------

# display(df_not_vending.groupBy("status_group_1").agg(round(sum("equipment_count"),0).alias("equipment_count"),round(sum("cdo_cost"),2).alias("cdo_cost")).withColumn("calc_cost",round(col("equipment_count") * not_vending_rate,2)))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Total CDO

# COMMAND ----------

df_cdo = df_vending.union(df_not_vending)

# COMMAND ----------

display(df_cdo.groupBy("object_type_group").agg(round(sum("equipment_count"),0).alias("equipment_count"),round(sum("cdo_cost"),2).alias("cdo_cost")).orderBy("object_type_group"))

# COMMAND ----------

display(df_cdo.groupBy("fisc_per") \
    .agg(round(sum("cdo_cost"),2).alias("total_cost"),round(sum("equipment_count"),0).alias("total_equipment")) \
    .join(df_cdo_costs.groupBy("fisc_per").agg(round(sum("cdo_cost"),2).alias("gl_balance")), on="fisc_per", how="inner") \
    .join(df_cdo_counts.groupBy("fisc_per").agg(round(sum("equipment_count"),0).alias("equipment_count_from_EM")), on="fisc_per", how="inner") \
    .withColumn("cost_diff", col("gl_balance") - col("total_cost")) \
    .withColumn("count_diff", col("equipment_count_from_EM") - col("total_equipment"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # write results to target table

# COMMAND ----------

# DBTITLE 1,create target table
create_query = f"""
create table if not exists {gold_schema}.profitability.cdo_monthly
(
    fisc_year int COMMENT 'Fiscal Year',
    fisc_per string COMMENT 'Fiscal Year and Period in SAP Database Format YYYYPPP',
    customer string COMMENT 'Customer code for allocated CDO Cost. For equipment not in trade, this will be a Sales Group Code',
    object_type_group string COMMENT 'Equipment Type. This identifies whether the equipment is Vending or Non-Vending',
    status_group_1 string COMMENT 'Trade Status of the equipment. This is used to determine if the equipment is in trade or not.',
    equipment_count decimal(38,18) COMMENT 'Number of pieces of equipment. For status_group_1 = "In Trade" this is the number of pieces of equipment assigned to the customer. For status_group_1 = "Not In Trade" this is the number of pieces of equipment assigned to the Sales Group of the Cost Center from the Equipment Master.',
    cdo_cost decimal(38,10) COMMENT 'this is the apportioned CDO cost from the GL. The cost is apportioned based on the number of pieces of equipment assigned to the customer or Sales Group.'
)
COMMENT 'This tables contains the allocated costs for CDO from the GL down to the Customer or Sales Group level. Data in this table is split by two key dimensions (status_group_1, object_type_group) and each dimension has two possible values. this means there are four key groupings of data in this table: (Vending, In Trade), (Vending, Not In Trade), (Non-Vending, In Trade), (Non-Vending, Not In Trade). Whenever the equipment is in trade, the customer code is in the customer field. When the equipment is not in trade, the customer field contains the Sales Group code from the Cost Center Mapping.'
CLUSTER BY (fisc_per)
"""

spark.sql(create_query)

# COMMAND ----------

# DBTITLE 1,remove data from target table for selected periods
query = f"""
DELETE FROM {gold_schema}.profitability.cdo_monthly
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,write to Target Table
df_cdo.write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(f'{gold_schema}.profitability.cdo_monthly')

# COMMAND ----------

# DBTITLE 1,exchange rate code
df_exchange_rates  = spark.sql("""select exchange_rate
 from dev_silver.ecc.exchange_rates where valid_date = (select PeriodStart from dev_gold.common_dim.fisc_per where FiscalPeriod >= '{v_start_fisc_per}' AND FiscalPeriod <= '{v_end_fisc_per}') and from_currency ="PHP" and to_currency = "EUR" and exchange_rate_type = "EURX" """)
df_exchange_rates.display()
# v_exchange_rate = df_exchange_rates.collect()[0]['exchange_rate']
# print(v_exchange_rate)

# COMMAND ----------

# %sql
# SELECT * from dev_gold.profitability.cdo_monthly

# -- cust, fis - group sum (cdo_cost)


# COMMAND ----------

# DBTITLE 1,creating and renaming columns
df_cdo_monthly = spark.table(f'{gold_schema}.profitability.cdo_monthly')

df_cdo_monthly_fnl = df_cdo_monthly.groupBy('fisc_per', 'customer').agg(sum('cdo_cost').alias('amount_lc').cast('decimal(38,18)'))\
    .withColumn('scenario', lit('Actuals')) \
    .withColumn('profit_loss_item', lit('cold_drink_operations')) \
    .withColumn('MaterialNo', lit(None)) \
    .withColumn('profit_loss_component', lit('CDO')) \
    .withColumn('volume', lit(Decimal(0.0)).cast('decimal(38,18)')) \
    .withColumn('currency_lc', lit('PHP')) \
    .withColumn('amount_gc', (col('amount_lc') * 1).cast('decimal(38,18)')) \
    .withColumn('currency_gc', lit('EUR'))\
    .withColumnRenamed('fisc_per', 'FiscalPeriod')\
    .withColumnRenamed('customer', 'CustomerNo')

df_cdo_monthly_fnl = df_cdo_monthly_fnl.select('FiscalPeriod', 'scenario', 'profit_loss_item','CustomerNo','MaterialNo', 'profit_loss_component', 'volume','amount_lc', 'currency_lc', 'amount_gc','currency_gc')

display(df_cdo_monthly_fnl)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from dev_gold.fact.profitability01 where profit_loss_item = 'cold_drink_operations'

# COMMAND ----------

# DBTITLE 1,writing into prof table
df_cdo_monthly_fnl.write \
    .mode('append') \
    .saveAsTable(f'{gold_schema}.fact.profitability01')

# COMMAND ----------


