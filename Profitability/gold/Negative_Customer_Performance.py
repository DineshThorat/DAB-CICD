# Databricks notebook source
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

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def get_ytd_performance(fisc_per):  
    fiscal_year = str(fisc_per)[:4]
    start_fisc_per = fiscal_year + "001"
    
    df_ytd = spark.table("dev_gold.fact.copa_items").filter(
        (col("fisc_per") >= start_fisc_per) & (col("fisc_per") <= fisc_per)
    ).groupBy("customer").agg(
        sum(col("revenue_after_curve").cast(DecimalType(38, 18))).alias("revenue_after_curve"),
        sum(col("gross_revenue").cast(DecimalType(38, 18))).alias("gross_profit"),
        sum(col("fdm_after_curve").cast(DecimalType(38, 18))).alias("fdm_after_curve"),
        sum(col("operating_profit").cast(DecimalType(38, 18))).alias("operating_profit")
    ).withColumn("fisc_per", lit(fisc_per))
    
    return df_ytd

# Define the schema for the empty DataFrame
schema = StructType([
    StructField("customer", StringType(), True),
    StructField("revenue_after_curve", DecimalType(38,18), True),
    StructField("gross_profit", DecimalType(38,18), True),
    StructField("fdm_after_curve", DecimalType(38,18), True),
    StructField("operating_profit", DecimalType(38,18), True),
    StructField("fisc_per", StringType(), True)
])

# Initialize an empty DataFrame to collect the final results
df_final = spark.createDataFrame([], schema)

# Loop through all fiscal periods from start_fisc_per to end_fisc_per
for fisc_per in range(int(v_start_fisc_per), int(v_end_fisc_per) + 1):
    df = get_ytd_performance(str(fisc_per))
    df_final = df_final.union(df)

# Add calculated fields for negative revenue, gross profit, fdm, and operating profit
df_final = df_final.withColumn(
    "negative_revenue", when(col("revenue_after_curve") < 0, "X").otherwise("")
).withColumn(
    "negative_gp", when(col("gross_profit") < 0, "X").otherwise("")
).withColumn(
    "negative_fdm", when(col("fdm_after_curve") < 0, "X").otherwise("")
).withColumn(
    "negative_op", when(col("operating_profit") < 0, "X").otherwise("")
) 

# Filter out rows where all negative fields are empty
df_final = df_final.filter(
    (col("negative_revenue") != "") | 
    (col("negative_gp") != "") | 
    (col("negative_fdm") != "") | 
    (col("negative_op") != "")
)

# Optionally, write the result to the target table
df_final.write.format("delta").mode("overwrite").saveAsTable("dev_gold.profitability.customer_performance")

# COMMAND ----------

query = f"""
CREATE OR REPLACE VIEW dev_gold.profitability.negative_performance AS
SELECT 
    fp.*,   
    cp.customer, cp.fisc_per, cp.negative_revenue, cp.negative_gp, cp.negative_fdm, cp.negative_op,   
    co.SK_Outlet, 
    co.MEPNo, 
    co.RegionDesc, 
    co.CustomerName, 
    co.SalesRoute, 
    co.Latitude, 
    co.Longtitude, 
    co.Region, 
    co.TrainStation, 
    co.Province, 
    co.StoreSizeSTT, 
    co.ServiceModel, 
    co.OutletGroup, 
    co.SuppressionReason, 
    co.BusinessType, 
    co.CUDChannelHead, 
    co.TradeName, 
    co.DelLoc, 
    co.DelLocCode, 
    co.StoreSizeSTTDesc, 
    co.ServiceModelDesc, 
    co.OutletGroupDesc, 
    co.TradeNameDesc, 
    co.SalesOrg, 
    co.SalesOrgDesc, 
    co.CU, 
    co.CUDesc, 
    co.GTMT, 
    co.GTMTDesc, 
    co.CUDChannelHeadDesc, 
    co.RSM_MTMCut, 
    co.RSM_MTMCutDesc, 
    co.TSM_MTAMCut, 
    co.TSM_MTAMCutDesc, 
    co.AccountManager, 
    co.AccountManagerDesc, 
    co.AccountManagerHead, 
    co.AccountManagerHeadDesc, 
    co.BusinessTypeDesc, 
    co.ChannelSTT, 
    co.ChannelSTTDesc, 
    co.SubChannelSTT, 
    co.SubChannelSTTDesc, 
    co.DirectIndirect, 
    co.DirectIndirectDesc, 
    co.SuppressionReasonDesc, 
    co.SalesRouteDesc, 
    co.FrontlinerType, 
    co.FrontlinerTypeDesc, 
    co.VisitStrategy, 
    co.VisitStrategyDesc,
    cm.MaterialDescription, 
    cm.MajorBrand, 
    cm.SubBrand, 
    cm.ServingSize, 
    cm.PackSize, 
    cm.Package, 
    cm.Category, 
    cm.SubCategory, 
    cm.PackClass, 
    cm.ProfitabilityClass1, 
    cm.IsBIB, 
    cm.PackSegment, 
    cm.DeleteFlag, 
    cm.MultipackSinglepack, 
    cm.MaterialDescriptionClean, 
    cm.PackGroup, 
    cm.GTFocusSKU, 
    cm.MTFocusSKU, 
    cm.Quantity, 
    cm.MaterialType,
    cf.Year,
    cf.PeriodNumber,
    cf.RoutingDays,
    cf.PeriodStart,
    cf.PeriodEnd,
    cf.PeriodLabel,
    cf.YearPeriod,
    cf.PeriodName,
    cf.PeriodYear
FROM 
    dev_gold.fact.profitability01 fp
INNER JOIN 
    dev_gold.profitability.customer_performance cp
    ON fp.CustomerNo = cp.customer AND
    fp.FiscalPeriod = cp.fisc_per 
INNER JOIN 
    dev_gold.common_dim.fisc_per cf
    ON fp.FiscalPeriod = cf.FiscalPeriod
LEFT OUTER JOIN 
    dev_gold.common_dim.current_outlets co
    ON fp.CustomerNo = co.CustomerNo
LEFT OUTER JOIN 
    dev_gold.common_dim.current_materials cm
    ON fp.MaterialNo = cm.MaterialNo """

spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dev_gold.profitability.negative_performance
