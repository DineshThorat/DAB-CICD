# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum, lit, expr
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

dbutils.widgets.text("start_fisc_per", "", "")
dbutils.widgets.text("end_fisc_per", "", "")
dbutils.widgets.text("environment", "Dev", "")

# COMMAND ----------

v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
Env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run
# MAGIC /Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# MAGIC %run
# MAGIC /Profitability/DataTransformation(GoldLayer)/reusable_object_calculation/call_reusable_business_logic

# COMMAND ----------

# MAGIC %run 
# MAGIC ../../Profitability/DataTransformation(GoldLayer)/std_cogs_latest_rec_per_period

# COMMAND ----------

bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# DBTITLE 1,Creating DF from tables
df_copa_items = spark.read.table(f"{silver_schema}.ecc.copa_items").select("fisc_per", "material", "physical_case_qty", "fisc_year","sales_doc_type")
df_mat = spark.read.table(f"{gold_schema}.common_dim.current_materials").select("MaterialNo", "MajorBrand", "MaterialType", "MaterialDescription")
df_per = spark.read.table(f"{gold_schema}.common_dim.fisc_per").select("FiscalPeriod","PeriodStart","PeriodEnd","Year")
df_std_cogs = std_cogs_call(bronze_schema,silver_schema,gold_schema,v_start_fisc_per,v_end_fisc_per).select("fisc_per", "material", "conv_full_case", "conv_to_unit_case","__START_AT","__END_AT")
# df_std_cogs = spark.read.table(f"{silver_schema}.sharepoint.standard_cogs").select("fisc_year", "material", "conv_full_case", "conv_to_unit_case","__START_AT","__END_AT")

# COMMAND ----------

export_skus = list(export_skus)
CO2_skus = list(CO2_skus)

# COMMAND ----------

# DBTITLE 1,Join and filter
joined_df = df_copa_items \
    .join(df_mat, df_copa_items.material == df_mat.MaterialNo, "left_outer")


year_part = str(v_start_fisc_per)[:4]
base_fisc_per = year_part + '001'

filtered_df = joined_df \
    .filter(
        (joined_df.fisc_per >= base_fisc_per) &
        (joined_df.fisc_per <= v_end_fisc_per) &
        (~joined_df.material.isin(export_skus)) &
        (~joined_df.material.isin(CO2_skus)) &
        (df_mat.MaterialType == finished_goods_mat_type)
    )

# COMMAND ----------

# DBTITLE 1,sum physical_case_quantity
sum_filtered_df = filtered_df.groupBy("material", "MajorBrand", "fisc_per","fisc_year") \
    .agg(
        _sum("physical_case_qty").alias("physical_case_qty_sum")
    )

# COMMAND ----------

# Define a window for cumulative sum within each year
year_window = Window.partitionBy("material", "fisc_year").orderBy("fisc_per").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative volume
df_cumulative_vol = sum_filtered_df.withColumn("physical_case_qty_ytd", _sum("physical_case_qty_sum").over(year_window))

# Adjust cumulative sums for year-crossing
df_vol_ytd = df_cumulative_vol.withColumn(
    "cumulative_vol_pc",
    expr("""
        SUM(physical_case_qty_sum) OVER (
            PARTITION BY material ORDER BY fisc_per
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    """)
)

# COMMAND ----------

# display(df_vol_ytd.filter((col("material") == '101650') & (col("fisc_per") == '2024010')))

# COMMAND ----------

df_cogs_joined = df_vol_ytd \
    .join(df_per, df_vol_ytd.fisc_per == df_per.FiscalPeriod, "left_outer")\
    .join(df_std_cogs,(df_vol_ytd.fisc_per == df_std_cogs.fisc_per) 
        & (df_vol_ytd.material == df_std_cogs.material),"left_outer") \
    .filter((df_vol_ytd.fisc_per >= v_start_fisc_per)) \
    .drop(df_vol_ytd.cumulative_vol_pc, df_std_cogs.material, df_std_cogs.fisc_per, df_per.FiscalPeriod, df_per.PeriodStart, df_per.PeriodEnd, df_per.Year, df_std_cogs.__START_AT, df_std_cogs.__END_AT)

# COMMAND ----------

# display(df_cogs_joined.filter(col("material") == '101650'))

# COMMAND ----------

# DBTITLE 1,creating financial_vol_uc_ytd columns
df_final = df_cogs_joined \
.withColumn("mktg_vol_uc_ytd", 
    col("physical_case_qty_ytd") * col("conv_full_case") * col("conv_to_unit_case")
)

brand_window_spec = Window \
    .partitionBy("MajorBrand", "fisc_per") \
    .orderBy("fisc_per")

df_final = df_final \
    .withColumn("total_brand_qty_uc_ytd", _sum("mktg_vol_uc_ytd").over(brand_window_spec)) \
    .withColumn("mix_brand_uc_ytd", (col("mktg_vol_uc_ytd") / col("total_brand_qty_uc_ytd")).cast('decimal(38,18)')) \
    .select('fisc_per','fisc_year','material','MajorBrand','mktg_vol_uc_ytd','total_brand_qty_uc_ytd','mix_brand_uc_ytd')


# COMMAND ----------

# MAGIC %md
# MAGIC ### create target table if not exists

# COMMAND ----------

create_query = f"""
create table if not exists {gold_schema}.profitability.vol_mix_brandmat
(fisc_per STRING COMMENT 'Fiscal Year and Period in SAP Database Format YYYYPPP',
fisc_year INT COMMENT 'Fiscal Year in format YYYY',
material STRING COMMENT 'SAP Material Code',
MajorBrand STRING COMMENT 'The MajorBrand from the Material Master',
mktg_vol_uc_ytd DECIMAL(38,6) COMMENT 'Marketing Volume Year to Date in Unit Cases for the Material',
total_brand_qty_uc_ytd DECIMAL(38,6) COMMENT 'Marketing Volume Year to Date in Unit Cases for the MajorBrand',
mix_brand_uc_ytd DECIMAL(38,18) COMMENT 'Brand Mix for the Material. calculated as Material Volume YTD divided by Brand Volume YTD')
COMMENT 'contains the Year to Date volume and YTD Brand volume mix for each material in each period.'
CLUSTER BY (fisc_per)
"""

spark.sql(create_query)

# COMMAND ----------

# DBTITLE 1,Deletion Query
delete_query = f"""
DELETE FROM {gold_schema}.profitability.vol_mix_brandmat
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(delete_query)

# COMMAND ----------

try:
    df_final.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{gold_schema}.profitability.vol_mix_brandmat")
except Exception as e:
    raise Exception(f"Error writing to table: {e}")
