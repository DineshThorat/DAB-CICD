# Databricks notebook source
# DBTITLE 1,set global parameters
v_start_fisc_per = dbutils.widgets.get("start_fisc_per")
v_end_fisc_per = dbutils.widgets.get("end_fisc_per")
dbutils.widgets.text("environment", "Dev", "")
Env = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,If no period set, use current period
from datetime import datetime
from pyspark.sql.functions import lit,col
current_date = datetime.now()
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

# DBTITLE 1,Import Libraries

import pandas as pd
import os
from datetime import datetime,date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DateType,DoubleType,BooleanType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,get common parameters
# MAGIC %run /Workspace/Profitability/DataTransformation(GoldLayer)/common_parameters

# COMMAND ----------

# DBTITLE 1,set schema's based on Environment
bronze_schema,silver_schema,gold_schema = create_config(Env)
print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# DBTITLE 1,get GL Headers
df_gl_header = spark.table(f'{silver_schema}.ecc.gl_header').filter((col("fisc_per") >= v_start_fisc_per ) & (col("fisc_per") <= v_end_fisc_per))

# COMMAND ----------

# DBTITLE 1,get all mapping tables
df_pcm = spark.table(f'{silver_schema}.sharepoint.pc_mapping')
df_glm = spark.table(f'{silver_schema}.sharepoint.gl_mapping')
df_fam = spark.table(f'{silver_schema}.sharepoint.fa_mapping')
df_ccm = spark.table(f'{silver_schema}.sharepoint.cc_mapping')
# this is only temporary. once the FUNCTION column is available directly in the cc_mapping table, then remove the use of the tmp table
df_ccm_tmp = spark.table(f'{bronze_schema}.tmp.cc_mapping')
df_ccm = df_ccm.join(df_ccm_tmp, on="cost_center", how="leftouter").drop(df_ccm_tmp.cost_center_name)

# COMMAND ----------

# DBTITLE 1,get Line items for L1 Local Ledger
df_gl_L1_items = spark.table(f'{silver_schema}.ecc.gl_items_l1').alias("gli") \
    .join(df_gl_header.alias("glh"), on=["acc_docnum", "fisc_year"], how="inner") \
    .join(df_pcm.alias("pcm"), on=["profit_center"], how="leftouter") \
    .filter((col("glh.comp_code") == ccbpi_comp_code) & (col("gli.ledger") == gl_local_ledger) & (col("glh.doc_status") == 'L') \
        & (col("gli.gl_account").between(gl_pnl_accounts_low, gl_pnl_accounts_high))) \
    .withColumn("assigned_fa", when(col("gli.func_area").isNull(), \
                                    when(col("pcm.new_pl") == 'MU', lit('B30')).otherwise( \
                                    when(col("pcm.new_pl") == 'LU', lit('B24')).otherwise( \
                                    when(col("pcm.new_pl") == 'CU', lit('B24')).otherwise( \
                                    when(col("pcm.new_pl") == 'TU', lit('B23')).otherwise( \
                                    when(col("pcm.new_pl") == 'HO', lit('B24')) \
                                        ))))) \
                                .otherwise(col("gli.func_area"))) \
    .groupBy("glh.fisc_year","glh.fisc_per","gli.gl_account","gli.cost_center","gli.profit_center","func_area","assigned_fa","new_pl") \
        .agg(sum("gli.debit_amount").alias("debit"), sum("gli.credit_amount").alias("credit"), sum("gli.balance").alias("balance"))

# COMMAND ----------

# DBTITLE 1,get Line items for 0L Leading Ledger
df_gl_0L_items = spark.table(f'{silver_schema}.ecc.gl_items').alias("gli") \
    .join(df_gl_header.alias("glh"), on=["acc_docnum", "fisc_year"], how="inner") \
    .join(df_pcm.alias("pcm"), on=["profit_center"], how="leftouter") \
    .filter((col("glh.comp_code") == ccbpi_comp_code) & (col("glh.ledger_grp").isNull()) & (col("glh.doc_type") != 'LV') \
        & (col("gli.gl_account").between(gl_pnl_accounts_low, gl_pnl_accounts_high)) \
        & (col("glh.doc_status").isNull() | ~col("glh.doc_status").isin('L', 'D'))) \
    .withColumn("assigned_fa", when(col("gli.func_area").isNull(), \
                                    when(col("pcm.new_pl") == 'MU', lit('B30')).otherwise( \
                                    when(col("pcm.new_pl") == 'LU', lit('B24')).otherwise( \
                                    when(col("pcm.new_pl") == 'CU', lit('B24')).otherwise( \
                                    when(col("pcm.new_pl") == 'TU', lit('B23')).otherwise( \
                                    when(col("pcm.new_pl") == 'HO', lit('B24')) \
                                        ))))) \
                                .otherwise(col("gli.func_area"))) \
    .groupBy("glh.fisc_year","glh.fisc_per","gli.gl_account","gli.cost_center","gli.profit_center","func_area","assigned_fa","new_pl") \
    .agg(sum("gli.debit_amount").alias("debit"), sum("gli.credit_amount").alias("credit"), sum("gli.balance").alias("balance"))

# COMMAND ----------

# DBTITLE 1,Union the Local and Leading Ledger removing duplicates
df_gl_balance = df_gl_L1_items.union(df_gl_0L_items).dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Derive all Fields for CCBPI P&L
df_pnl_balance = df_gl_balance.alias("gli") \
    .join(df_glm.alias("glm"), on=["gl_account"], how="left") \
    .join(df_ccm.alias("ccm"), on=["cost_center"], how="left") \
    .join(df_fam.alias("fam"), on=[df_gl_balance["assigned_fa"] == df_fam["bcode"]], how="left") \
    .withColumn("operating_unit",when((col("gli.gl_account").isin('865101200','865101000')) & (col("gli.profit_center") == 'PH12150121'), lit('CU')) \
        .otherwise(when(col("gli.new_pl").isNotNull(), col("gli.new_pl")) \
            .otherwise(lit("OTHER")))) \
    .withColumn("fs_hierarchy_1", when(col("glm.class_per_bi") == 'FA', col("fam.class_per_bi")) \
        .otherwise(when(col("glm.class_per_bi").isNotNull(), regexp_replace(col("glm.class_per_bi"), '                                 ', '')) \
            .otherwise(when((col("fam.class_per_bi").isNull()) | (col("glm.class_per_bi").isNull()), lit('UNASSIGNED'))))) \
    .withColumn("function",when(upper(col("fs_hierarchy_1")).isin('NET INTEREST', 'ALL OTHER', 'COGS', 'GROSS REVENUES', 'INCOME TAXES', 'REVENUE DEDUCTIONS','DIRECT MARKETING EXPENSES','HO ALLOCATED COST'), upper(col("fs_hierarchy_1"))) \
        .otherwise(when((col("gli.profit_center") == 'PH12150121') & (col("gli.gl_account").isin('865101000','865101200')),lit('COMMERCIAL')) \
            .otherwise(when((col("glm.level3_opexpnl_class") == 'FREIGHT TPD') & (col("operating_unit") == 'CU'),lit('TRANSPORT')) \
                .otherwise(when((col("gli.cost_center").isNotNull()) & (col("ccm.functional_reporting").isNotNull()), col("ccm.functional_reporting")) \
                    .otherwise(when(col("gli.cost_center").isNull(), \
                        when(col("fam.function").isNotNull(),col("fam.function")) \
                        .otherwise(when(col("gli.profit_center").isin('PH12150100', 'PH12150109', 'PH12150109', 'PH12150153', 'PH12150225', 'PH12150200', 'PH1215F160', 'PH1215F160'),lit('WHSE & DISTRIBUTION')) \
                            .otherwise(when(col("gli.profit_center").isin('PH12157104','PH1215A075','PH1215A076','PH12157100'),lit('COMMERCIAL')) \
                                .otherwise(when(col("operating_unit") == 'TU', lit('TRANSPORT')) \
                                    .otherwise(when(col("operating_unit").isin('MU', 'LU', 'HO'), lit('WHSE & DISTRIBUTION')) \
                                        .otherwise(when(col("operating_unit").isin('CU', 'RSM'), lit('COMMERCIAL')) \
                                            .otherwise(lit('OTHER')) \
                                                    ))))))))))) \
    .withColumn("FS_Items",when(col("function") == "TRANSPORT", "Transport")
        .when(
            col("function").isin("WHSE & DISTRIBUTION", "MANUFACTURING", "MANUF"),
            "Logistic Operating Expenses (Whse & Distribution)"
        )
        .when(
            col("function").isin("COMMERCIAL", "Modern Trade"),
            "Commercial Operating Expenses (Commercial)"
        )
        .when(
            col("fs_hierarchy_1").isin(
                "Net Interest", "All Other", "COGS", "Gross Revenues", 
                "Income Taxes", "Revenue Deductions", "Direct Marketing Expenses"
            ),
            col("fs_hierarchy_1")
        )
        .when(col("fs_hierarchy_1").like("%HO%"), "COGS")
    .otherwise("Support Services (General & Administrative)")) \
    .withColumn("cdo", when((col("glm.cdo_tagging") == "CDO") | \
        ((col("glm.cdo_tagging").isNull()) & (col("glm.pnl_level1_class") == "Operating Expenses") & (col("ccm.profit_tool2") == "CDO")),lit("yes")) \
            .otherwise(lit("no"))) \
    .groupBy("fisc_year", "fisc_per","gli.gl_account","gli.cost_center","gli.profit_center","gli.func_area","gli.assigned_fa","gli.new_pl","glm.class_per_bi","glm.level3_opexpnl_class","fs_hierarchy_1","function","operating_unit","FS_Items", "cdo") \
    .agg(sum("debit").alias("debit"), sum("credit").alias("credit"), sum("balance").alias("balance")) \
    .withColumnRenamed("level3_opexpnl_class","classification_int")

# COMMAND ----------

# DBTITLE 1,check Balance
# display(df_pnl_balance.where(col("fisc_year") == 2024).groupBy(col("fisc_per")).agg(sum(col("balance"))).orderBy(col("fisc_per")))

# COMMAND ----------

# DBTITLE 1,check P&L Lines
# display(df_pnl_balance.groupBy(col("FS_Items")).agg(sum(col("debit")), sum(col("credit")), sum(col("balance"))))

# COMMAND ----------

# DBTITLE 1,create target table
create_query = f"""
create table if not exists {gold_schema}.profitability.gl_balances
(fisc_year int COMMENT 'Fiscal Year',
fisc_per string COMMENT 'Fiscal Year and Period in SAP Database Format YYYYPPP',
gl_account string COMMENT 'General Ledger Account Number',
cost_center string COMMENT 'Cost Center Code',
profit_center string COMMENT 'Profit Center Code',
func_area string COMMENT 'Functional Area Code',
assigned_fa string COMMENT 'Derived Functional Area Code based on logic from Month-End P&L',
new_pl string COMMENT 'from the Profit Center Mapping Table',
class_per_bi string COMMENT 'from the GL Mapping Table',
classification_int string COMMENT 'Derived value that is either equal to fs_hierarchy_1 or looked up from GL Mapping table based on business rules',
fs_hierarchy_1 string,
function string COMMENT 'This is the main column that drives the P&L tagging',
operating_unit string COMMENT 'Operating Unit',
FS_Items string COMMENT ' a grouping of values of the function column. these are the final P&L line values',
cdo string COMMENT 'defines whether each row in this table is relevant for Cold Drink Operations (CDO). Possible values are yes or no',
debit decimal(38,5),
credit decimal(38,5),
balance decimal(38,5))
COMMENT 'contains the General Ledger balances for the L1 Ledger for company code 1215 and the GL Account range 300000000 to 899999999. 
Data is aggregated by GL Account, Cost Center, Profit Center, Functional Area. There are multiple dervied columns which are used to classify the data into P&L Lines based on business logic contained in the month-end P&L Excel files'
CLUSTER BY (fisc_per, FS_Items, gl_account, cost_center)
"""

spark.sql(create_query)

# COMMAND ----------

# DBTITLE 1,remove data from target table for selected periods
query = f"""
DELETE FROM {gold_schema}.profitability.gl_balances
WHERE fisc_per >= '{v_start_fisc_per}'
  AND fisc_per <= '{v_end_fisc_per}'
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,write to Target Table
df_pnl_balance.write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(f'{gold_schema}.profitability.gl_balances')
