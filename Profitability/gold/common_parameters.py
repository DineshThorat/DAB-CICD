# Databricks notebook source
# DBTITLE 1,Env based Schemas
def create_config(env):
  if env == "prod":
    bronze_schema = "dev_bronze"
    silver_schema = "dev_silver"
    gold_schema ="dev_gold"
  elif env == "test":
    bronze_schema = "dev_bronze"
    silver_schema = "dev_silver"
    gold_schema ="dev_gold" 
  else:
    bronze_schema = "dev_bronze"
    silver_schema = "dev_silver"
    gold_schema ="dev_gold"
  return bronze_schema, silver_schema, gold_schema

# COMMAND ----------

# Table required for the pipeline

# COMMAND ----------

# DBTITLE 1,Common parameters
df_ylp2_cond_condition_type = 'YLP2'
df_ylp2_items_posting_status = 'C'
df_on_inv_disc_bc_condition_type = ('YIC2', 'YIC4', 'YLP9', 'ZA01', 'YPA2', 'YDIF')
df_on_inv_disc_bc_posting_status = 'C'
df_gl_ctm_gl_account = ('408107000', '408107200')
df_gl_other_rev_gl_account_start = '305400000'
df_gl_other_rev_gl_account_end = '305499999'
df_gl_dfr_curve_h_ledger_grp = 'L1'
df_gl_dfr_curve_h_doc_type = 'R3'
df_gl_dfr_curve_i_gl_account = '405801210'
df_gl_dfr_curve_i_cost_center = '1215038109'
df_gl_dfr_curve_i_profit_center = 'PH12150121'
df_from_rms_condition_types = ('YRBA', 'YRB0', 'YRB2', 'YRBI', 'YRBJ', 'YRBK', 'YRB4', 'YRB6', 'YRB1', 'YRB3', 'YRB5', 'YRBB', 'YRBL', 'YRBT', 'YRBN', 'YRBC', 'YRBM', 'YRBS', 'YRBO', 'YRBR', 'YRBP', 'YRBQ')
df_from_rms_posting_status = 'C'
df_var_disc_rms_condition_types = ('YRBA', 'YRB0', 'YRB2', 'YRBI', 'YRBJ', 'YRBK', 'YRB4', 'YRB6', 'YRB1', 'YRBC', 'YRBM', 'YRBS', 'YRBO', 'YRBR', 'YRBP', 'YRBQ')
dmi_rms_condition_types = ('YRB3', 'YRB5', 'YRBB', 'YRBL', 'YRBT', 'YRBN')
df_variable_disc_amount_condition_value = ('YRBA' ,'YRB0','YRB2','YRBJ','YRBI','YRBK','YRB4','YRB6','YRB1')
vat = 1.12
ccbpi_comp_code = '1215'
gl_local_ledger = 'L1'
gl_leading_ledger = '0L'
gl_pnl_accounts_low = '300000000'
gl_pnl_accounts_high = '899999999'
df_haulage_gl_accounts = ('710306230', '845201220', '880103130')
df_haulage_comp_code = '1215'
df_t1_opex_gl_accounts =  ('875108241', '875108242', '875108243', '880103130', '845201200', '845201201')
df_t1_opex_profit_centres = ('PH1215A082', 'PH1215A083', 'PH1215A084', 'PH1215A085', 'PH1215A086', 'PH1215A087', 'PH1215A088', 'PH1215A089', 'PH1215A130', 'PH1215A091', 'PH1215A092', 'PH1215A093', 'PH1215A094', 'PH1215A095', 'PH1215A096', 'PH1215A085', 'PH1215A097', 'PH1215A098', 'PH1215A082', 'PH1215A099', 'PH1215A129', 'PH1215A101', 'PH1215A083', 'PH1215A102', 'PH1215A084', 'PH1215A103', 'PH1215A104', 'PH1215A105', 'PH1215A106', 'PH1215A086', 'PH1215A107', 'PH1215A108', 'PH1215A109', 'PH1215A110', 'PH1215A111', 'PH1215A087', 'PH1215A087', 'PH1215A112', 'PH1215A089', 'PH1215A130', 'PH1215A113', 'PH1215A114', 'PH1215A115', 'PH1215A116', 'PH1215A091', 'PH1215A117', 'PH1215A118', 'PH1215A092', 'PH1215A119', 'PH1215A120', 'PH1215A093', 'PH1215A121', 'PH1215A122', 'PH1215A123', 'PH1215A124', 'PH1215A125', 'PH1215A126', 'PH1215A127', 'PH1215A128', 'PH1215A133', 'PH1215A134', 'PH1215A131', 'PH1215A132', 'PH1215A137', 'PH1215A138', 'PH1215A139', 'PH1215A088', 'PH1215A073', 'PH1215A142', 'PH1215A143', 'PH1215A144', 'PH1215A145', 'PH1215A146', 'PH1215A147', 'PH1215A148', 'PH1215A149', 'PH1215A157', 'PH12150121', 'PH1215A158')
df_t1_opex_comp_code = '1215'
finished_goods_mat_type = 'ZFER'
export_skus = ('109580')
CO2_skus = ('101209', '101210', '109800')
