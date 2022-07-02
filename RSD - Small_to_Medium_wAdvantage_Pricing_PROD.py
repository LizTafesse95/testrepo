# Databricks notebook source
# MAGIC %run /DA_CORP_MKT/Utilities/Connections_PROD

# COMMAND ----------

import pandas as pd;
from pandas import ExcelWriter
# from sqlalchemy import types, create_engine
from datetime import date, timedelta
from datetime import datetime
import calendar
from pyspark.sql.types import *
#from pyspark.sql.functions import *

from pyspark.sql.window import Window

from pyspark.sql.types import StringType, FloatType, IntegerType, DateType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /DA_CORP_MKT/Utilities/Std_Start_PROD

# COMMAND ----------

if (run_env == 'tfsprod'):
  s3_bucket = "tfsdl-corp-mkt-ws-prod" #Prod S3 bucket
else: #assume 'tfsedp'
  s3_bucket = "tfsdl-corp-mkt-ws-test" #QA S3 bucket
print(s3_bucket)

# COMMAND ----------

# MAGIC %md ## Bringing In Data from Tables

# COMMAND ----------

def jdbc_conn_config(host,port,db,username,password):
  jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(host, port, db, username, password)
  connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize":"10000"}
  return (jdbcUrl,connectionProperties)
# Connection String
JDBC_host = '10.0.117.71'
JDBC_port = '1433'
JDBC_db = 'PROJECTS_DataScience'
JDBC_username = 'IT_DataScience'
JDBC_password = dbutils.secrets.get('DACorpMktTier1_JDBC_Other_Scope','IT_DataScience_PassWord') 

url,conn = jdbc_conn_config(JDBC_host, JDBC_port, JDBC_db,JDBC_username,JDBC_password)

model_query = """
(select * FROM PROJECTS_DataScience.dbo.DSE_SM_RSD) tab_alias
"""

model_DF = spark.read.jdbc(url=url,
            table=model_query,
            properties=conn)

# COMMAND ----------

custkey_tog_ref_query = """
(select * FROM PROJECTS_DataScience.dbo.DSE_SM_RSD_TOG) tab_alias
"""

custkey_tog_ref_DF = spark.read.jdbc(url=url,
            table=custkey_tog_ref_query,
            properties=conn)

# COMMAND ----------

#display(custkey_tog_ref_DF)

# COMMAND ----------

# MAGIC %md ##Checking model data with reference tables

# COMMAND ----------

custcheck_DF = model_DF \
  .join(custkey_tog_ref_DF, custkey_tog_ref_DF.GRP_ACCT_TOP_CODE == model_DF.TOG) \
  .drop(custkey_tog_ref_DF.GRP_ACCT_TOP_CODE)

# COMMAND ----------

# display(custcheck_DF)

# COMMAND ----------

custDataQRY="""(SELECT DISTINCT CUST_NBR,
                   EMAIL,
                   EMAIL_KEY  
                FROM CCGDW.D_CUST DC
                INNER JOIN ( 
                SELECT CUST_KEY, CONTACT_KEY FROM CCGDW.F_CUST_CONTACT_ASSOC C             
                           ) C
                      ON DC.CUST_KEY=C.CUST_KEY
                INNER JOIN D_CONTACT DCT
                      ON DCT.CONTACT_KEY =C.CONTACT_KEY
             where EMAIL_KEY > 0
           ) CUST"""

custDataDF = spark.read.jdbc(url=url_CCG,
                        table=custDataQRY,
                        properties=conn_CCG)

#remove any email addresses with more than 1 email key
custDataDF = custDataDF.withColumn(
  "EMAIL_RN", row_number().over(Window.partitionBy("EMAIL").orderBy(desc("EMAIL_KEY")))).where('EMAIL_RN = 1').drop('EMAIL_RN')

wcs_query = "(SELECT DISTINCT WCS_USER_KEY, EMAIL_KEY FROM CCGCM.D_WCS_USER) WCS"

wcsDF = spark.read.jdbc(url=url_CCG,
                    table=wcs_query,
                    properties=conn_CCG)

visitor_query = "(SELECT DISTINCT VISITOR_KEY, POST_VISID_HIGH, POST_VISID_LOW, WCS_USER_KEY FROM CCGCM.D_OMNI_VISITOR) VIS"

visitorDF = spark.read.jdbc(url=url_CCG,
                    table=visitor_query,
                    properties=conn_CCG)

hit_query = "(SELECT DISTINCT VISITOR_KEY FROM CCGCM.F_OMNI_HIT_DATA WHERE HIT_TIME_GMT_TS >= (CURRENT_DATE - 90)) HIT"

hitDF = spark.read.jdbc(url=url_CCG,
                    table=hit_query,
                    properties=conn_CCG)

wcsDF.cache()
visitorDF.cache()
hitDF.cache()
custDataDF.cache()

# COMMAND ----------

unfiltered_DF = custcheck_DF \
  .join(custDataDF, custcheck_DF.CUST_NUMBER_KEY_FK == custDataDF.CUST_NBR)

unfiltered_DF = unfiltered_DF.select('TOG','segment','Leaf_node','skutouse','Opportunity','LTM_sales','Last30d_sales','Last60d_sales','Last90d_sales', 'Last180d_sales', 'CMT2', 'Leaf_node_description', 'CUST_NBR','EMAIL', 'EMAIL_KEY') \
  .withColumnRenamed('segment','SEGMENT') \
  .withColumnRenamed('Leaf_node','LEAFNODE') \
  .withColumnRenamed('skutouse','SKU') \
  .withColumnRenamed('EMAIL_KEY','email_key1') \
  .withColumn('LTM_sales', unfiltered_DF["LTM_sales"].cast(IntegerType())) \
  .withColumn('Last30d_sales', unfiltered_DF["Last30d_sales"].cast(IntegerType())) \
  .withColumn('Last60d_sales', unfiltered_DF["Last60d_sales"].cast(IntegerType())) \
  .withColumn('Last90d_sales', unfiltered_DF["Last90d_sales"].cast(IntegerType())) \
  .withColumn('Last180d_sales', unfiltered_DF["Last180d_sales"].cast(IntegerType())) \
  #.withColumn('Opportunity', unfiltered_DF["Opportunity"].cast(IntegerType())) \

unfiltered_DF.toDF(*[c.upper() for c in unfiltered_DF.columns])

unfiltered_DF.cache()

# COMMAND ----------

# File location and type
file_location = "s3://tfsdl-corp-mkt-ws-test/elizabeth.tafesse/FS_CNTCT_HIST_202104281145.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
targetedemailkeys = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(excluded_accounts) #10151 unique accounts

# COMMAND ----------

final_email_key_df= unfiltered_DF.join(targetedemailkeys, unfiltered_DF.email_key1 == targetedemailkeys.EMAIL_KEY,'inner')

# COMMAND ----------

# final_email_key_df.select('email_key1').dropDuplicates().count()

# COMMAND ----------

# MAGIC %md ##Importing 9Digit Exclusion CSV

# COMMAND ----------

# File location and type
file_location = "s3://tfsdl-corp-mkt-ws-test/elizabeth.tafesse/List_of_All_9_digits.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
excluded_accounts = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(excluded_accounts) #10151 unique accounts

# COMMAND ----------

#FINAL CSV FOR EXCLUSION
final_excluded_accounts = excluded_accounts.drop('PA_NBR', 'PA_NAME') \
  .withColumnRenamed('9 digit account number', 'nine_digit_account_number') \

# COMMAND ----------

# MAGIC %md ## Adding in SKU Ranking to Unfiltered_DF

# COMMAND ----------

for colN in unfiltered_DF.columns:
  unfiltered_DF = unfiltered_DF.withColumnRenamed(colN, colN.lower())

# COMMAND ----------

ordered_DF = unfiltered_DF.withColumn("sku_rn", row_number().over(Window.partitionBy("email").orderBy(desc("opportunity"))))

# COMMAND ----------

first_ordered_DF= ordered_DF.join(final_excluded_accounts, ordered_DF.cust_nbr == final_excluded_accounts.nine_digit_account_number, 'left')

# COMMAND ----------

final_ordered_DF = first_ordered_DF.select('tog', 'segment', 'leafnode', 'sku', 'opportunity', 'ltm_sales', 'last30d_sales', 'last60d_sales', 'last90d_sales', 'last180d_sales', 'cmt2', 'leaf_node_description', 'cust_nbr', 'email', 'email_key1', 'sku_rn', 'nine_digit_account_number').where('nine_digit_account_number is null')

# COMMAND ----------

# MAGIC %md ##Advantage Indicator (CSV)

# COMMAND ----------

# File location and type
file_location = "s3://tfsdl-corp-mkt-ws-test/elizabeth.tafesse/Advantage_PA_Pricing_Final1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
advatange_acct_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
#-------------------------------------------------------------------------------------------------------------------------
# File location and type
file_location = "s3://tfsdl-corp-mkt-ws-test/elizabeth.tafesse/Advantage_PA_Pricing_Final1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
advatange_sku_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

clean_advatange_acct_df=advatange_acct_df.select('ACCOUNT') \
.where('ADVANTAGE_FLAG = "Y"')\
.withColumnRenamed('ACCOUNT', 'Adv_Acct') \

clean_advatange_sku_df=advatange_acct_df.select('SKU', 'System_Price') \
.where('ADVANTAGE_FLAG = "Y"')\
.withColumnRenamed('SKU', 'Adv_Sku') \
.withColumnRenamed('System_Price', 'Target_Price') \

# COMMAND ----------

df_with_acct = final_ordered_DF.join(clean_advatange_acct_df, final_ordered_DF.cust_nbr == clean_advatange_acct_df.Adv_Acct, 'left') \

# Final_DF_All= essentialsfull_DF.join(BID_SG_Exclusion_DF, essentialsfull_DF.EMAIL_ADDRESS == BID_SG_Exclusion_DF.EMAIL_ADDRESS, 'left_anti')

# COMMAND ----------

display(df_with_acct)

# COMMAND ----------

df_with_all = df_with_acct.join(clean_advatange_sku_df, df_with_acct.sku == clean_advatange_sku_df.Adv_Sku, 'left') \

# COMMAND ----------

display(df_with_all)

# COMMAND ----------

Acct_Ind_DF = df_with_all\
.withColumn('Adv_Acct', F.when(col('Adv_Acct').isNull(), lit('0')).otherwise(lit('1')))

# COMMAND ----------

Total_Ind_DF = Acct_Ind_DF\
.withColumn('Adv_Sku', F.when(col('Adv_Sku').isNull(), lit('0')).otherwise(lit('1')))

# COMMAND ----------

display(Total_Ind_DF)

# COMMAND ----------

Advantage_Ind_DF=Total_Ind_DF \
.select('tog', 'segment', 'leafnode', 'sku', 'opportunity', 'ltm_sales', 'last30d_sales', 'last60d_sales', 'last90d_sales', 'last180d_sales', 'cmt2', 'leaf_node_description', 'cust_nbr', 'email', 'email_key1', 'sku_rn', 'nine_digit_account_number', 'Adv_Acct', 'Adv_Sku', 'Target_Price') \
# .where('Adv_Acct = "1" and Adv_Sku = "1"')

# COMMAND ----------

#create new data frame with One joined Ind, join back to Total Ind DF with new column...drop old ind columns.

# COMMAND ----------

# MAGIC %md ## Registered Users Indicator

# COMMAND ----------

RS_jdbcUrl = "jdbc:redshift://rs-custdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscustdm"
RS_port = '5439'
RS_db = 'rscustdm'
RS_username = 'ccgds_read'
RS_password = dbutils.secrets.get('DACorpMktTier1_JDBC_Other_Scope','Redshift_rscustdm_ccgds_read_PassWord')
AWS_BUCKET_NAME = "s3a://"+str(s3_bucket)+"/Loren/Redshift_CCGDS"

view_query = f'SELECT email_key '\
f'FROM ccgds.d_wcs_user'
f'WHERE (profl_type_cd in ("B", "C")) AND cntry_cd = "US" AND ' \
f'reg_type_cd = "R" AND email_key > 0'

registered_user_df = spark.read \
      .format("com.databricks.spark.redshift") \
      .option("url", RS_jdbcUrl) \
      .option("user",RS_username) \
      .option("password",RS_password) \
      .option("query", view_query) \
      .option("extracopyoptions", "acceptinvchars ' '") \
      .option("tempformat","CSV GZIP") \
      .option("forward_spark_s3_credentials", True) \
      .option("tempdir", AWS_BUCKET_NAME+"/temp/rsd_gap_model/") \
      .load()

# COMMAND ----------

new_registered_user_df = registered_user_df.select('email_key') \
.withColumnRenamed('email_key', 'registered_user') \

# COMMAND ----------

official_df = Advantage_Ind_DF.join(new_registered_user_df, (Advantage_Ind_DF.email_key1 == new_registered_user_df.registered_user), 'left') \

# COMMAND ----------

#Registered User Indicator
indicator_DF = (official_df.withColumn('registered_user', F.when(col('registered_user').isNull(), lit('0')).otherwise(lit('1')))) #is null = not registered user | is not null = registered user

# COMMAND ----------

official_ordered_df = indicator_DF.select('tog', 'segment', 'leafnode', 'sku', 'opportunity', 'ltm_sales', 'last30d_sales', 'last60d_sales', 'last90d_sales', 'last180d_sales', 'cmt2', 'leaf_node_description', 'cust_nbr', 'email', 'email_key1', 'sku_rn', 'nine_digit_account_number','registered_user','Adv_Acct', 'Adv_Sku','Target_Price')

# COMMAND ----------

# MAGIC %md ## Bringing in Web Data

# COMMAND ----------

webjoin_reference_DF = wcsDF \
  .join(visitorDF, (wcsDF.WCS_USER_KEY == visitorDF.WCS_USER_KEY) & (visitorDF.WCS_USER_KEY > 0)) \
  .join(hitDF, hitDF.VISITOR_KEY == visitorDF.VISITOR_KEY) \
  .drop(visitorDF.WCS_USER_KEY) \
  .drop(wcsDF.WCS_USER_KEY) \
  .drop(visitorDF.VISITOR_KEY)

webjoin_reference_DF.cache()
webjoin_reference_DF.count()

# COMMAND ----------

# MAGIC %md ## Bucket 1 - Yes Web, Yes Transacted

# COMMAND ----------

Bucket1_DF= official_ordered_df.join(webjoin_reference_DF, official_ordered_df.email_key1 == webjoin_reference_DF.EMAIL_KEY) #Active on Web And Transactors Inner Join

# COMMAND ----------

# FINAL BUCKET 1
FINALB1=Bucket1_DF.select('TOG', 'SKU', 'EMAIL', 'CUST_NBR', 'email_key1', 'SKU_RN', 'registered_user', 'opportunity','Adv_Acct', 'Adv_Sku','Target_Price').where('Last90d_SALES is not null and LAST90D_SALES > 0').dropDuplicates()

FINALB1.cache()
FINALB1.count()

# COMMAND ----------

# MAGIC %md ## Bucket 4 - No Web w/Yes Transactions

# COMMAND ----------

Bucket4_DF= official_ordered_df.join(webjoin_reference_DF, official_ordered_df.email_key1 == webjoin_reference_DF.EMAIL_KEY,'left_anti')

# Bucket4_DF.cache()
# Bucket4_DF.count()

# COMMAND ----------

FINALB4=Bucket4_DF.select('TOG', 'SKU', 'EMAIL', 'CUST_NBR', 'email_key1', 'SKU_RN', 'registered_user', 'opportunity','Adv_Acct', 'Adv_Sku', 'Target_Price').where('LAST90D_SALES is not null and LAST90D_SALES > 0').dropDuplicates()
FINALB4.cache()
FINALB4.count()

# COMMAND ----------

# MAGIC %md ### Add Offer Codes

# COMMAND ----------

#Active
Bucket1convert_DF = FINALB1.select('TOG', 'SKU', 'EMAIL', 'CUST_NBR', 'email_key1', 'SKU_RN','registered_user', 'opportunity','Adv_Acct', 'Adv_Sku', 'Target_Price') \
  .withColumn('segment_name',lit('OFR59'))
Bucket1convert_DF.cache()
Bucket1convert_DF.count()

# COMMAND ----------

#Inactive
Bucket4convert_DF = FINALB4.select('TOG', 'SKU', 'EMAIL', 'CUST_NBR', 'email_key1', 'SKU_RN', 'registered_user', 'opportunity','Adv_Acct', 'Adv_Sku', 'Target_Price') \
  .withColumn('segment_name',lit('OFR58'))
Bucket4convert_DF.cache()
Bucket4convert_DF.count()

# COMMAND ----------

#Combined both offer code DFs
Union_DF = Bucket1convert_DF.union(Bucket4convert_DF).drop('TOG', 'CUST_NBR') \
  .withColumnRenamed('EMAIL', 'EMAIL_ADDRESS') \
  .withColumnRenamed('email_key1', 'EMAIL_KEY') \
  .withColumnRenamed('segment_name', 'SEGMENT_NAME') \
  .withColumnRenamed('SKU_RN', 'SKU_RANKING') \
  .withColumnRenamed('Adv_Acct', 'ADVANTAGE_INDICATOR') \
  .withColumnRenamed('Adv_Sku', 'ADVANTAGE_SKU_INDICATOR') \
  .withColumnRenamed('Target_Price', 'TARGET_PRICE') \
  .withColumnRenamed('registered_user', 'REGISTERED_USER') \
  .withColumnRenamed('opportunity', 'OPPORTUNITY') \

# COMMAND ----------

display(Union_DF)

# COMMAND ----------

# MAGIC %md ## Bring in PostGres Fisher Sci Product Data

# COMMAND ----------

product_query = "(select * from fs_catalog.products where part_number is not NULL and language = 'en') tab_alias"

products_DF = spark.read.jdbc(url=url_ProductCatalog,
                    table=product_query,
                    properties=conn_ProductCatalog)

# COMMAND ----------

combinedwithprod_DF = Union_DF.join(products_DF, Union_DF.SKU == products_DF.part_number)

# COMMAND ----------

checking_df=combinedwithprod_DF.where('IMAGE_URL_SMALL is not null and EMAIL_ADDRESS not like "%!%" and PRODUCT_TITLE is not null and PRODUCT_TITLE != ""')

# COMMAND ----------

final_DF1 =  checking_df.withColumn(
  "SKU_RANK", row_number().over(Window.partitionBy("EMAIL_ADDRESS").orderBy(asc("SKU_RANKING")))) \
  .drop('SKU_RANKING')

# COMMAND ----------

ok_DF= final_DF1.select('email_key', 'email_address', 'sku_rank', 'segment_name', 'sku', 'product_title', 'image_url_small', 'orderable_product_page_url', 'registered_user', 'opportunity','advantage_indicator', 'advantage_sku_indicator', 'target_price') \
  .withColumnRenamed('orderable_product_page_url', 'destination') \
  .withColumnRenamed('image_url_small', 'image') \
  .withColumnRenamed('email', 'email_address') \
  .withColumnRenamed('path1', 'category') \
  .filter('image is not null or image != ""') \
  .filter('destination is not null or destination != ""') \
  .filter('product_title is not null or product_title != ""') \
  .filter('email_address not like "%!%"') \

# COMMAND ----------

#FINAL DF OFR58 & OFR59
final_ok_df = ok_DF.orderBy("email_address", "SKU_RANK", ascending=True).where("sku_rank > 0 and sku_rank <= 6").select('sku_rank','email_address','email_key','product_title','sku','image','destination', 'segment_name','advantage_indicator', 'advantage_sku_indicator', 'target_price', 'registered_user', 'opportunity').dropDuplicates()

# COMMAND ----------

display(final_ok_df)

# COMMAND ----------

# MAGIC %md ## Email Response DF (Clicks & Opens)

# COMMAND ----------

email_opened_query ="""(SELECT DISTINCT EMAIL_KEY FROM cdwcm.D_EMAIL_RESP_TYPE dert 
INNER JOIN cdwcm.f_email_resp r ON r.RESP_TYPE_KEY = dert.RESP_TYPE_KEY 
AND SRC_SYS_CD = 'ELQ4'
WHERE (ASSET_NM = 'RSD_21-0578-US-EML-Small to MId_ShareGainNonAdvantage_Active Purchasers_Email 1' OR
ASSET_NM = 'RSD_21-0578-US-EML-Small to MId_ShareGainNonAdvantage_Active Purchasers_Email 2' OR
ASSET_NM = 'RSD_US_21-530-0574-EML-RSD_Adv_Active_Data Sci_Small Mid_Email 1_Apr21' OR
ASSET_NM = 'RSD_US_21-530-0574-EML-RSD_Adv_Active_Data Sci_Small Mid_Email 2_Apr21' OR
ASSET_NM = 'FSE_21-530-0566-EML-FSE_Adv_Active_Data Sci_Small Mid_Email 1_Apr21' OR
ASSET_NM = 'FSE_21-530-0566-EML-FSE_Adv_Active_Data Sci_Small Mid_Email 2_Apr21')
AND r.RESP_TYPE_KEY = 2) CUST"""

email_opened_df = spark.read.jdbc(url=url_CDW,
                        table=email_opened_query,
                        properties=conn_CDW)

email_opened_df.cache()
email_opened_df.count()

# COMMAND ----------

activecustomers_query ="""(SELECT * FROM FS_CRDB_USER.FS_CNTCT_HIST WHERE CMP_NM = 'Small_to_Med' AND SEG_NM = 'Seg2') CUST"""

activecustomers_df = spark.read.jdbc(url=url_MKTPD_FS,
                        table=activecustomers_query,
                        properties=conn_MKTPD_FS)

activecustomers_df.cache()
activecustomers_df.count()

# COMMAND ----------

display(activecustomers_df)

# COMMAND ----------

active_email_opened_df= email_opened_df.join(activecustomers_df, email_opened_df.EMAIL_KEY == activecustomers_df.EMAIL_KEY, 'inner') \
.drop(email_opened_df.EMAIL_KEY) \
.dropDuplicates()

active_email_opened_df.count()

#these are the OFR59s that need to becomes OFR60s, so these emails keys need to be removed from the original union above

# COMMAND ----------

display(active_email_opened_df)

# COMMAND ----------

final_ok_df2 = final_ok_df.join(active_email_opened_df, active_email_opened_df.EMAIL_KEY == final_ok_df.email_key, "left_anti")
#removed old OFR59s

# COMMAND ----------

display(final_ok_df2)

# COMMAND ----------

#first only get emails from ok_DF that should be in OFR60
ofr60_df = final_ok_df.where('segment_name = "OFR59"').join(active_email_opened_df.select('email_key').dropDuplicates(), active_email_opened_df.EMAIL_KEY == ok_DF.email_key, 'inner') \
  .drop(active_email_opened_df.EMAIL_KEY) \

#this join removes lines in ofr60_df that we do not want based on email key and sku combo, leaving the next set of skus
ofr60_df = ofr60_df.join(active_email_opened_df, (active_email_opened_df.EMAIL_KEY == ok_DF.email_key) & (active_email_opened_df.PRODUCTCODE == ok_DF.sku), 'left_anti') \

#create new sku ranks, and rename back
ofr60_df =  ofr60_df.withColumn(
  "sku_ranking", row_number().over(Window.partitionBy("email_address").orderBy(asc("sku_rank")))) \
  .drop('sku_rank') \
  .withColumnRenamed('sku_ranking', 'sku_rank') \
  .withColumn('segment_name', lit('OFR60')) \

# COMMAND ----------

display(ofr60_df)

# COMMAND ----------

final_ofr60_df = ofr60_df.orderBy("email_address", "sku_rank", ascending=True).where("sku_rank > 0 and sku_rank <= 6").select('sku_rank','email_address','email_key','product_title','sku','image','destination', 'segment_name','advantage_indicator', 'advantage_sku_indicator', 'target_price','registered_user', 'opportunity').dropDuplicates()

# COMMAND ----------

#union the final ofr60 dataframe with the final ok df2 that had the OFR59s removed 
final_union_df = final_ofr60_df.union(final_ok_df2)

# COMMAND ----------

# MAGIC %md ## Truncating Image & Destination URLS and HTML

# COMMAND ----------

file_location = "s3://tfsdl-corp-mkt-ws-test/loren.chang/HTML_filters_databricks.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
html_DF = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("multiLine", "true") \
  .option("delimiter", delimiter) \
  .option("quote", "\"") \
  .load(file_location) \

# COMMAND ----------

from pyspark.sql.functions import split, explode

for col in html_DF.columns:
  if "unicode_db" in col:
    html_DF = html_DF.withColumnRenamed(col, "unicode_db")

explodedDF = html_DF.withColumn('tag_cd',explode(split('Tags',' '))) \
  .select('tag_cd', 'unicode_db').dropDuplicates()
  
# display(explodedDF)

dictT = explodedDF.rdd.collectAsMap()

def translate_from_dict(original_text,dictionary=dictT):
  for rec in dictionary:
    if dictionary[rec] is None:
      original_text = original_text.replace(rec,"")
    else:
      original_text = original_text.replace(rec,dictionary[rec].replace("\\/", "/").encode().decode('unicode_escape'))
  return original_text

    
translate_from_dictUdf = udf(translate_from_dict, StringType())

# cutting it down and converting it without cutting out "copywrite" part.

# COMMAND ----------

def imageReplaceValue(value):
  return value.replace('http://assets.fishersci.com/TFS-Assets/', '')
imageReplaceUdf = udf(imageReplaceValue, StringType())

def destReplaceValue(value):
  return value.replace('https://www.fishersci.com/shop/products/', '')
destReplaceUdf = udf(destReplaceValue, StringType())

def categoryReplaceValue(value):
  return value.replace('$', '')
categoryReplaceUdf = udf(categoryReplaceValue, StringType())

def getDecimalValue(value):
  if (value is not None and value != 'null'):
    return "$" + str('{:,.2f}'.format(float(value), 2))
cast_decimal_udf = udf(getDecimalValue, StringType())

def limitCharLength(value):
  if len(value) > 70:
    return value[0:66] + str('...')
  else:
    return value
limitCharLengthUdf = udf(limitCharLength, StringType())

def limitCharLengthProd(value):
  if len(value) > 240:
    return value[0:230] + str('...')
  else:
    return value
limitCharLengthProdUdf = udf(limitCharLengthProd, StringType())

def limitCharLengthAgain(value):
  if (value is not None and value != 'null'):
    if len(value) > 80:
      return value[0:80] + str('...')
    else:
      return value
limitCharLengthAgainUdf = udf(limitCharLength, StringType())

def outputDF(dataFrame):

  output_DF = dataFrame.withColumn('run_dt', lit(dt_string))
  
  output_DF = output_DF \
    .withColumn('image', imageReplaceUdf('image')) \
    .withColumn("product_title",limitCharLengthProdUdf("product_title")) \
    .withColumn("product_title",translate_from_dictUdf("product_title")) \
    .withColumn("product_title",limitCharLengthAgainUdf("product_title")) \
    .withColumn('target_price', cast_decimal_udf('target_price')) \
    .withColumn('destination', destReplaceUdf('destination')) \
  
  return output_DF

# COMMAND ----------

convert_DF = outputDF(final_union_df)

display(convert_DF)

# COMMAND ----------

# MAGIC %md ## Pivoting Table

# COMMAND ----------

def renameColumns(renameDF):
  titleCount = 0
  imageCount = 0
  skuCount = 0
  destCount = 0
  destPrice = 0
  for column in renameDF.columns:
    if "product_title" in column:
      titleCount = titleCount + 1
      renameDF = renameDF.withColumnRenamed(column, "title" + str(titleCount))
    elif "image" in column:
      imageCount = imageCount + 1
      renameDF = renameDF.withColumnRenamed(column, "image" + str(imageCount))
    elif "destination" in column:
      destCount = destCount + 1
      renameDF = renameDF.withColumnRenamed(column, "destination" + str(destCount))
    elif "target_price" in column:
      destPrice = destPrice + 1
      renameDF = renameDF.withColumnRenamed(column, "target_price" + str(destPrice))
    elif "sku" in column:
      skuCount = skuCount + 1
      renameDF = renameDF.withColumnRenamed(column, "sku" + str(skuCount))
  return renameDF

#pivot table based on RANK values, creating the dynamic columns needed

eloqua1DF = convert_DF.orderBy(asc("email_address"), asc("sku_rank")) \
  .groupby(convert_DF.segment_name, convert_DF.email_address, convert_DF.advantage_sku_indicator, convert_DF.advantage_indicator, convert_DF.registered_user) \
  .pivot("sku_rank") \
  .agg(max("product_title"), max("image"), max("destination"), max("target_price"), max("sku")) \
  .withColumnRenamed('advantage_sku_indicator', 'advantage_s_indicator')

eloqua2 = renameColumns(eloqua1DF) #apply "renameColumn rules to eloqua_DF"

# COMMAND ----------

#display(eloqua2)

# COMMAND ----------

for col in eloqua2.columns:
   eloqua2 = eloqua2.withColumnRenamed(col, col.upper())
    
eloquaDF = eloqua2.withColumnRenamed('advantage_s_indicator','ADVANTAGE_SKU_INDICATOR')

# COMMAND ----------

#display(eloquaDF)

# COMMAND ----------

# MAGIC %md ## Changing Nulls to Blank Strings for Eloqua

# COMMAND ----------

def convertNull(value):
  if (value is None or value == 'null'):
    return str('')
  else:
    return value
conver_null_udf = udf(convertNull, StringType())

from pyspark.sql.functions import lit

for column in eloquaDF.columns:
  eloquaDF = eloquaDF.withColumn(column, conver_null_udf(column)).cache()

# COMMAND ----------

eloquaDF.cache()
eloquaDF.count() #17150

# COMMAND ----------

#display(eloquaDF)

# COMMAND ----------

convert_DF_58 = convert_DF.select('EMAIL_KEY', 'SEGMENT_NAME', 'CURRENT_DATE', 'SKU').where('SEGMENT_NAME = "OFR58"') #Inactive
convert_DF_59 = convert_DF.select('EMAIL_KEY', 'SEGMENT_NAME', 'CURRENT_DATE', 'SKU').where('SEGMENT_NAME = "OFR59"') #Active
convert_DF_60 = convert_DF.select('EMAIL_KEY', 'SEGMENT_NAME', 'CURRENT_DATE', 'SKU').where('SEGMENT_NAME = "OFR60"') #Active w/email opened

# COMMAND ----------

# MAGIC %md ## Eloqua New CDO

# COMMAND ----------

import requests
import base64
from pyspark.sql.functions import lit
import json

data = "ThermoFisherScientificCCG\Alok.mehta:" + dbutils.secrets.get('DACorpMktTier1_JDBC_Other_Scope','Eloqua_LPG_AlokMehta_PassWord')
api_url = "https://secure.eloqua.com/"

# Standard Base64 Encoding
encodedBytes = base64.b64encode(data.encode("utf-8"))
TOKEN = str(encodedBytes, "utf-8")

header = {
    "Content-Type": "application/json",
    "Authorization": "Basic %s" % TOKEN
  }

# COMMAND ----------

def getCustomObjectDetails():
  print("Get custom object details.")
  
  response = requests.get(
    api_url + '/api/REST/2.0/assets/customObjects',
    headers = header
  )
  
  if response.status_code == 200:
    jsonTX = response.json()
    elem = jsonTX['elements']
    customObj = [obj for obj in elem if(obj['name'] == "CORP_Product_Feed_Dynamic")]
    print (customObj)

  if customObj: 
    customObjId = customObj[0]['id']
    print ("Custom object id is: " + str(customObjId))
    
    return customObjId
  else:
    print(response.status_code)

# COMMAND ----------

customObjId = getCustomObjectDetails()

# COMMAND ----------

def genJsonForImportDef(fieldIds):
  print("Generate json for the import definition.")

  impFields = {}

  for row in fieldIds:
    impFields.update({
      row['name']: "{{CustomObject[" + customObjId + "].Field(" + row['internalName'] + ")}}"
    })

  impJsonStr = {
    "name": "Custom Object Import",
    "mapDataCards": "true",
    "mapDataCardsEntityField": "{{Contact.Field(C_EmailAddress)}}",
    "mapDataCardsSourceField": "EMAIL_ADDRESS",
    "mapDataCardsEntityType": "Contact",
    "mapDataCardsCaseSensitiveMatch": "false",
    "isSyncTriggeredOnImport": "true",
    "fields": impFields,
    "identifierFieldName": "EMAIL_ADDRESS"
  }
  impJsonObj = json.dumps(impJsonStr, sort_keys = True, indent = 2)

  print(impJsonObj)
  
  return impJsonStr

# COMMAND ----------

def getCustomObjectFields():
  print("Get custom object fields.")
  
  response = requests.get(
    api_url + "/api/bulk/2.0/customObjects/" + customObjId + "/fields",
    headers = header
  )
  
  if response.status_code == 200:
    fiels = response.json()['items']
    ids = []
    for field in fiels:
      if 'DataCard' not in field['name']:
        mapF = {}
        mapF.update( {'internalName' : field['internalName']} )
        mapF.update( {'name' : field['name']} )
        ids.append(mapF)

    print("Field IDs: " + str(ids))
    
    return ids

  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

fieldIds = getCustomObjectFields()

# COMMAND ----------

jsonImport = genJsonForImportDef(fieldIds)

# COMMAND ----------

def createImportDef(importJson):
  print("Create an import definition for a custom object")

  response = requests.post(
    api_url + '/api/bulk/2.0/customObjects/' + customObjId + '/imports',
    headers = header,
    json = importJson
  )

  if response.status_code == 201:
    print(response.json())

    importDef = response.json()['id']
    
    return importDef
  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

importDefId = createImportDef(jsonImport)

# COMMAND ----------

def getCustomObjectImportDef():
  print("Get custom object import definition.")
  
  response = requests.get(
    api_url + "/api/bulk/2.0/customObjects/" + customObjId + "/imports/",
    headers = header
  )

  if response.status_code == 200:
#     print(response.json())

    items = response.json()['items']
    for item in items:
      if 'Custom Object Import' in item['name']:
        importId = item['uri'].split("/", 4)[4]
    
    print('Import definition ID: ' + str(importId))
    return importId

  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

importDefId = getCustomObjectImportDef() #251168 THIS SHOULD MATCH ID IN ABOVE CMD importDefId = createImportDef(jsonImport)

# COMMAND ----------

def genJsonDataImport(dataFrame):
  print("Generate json for data import.")
  
  list = dataFrame.collect()

  recordsStr = []

  for row in list:
    insertFields = {}

    for col in dataFrame.columns:
      insertFields.update({
        col:str(row[col])
      })

    recordsStr.append(insertFields)

  json_obj = json.dumps(recordsStr, sort_keys = True, indent = 2)

#   print(json_obj)
  
  return recordsStr

# COMMAND ----------

def pushDataToCDO(recordsJson, importDefId):
  print("Push data to Eloqua CDO.")
  
  response = requests.post(
    api_url + '/api/bulk/2.0/customObjects/' + customObjId + '/imports/' + str(importDefId) + '/data',
    headers = header,
    json = recordsJson
  )

  if response.status_code == 201:
    print(response.json())

  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

window=Window.orderBy('SEGMENT_NAME')
length=eloquaDF.count()
chuckEloquaDF=eloquaDF.withColumn('row_num',row_number().over(window))

step = 10000
for i in range(1,length,step):
  chuckDF = chuckEloquaDF.filter((F.col('row_num') >= i) & (F.col('row_num') <= i+step-1)).drop('row_num')
  print("Chunk loaded: " + str(chuckDF.count()))

  jsonRecords = genJsonDataImport(chuckDF)
  pushDataToCDO(jsonRecords, importDefId)

# COMMAND ----------

# MAGIC %md ## Push to Campaign Staging

# COMMAND ----------

camp_stg_DF_58 = convert_DF_58.select('EMAIL_KEY', 'CURRENT_DATE').dropDuplicates() \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY_ID') \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg3')) \
.withColumn('OFR_NM', lit('Small_to_med_1')) \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('current_date()','RUNDATE') \

camp_stg_DF_58 = camp_stg_DF_58.select('EMAIL_KEY_ID', 'POST_VISID_HIGH', 'POST_VISID_LOW', 'CMP_NM', 'SEG_NM','OFR_NM','AAUSERID','RUNDATE')

# COMMAND ----------

camp_stg_DF_59 = convert_DF_59.select('EMAIL_KEY', 'CURRENT_DATE').dropDuplicates() \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY_ID') \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg2')) \
.withColumn('OFR_NM', lit('Small_to_med')) \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('current_date()','RUNDATE') \

camp_stg_DF_59 = camp_stg_DF_59.select('EMAIL_KEY_ID', 'POST_VISID_HIGH', 'POST_VISID_LOW', 'CMP_NM', 'SEG_NM','OFR_NM','AAUSERID','RUNDATE')

# COMMAND ----------

camp_stg_DF_60 = convert_DF_60.select('EMAIL_KEY', 'CURRENT_DATE').dropDuplicates() \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY_ID') \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg1')) \
.withColumn('OFR_NM', lit('Small_to_med_2')) \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('current_date()','RUNDATE') \

camp_stg_DF_60 = camp_stg_DF_60.select('EMAIL_KEY_ID', 'POST_VISID_HIGH', 'POST_VISID_LOW', 'CMP_NM', 'SEG_NM','OFR_NM','AAUSERID','RUNDATE')

# COMMAND ----------

camp_hist_DF_58 = convert_DF_58.select('EMAIL_KEY', 'SKU', 'current_date()').dropDuplicates() \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg3')) \
.withColumnRenamed('current_date()','RUNDATE') \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY') \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('SKU','PRODUCTCODE') \

camp_hist_DF_58 = camp_hist_DF_58.select('CMP_NM', 'SEG_NM', 'RUNDATE', 'POST_VISID_LOW', 'POST_VISID_HIGH','EMAIL_KEY','AAUSERID','PRODUCTCODE')

# COMMAND ----------

camp_hist_DF_59 = convert_DF_59.select('EMAIL_KEY', 'SKU','current_date()').dropDuplicates() \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg2')) \
.withColumnRenamed('current_date()','RUNDATE') \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY') \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('SKU','PRODUCTCODE') \

camp_hist_DF_59 = camp_hist_DF_59.select('CMP_NM', 'SEG_NM', 'RUNDATE', 'POST_VISID_LOW', 'POST_VISID_HIGH','EMAIL_KEY','AAUSERID','PRODUCTCODE')

# COMMAND ----------

camp_hist_DF_60 = convert_DF_60.select('EMAIL_KEY', 'SKU','current_date()').dropDuplicates() \
.withColumn('CMP_NM', lit('Small_to_Med')) \
.withColumn('SEG_NM', lit('Seg1')) \
.withColumnRenamed('current_date()','RUNDATE') \
.withColumn('POST_VISID_LOW', lit(None).cast(StringType())) \
.withColumn('POST_VISID_HIGH', lit(None).cast(StringType()) ) \
.withColumnRenamed('EMAIL_KEY','EMAIL_KEY') \
.withColumn('AAUSERID', lit(None).cast(StringType())) \
.withColumnRenamed('SKU','PRODUCTCODE') \

camp_hist_DF_60 = camp_hist_DF_60.select('CMP_NM', 'SEG_NM', 'RUNDATE', 'POST_VISID_LOW', 'POST_VISID_HIGH','EMAIL_KEY','AAUSERID','PRODUCTCODE')

# COMMAND ----------

camp_stg_DF_58.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.CAMPAIGN_STAGING", mode = "append",properties=conn_MKTPD_FS)
camp_stg_DF_59.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.CAMPAIGN_STAGING", mode = "append",properties=conn_MKTPD_FS)
camp_stg_DF_60.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.CAMPAIGN_STAGING", mode = "append",properties=conn_MKTPD_FS)

camp_hist_DF_58.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.FS_CNTCT_HIST", mode = "append",properties=conn_MKTPD_FS)
camp_hist_DF_59.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.FS_CNTCT_HIST", mode = "append",properties=conn_MKTPD_FS)
camp_hist_DF_60.write.jdbc(url=url_MKTPD_FS, table="FS_CRDB_USER.FS_CNTCT_HIST", mode = "append",properties=conn_MKTPD_FS)

# COMMAND ----------

# MAGIC %md ## Databricks to Eloqua CDO push

# COMMAND ----------

import requests
import base64
from pyspark.sql.functions import lit
import json

data = "ThermoFisherScientificCCG\Alok.mehta:" + dbutils.secrets.get('DACorpMktTier1_JDBC_Other_Scope','Eloqua_LPG_AlokMehta_PassWord')
api_url = "https://secure.eloqua.com/"

# Standard Base64 Encoding
encodedBytes = base64.b64encode(data.encode("utf-8"))
TOKEN = str(encodedBytes, "utf-8")

header = {
    "Content-Type": "application/json",
    "Authorization": "Basic %s" % TOKEN
  }

# COMMAND ----------

# MAGIC %md ##First Time Run End

# COMMAND ----------

def getCustomObjectDetails():
  print("Get custom object details.")
  
  response = requests.get(
    api_url + '/api/REST/2.0/assets/customObjects',
    headers = header
  )
  
  if response.status_code == 200:
    jsonTX = response.json()
    elem = jsonTX['elements']
    customObj = [obj for obj in elem if(obj['name'] == "CORP_Product_Feed_Dynamic")]
    print (customObj)
 
  if customObj: 
    customObjId = customObj[0]['id']
    print ("Custom object id is: " + str(customObjId))
    
    return customObjId
  else:
    print(response.status_code)

# COMMAND ----------

customObjId = getCustomObjectDetails()

# COMMAND ----------

def getCustomObjectFields():
  print("Get custom object fields.")
  
  response = requests.get(
    api_url + "/api/bulk/2.0/customObjects/" + customObjId + "/fields",
    headers = header
  )
  
  if response.status_code == 200:
#     print(response.json())

    fiels = response.json()['items']
    ids = []
    for field in fiels:
      if 'DataCard' not in field['name']:
        mapF = {}
        mapF.update( {'internalName' : field['internalName']} )
        mapF.update( {'name' : field['name']} )
        ids.append(mapF)

    print("Field IDs: " + str(ids))
    
    return ids

  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

fieldIds = getCustomObjectFields()

# COMMAND ----------

def getCustomObjectImportDef():
  print("Get custom object import definition.")
  
  response = requests.get(
    api_url + "/api/bulk/2.0/customObjects/" + customObjId + "/imports/",
    headers = header
  )
 
  if response.status_code == 200:
#     print(response.json())
 
    items = response.json()['items']
    for item in items:
      if 'Custom Object Import' in item['name']:
        importId = item['uri'].split("/", 4)[4]
    
    print('Import definition ID: ' + str(importId))
    return importId
 
  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

importDefId = getCustomObjectImportDef()

# COMMAND ----------

def genJsonDataImport(dataFrame):
  print("Generate json for data import.")
  
  list = dataFrame.collect()
 
  recordsStr = []
 
  for row in list:
    insertFields = {}
 
    for col in dataFrame.columns:
      insertFields.update({
        col:str(row[col])
      })
 
    recordsStr.append(insertFields)
 
  json_obj = json.dumps(recordsStr, sort_keys = True, indent = 2)
 
  print(json_obj)
  
  return recordsStr

# COMMAND ----------

jsonRecords = genJsonDataImport(eloquaDF)

# COMMAND ----------

def pushDataToCDO(recordsJson, importDefId):
  print("Push data to Eloqua CDO.")
  
  response = requests.post(
    api_url + '/api/bulk/2.0/customObjects/' + customObjId + '/imports/' + str(importDefId) + '/data',
    headers = header,
    json = recordsJson
  )
 
  if response.status_code == 201:
    print(response.json())
 
  else:
    print(response.status_code)
    print(response.json())

# COMMAND ----------

window=Window.orderBy('SEGMENT_NAME')
length=eloquaDF.count()
chuckEloquaDF=eloquaDF.withColumn('row_num',row_number().over(window))

step = 10000
for i in range(1,length,step):
  chuckDF = chuckEloquaDF.filter((F.col('row_num') >= i) & (F.col('row_num') <= i+step-1)).drop('row_num')
  print("Chunk loaded: " + str(chuckDF.count()))

  jsonRecords = genJsonDataImport(chuckDF)
  pushDataToCDO(jsonRecords, importDefId)
