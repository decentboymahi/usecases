
#login to SDK
 gcloud auth login
 
#create Bucket through SDK with dual location(region must same)
 gsutil mb -c standard --placement us-south1,us-east1 gs://prac_case

#acquire a path of file to copy from local (or) landing zone to Bucket
 "D:\GCP\usecases\usecase_2\Repurchase_Product_new.csv"
 
#copy data from local (or) landing zone to Bucket
 gsutil cp "D:\GCP\usecases\usecase_2\Repurchase_Product_new.csv" gs://prac_case/Repur_product.csv
  
#create datasets in Bigquery for raw/refine/presentation(raw=stage,refine=history,presentation=auth_views)
 bq mk -d prd_raw_ds (only making 1-dataset for practice purpose ) real time we need to make 3-datasets
 bq mk -d prd_refine_ds
 bq mk -d prd_presentation_ds
 
 #checking the dataset creation
 bq show prd_raw_ds
 bq show prd_refine_ds
 bq show prd_presentation_ds
 
 
#create silver table for raw data without schema or with schema(must give all string data type)
bq mk -t prd_raw_ds.prd_rt1 cid:string,upc:string,oid:string,dt:string,r_dt:string,prc:string,qty:string,amt:string,r_qty:string,q_amt:string,web_prod_id:string,GMM_DESC:string,PARENT_MDSE_DIVN_DESC:string,Last_Modifies_Time:string,Country_code:string,entity_code:string

#load data to bq in table from Bucket
bq load --source_format=CSV --skip_leading_rows=1 prd_raw_ds.prd_rt1 gs://prac_case/Repur_product.csv cid:string,upc:string,oid:string,dt:string,r_dt:string,prc:string,qty:string,amt:string,r_qty:string,q_amt:string,web_prod_id:string,GMM_DESC:string,PARENT_MDSE_DIVN_DESC:string,Last_Modifies_Time:string,Country_code:string,entity_code:string

#create table in refine dataset with schema with default data types
bq mk -t prd_refine_ds.prd_ret1 CUST_ID:int64,USR_COUNT:numeric,ORD_ID:numeric,PURCH_DATE:date,REC_DATE:date,PRICE_ITEM:float64,QTY:int64,CUST_AMT:float64,REC_QTY:int64,REC_AMT:float64,web_prod_id:int64,GMM_DESC:string,PARENT_MDSE_DIVN_DESC:string,Last_Modifies_Time:timestamp,Country_code:string,ENTITY_CODE:string

#Do the transformation and get data into refine table from raw table
 bq query --use_legacy_sql=false insert into prd_refine_ds.prd_ret1 select safe_cast(cid as int64) as CUST_ID,safe_cast(upc as numeric) as USR_COUNT,safe_cast(oid as numeric) as ORD_ID,safe_cast(case when dt like '%/%' then format_date('%Y-%m-%d',parse_date('%m/%d/%Y',dt)) when dt like '%-%' then format_date('%Y-%m-%d',parse_date('%m-%d-%Y',dt)) else null end as date) as PURCH_DATE,safe_cast(CASE WHEN r_dt LIKE '%/%' THEN FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', r_dt)) WHEN r_dt LIKE '%-%' THEN FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m-%d-%Y', r_dt)) else null END as date) as REC_DATE,safe_cast(prc as float64) as PRICE_ITEM,safe_cast(qty as int64) as QTY,safe_cast(amt as float64) as CUST_AMT,safe_cast(r_qty as int64) as REC_QTY,safe_cast(q_amt as float64) as REC_AMT,safe_cast(web_prod_id as int64) as web_prod_id,gmm_desc as GMM_DESC,parent_mdse_divn_desc as PARENT_MDSE_DIVN_DESC,TIMESTAMP(current_datetime()) as Last_Modifies_time,country_code as Country_code,entity_code as ENTITY_CODE from prd_raw_ds.prd_rt1

#create Audit dataset and table for daily count of prd (raw & refine) data
 bq mk -t prd_audit_ds.audit_raw_rt1 dataset_name:string,table_name:string,date_audit:date,total_record_count:int64,audited_by:string
 
 bq mk -t prd_audit_ds.audit_raw_rt1 dataset_name:string,table_name:string,date_audit:date,total_record_count:int64,audited_by:string
 
#insert audit data from (raw & refine)
 
 bq query --use_legacy_sql=false insert into prd_audit_ds.audit_raw_rt1 select 'prd_raw_ds' as dataset_name,'prd_rt1' as table_name,current_date() as date_audit,count(*) as total_record_count,'mahi-1421' as audited_by from prd_raw_ds.prd_rt1
 
 bq query --use_legacy_sql=false insert into prd_audit_ds.audit_refine_ret1 select 'prd_refine_ds' as dataset_name,'prd_ret1' as table_name,current_date() as date_audit,count(*) as total_record_count,'mahi-1421' as audited_by from prd_refine_ds.prd_ret1

