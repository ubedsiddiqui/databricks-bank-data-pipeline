# ingestion customer data 2023 

import dlt
from pyspark.sql import *
from pyspark.sql.types import *

Customer_schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("dob",DateType(),True),
    StructField("gender",StringType(),True),
    StructField("city",StringType(),True),
    StructField("join_date",DateType(),True),
    StructField("Status",StringType(),True),
    StructField("email",StringType(),True),
    StructField("Phone_number",StringType(),True),
    StructField("preferred",StringType(),True),
    StructField("occupation",StringType(),True),
    StructField("income_range",StringType(),True),
    StructField("risk_segment",StringType(),True)


])

@dlt.table(
    name = "landing_customer_increamental",
    comment = 'landing customer data'

)

def customer_incremental():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.includeExistingFiles", "true") 
        .option("header","true")  
        .schema(Customer_schema) 
        .load("/Volumes/dlt_bank_project_catlog/dlt_bank__schema/dlt_bank_volume/customer/")

    )


# ingestion for transcation_account 

account_schema = StructType([
    StructField("account_id",IntegerType(),True),
    StructField("Customer_id",IntegerType(),True),
    StructField("account_type",StringType(),True),
    StructField("balance",FloatType(),True),
    StructField("txn_id",IntegerType(),True),
    StructField("txn_date",DateType(),True),
    StructField("txn_type",StringType(),True),
    StructField("txn_amount",DoubleType(),True),
    StructField("txn_channel",StringType(),True),
    


])



@dlt.table(
    name = "landing_account_increamental",
    comment = 'landing account data'

)

def account_incremental():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.includeExistingFiles", "true") 
        .option("header","true")  
        .schema(account_schema) 
        .load("/Volumes/dlt_bank_project_catlog/dlt_bank__schema/dlt_bank_volume/accounts/")

    )