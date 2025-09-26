import dlt
from pyspark.sql.functions import *


# ingest a customer data in incremental mode

################## cleaning customer data ##################

@dlt.table(
    name="customer_bronze_ingestion",
    comment="customer cleaning data"
)
@dlt.expect("valid_gender", "gender IS NOT NULL")
@dlt.expect_or_drop("name_not_null", "name IS NOT NULL")
@dlt.expect_or_drop("city_not_null", "city IS NOT NULL")
@dlt.expect_or_drop("join_date_not_null", "join_date IS NOT NULL")
@dlt.expect_or_drop("Status_not_null", "Status IS NOT NULL")
@dlt.expect_or_drop("phone_not_null", "phone_number IS NOT NULL")
@dlt.expect_or_drop("preferred_not_null", "preferred IS NOT NULL")
@dlt.expect_or_drop("occupation_not_null", "occupation IS NOT NULL")
@dlt.expect_or_drop("income_range_not_null", "income_range IS NOT NULL")
@dlt.expect_or_drop("risk_segment_not_null", "risk_segment IS NOT NULL")
def bronze_customer_ingestion():
   
    df = dlt.read_stream("landing_customer_increamental")

    df = df.withColumn("name", upper(col("name")))
    df = df.withColumn("email", lower(col("email")))
    df = df.withColumn("occupation", upper(col("occupation")))
    df = df.withColumn("income_range", upper(col("income_range")))
    df = df.withColumn("risk_segment", upper(col("risk_segment")))
    df = df.withColumn("preferred", upper(col("preferred")))

    df = df.withColumn(
        "gender",
        when(col("gender") == "M", lit("Male"))
        .when(col("gender") == "F", lit("Female"))
        .otherwise("unknown")
    )


   





    

    return df

# ingest a transaction data in incremental mode
##################### cleaning transcation account data  #################

@dlt.table(
    name = "account_bronze_ingestion",
    comment = "account data ingestion"
)
@dlt.expect_or_fail("valid_account_id","account_id IS NOT NULL" )
@dlt.expect_or_fail("valid_customer_id","customer_id IS NOT NULL" )
@dlt.expect_or_fail("valid_txn_id","txn_id IS NOT NULL" )
@dlt.expect_or_drop("valid_account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")


def account_bronze_ingestion ():
    df = dlt.read_stream("landing_account_increamental")
    df = df.withColumn("account_type",upper(col("account_type")))
    df = df.withColumn("txn_type", upper(col("txn_type")))
    df = df.withColumn("txn_channel",upper(col("txn_channel")))
    return df
    