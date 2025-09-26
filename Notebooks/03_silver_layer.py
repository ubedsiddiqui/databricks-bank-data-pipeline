import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
@dlt.table( 
    name = "silver_customer_tranformation",
    comment = "customer data transformation"

)

def silver_customer_tranformation():   
    df =spark.readStream.table("customer_bronze_ingestion")
    df = df.withColumn("customer_age",when(col("dob").isNotNull(),floor(months_between(current_timestamp(),col("dob"))/12 )).otherwise(lit(None)))
    df = df.withColumn("tenure_day",when(col("join_date").isNotNull(),datediff(current_date(), col("join_date"))).otherwise(lit(None)))
    df = df.withColumn("age_outof_range_flag", (col("dob") <lit("1900-01-01"))| (col("dob")> current_date()))
    df = df.withColumn("transformed_date",current_timestamp())

    return df

############ customer silver scd 1 ###########
dlt.create_streaming_table(
    name = "silver_customer_scd1"
)

# for SCD 2 
#dlt.create_auto_cdc_flow(
   # target="silver_customer_scd1",
   # source="silver_customer_tranformation",
   # keys=["customer_id"],
   # sequence_by=col("transformed_date"),
   # stored_as_scd_type=1,
   # except_column_list=["transformed_date"]
#)



dlt.apply_changes(
    source='silver_customer_tranformation',
    target='silver_customer_scd1',
    keys=['customer_id'],
    sequence_by= col('transformed_date'),
    stored_as_scd_type=1,
    except_column_list=['transformed_date']
)

############ creating view ##############
@dlt.view(
    name = "silver_customer_transformation_view",
    comment = "customer silver transformation view"
)

def silver_customer_transformation_view():
    df = spark.readStream.table("silver_customer_tranformation")
    return df










##### transformed account data ####

@dlt.table(
    name = "silver_account_tranformation",
    comment = "transformed account data"
)

def silver_account_transformation():
    df = spark.readStream.table("account_bronze_ingestion")
    df = df.withColumn("channel_type",when((col("txn_channel") == "ATM") | (col("txn_channel") == "BRANCH"),lit("PHYSICAL")).otherwise(lit("DIGITAL")))

    df = df.withColumn("txn_year",year(col("txn_date"))).withColumn("txn_month", month(col("txn_date"))).withColumn("txn_days", dayofmonth(col("txn_date")))
    df = df.withColumn("acc_transformation_date",current_timestamp())

    return df

################ transformed customer data scd 1 ##############

dlt.create_streaming_table(
    name = "silver_account_scd2"
)

dlt.create_auto_cdc_flow(
    target="silver_account_scd2",
    source="silver_account_tranformation",
    keys=["txn_id"],
    sequence_by=col("acc_transformation_date"),
    stored_as_scd_type=2,
    except_column_list=["acc_transformation_date"]
)

############## creating view of account ###########

@dlt.view(
    name = "silver_account_transformation_view",
    comment = "account silver transformation view"
)

def silver_account_transformation_view():
    df = spark.readStream.table("silver_account_tranformation")
    return df


