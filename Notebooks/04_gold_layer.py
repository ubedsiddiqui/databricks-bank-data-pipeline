import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *
@dlt.table(
    name= "gold_cust_acc_transfromed",
    comment = "account and customer materalized view"
)

def gold_cust_acc_transfromed():
    cust = dlt.read("silver_customer_tranformation")
    acc = dlt.read("silver_account_tranformation")
    return cust.join(acc, on ="customer_id", how="inner")


 ################# aggregation on gold ##################

@dlt.table(
     name="gold_cust_acc_agg ",
     comment = "aggregated gold customer + account txn "
 )
def gold_cust_acc_agg():
    df = dlt.read("gold_cust_acc_transfromed")
    return df.groupBy(
        "customer_id", "name", "gender", "city", "status",
        "income_range", "risk_segment", "customer_age", "tenure_day"
        ).agg(
           countDistinct("account_id").alias("total_accounts"),
           count("*").alias("txn_count"),
           sum(when(col("txn_type") == "CREDIT", col("txn_amount")).otherwise(lit(0))).alias("total_credit"),
           sum(when(col("txn_type") == "DEBIT", col("txn_amount")).otherwise(lit(0))).alias("total_debit"),
           min("txn_date").alias("first_day"),
           max("txn_date").alias("last_day"),
           sum("txn_amount").alias("total_txn_amount"),
           countDistinct("txn_channel").alias("channels_used"))
    
                       















