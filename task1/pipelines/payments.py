import argparse
from pyspark.sql import SparkSession
from os import listdir
from os.path import isfile, join
import uuid

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, ArrayType
from pyspark.sql.functions import col, explode, sum, count, current_timestamp, lit, to_timestamp
from pyspark.sql.streaming import StreamingQueryListener

# Create a Spark session
spark = SparkSession.builder.appName("ReadJSONData") \
        .master("spark://spark-master:7077") \
        .getOrCreate()


# Create audit table
spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/data/delta/table_audit/`
        (
            tableName STRING,
            ingestionTimestamp TIMESTAMP,
            batchId STRING,
            noOfRecords LONG,
            comments STRING
        )
        USING delta
        PARTITIONED BY (ingestionTimestamp)
""")

# Specify checkpoint location and delta table path
checkpoint_location = "/data/checkpoints/"
delta_table_folder = "/data/delta/"

#expected payment schema 
payments_schema = StructType([
    StructField("paymentId", StringType(), False),
    StructField("installmentId", StringType(), False),
    StructField("paymentDate", DateType(), False),
    StructField("paymentValue", StringType(), False)
])


#expected originations schema
originations_schema = StructType([
    StructField("originationId", StringType(), False),
    StructField("clientId", StringType(), False),
    StructField("registerDate", DateType(), False),
    StructField("installments", ArrayType(
        StructType([
            StructField("installmentId", StringType(), True),
            StructField("dueDate", DateType(), True),  
            StructField("installmentValue", StringType(), True) 
        ])
    ), True)
])

def load_batch():
    originations = spark.read.json("/data/originations/*.json", schema=originations_schema)

    installments = transform_installments(originations)
    batch_data_to_delta(installments,"fact_installments", "dueDate")

    transformed_originations = transform_originations(installments)
    batch_data_to_delta(transformed_originations,"fact_originations", "registerDate")


def transform_payments(payments):
    return payments.select("paymentId",
                           "installmentId",
                           "paymentDate",
                            payments.paymentValue.cast(DecimalType(38, 8)).alias('paymentValue'),
                            current_timestamp().alias('ingestionTimestamp')
                        )

def transform_originations(originations):
    return originations.groupBy(
        "originationId", 
        "clientId", 
        "registerDate") \
        .agg(sum("installmentValue").alias("value")) \
        .withColumn("ingestionTimestamp", current_timestamp()
    )

def transform_installments(originations):
    originations = originations.withColumn("installment", explode(originations.installments))

    return originations.select(
        "originationId", 
        "clientId",
        "registerDate", 
        col("installment.installmentId").alias("installmentId"),
        col("installment.dueDate").alias("dueDate"),
        col("installment.installmentValue").cast(DecimalType(38, 8)).alias("installmentValue"),
        current_timestamp().alias('ingestionTimestamp')
    )


def batch_data_to_delta(data, table, partition_field=None):
    batch_guid = str(uuid.uuid4())
    data = data.withColumn("batchId", lit(batch_guid))
    data.show(10)
    if partition_field:
        data.write \
        .partitionBy(partition_field) \
        .format("delta") \
        .save(f"/data/delta/{table}")
    else:
        data.write \
        .format("delta") \
        .save(f"/data/delta/{table}")

    audit_data = data.groupBy("batchId","ingestionTimestamp") \
                    .agg(count("*").alias('noOfRecords')) \
                    .withColumn("tableName", lit(table))
    
    audit_data.show()
    
    audit_data.write \
        .partitionBy("ingestionTimestamp") \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(f"/data/delta/table_audit")

    # .save(f"/opt/bitnami/spark/delta-log/{table}")
    # .save(delta_table_folder + table)

# def stream_data_to_delta(data, table):
#     query = data.writeStream \
#     .outputMode("append") \
#     .format("delta") \
#     .option("checkpointLocation", checkpoint_location) \
#     .trigger(processingTime="10 seconds") \
#     .start(delta_table_folder + table)


def load_streaming():

    payments = spark.readStream \
        .schema(payments_schema) \
        .json("/data/payments/*.json")
    
    payments = transform_payments(payments)
    paymentsStream = stream_data_to_delta(payments, "fact_payments", "paymentDate", "append")

    originations = spark.readStream \
        .schema(originations_schema) \
        .json("/data/originations/*.json")
    
    installments = transform_installments(originations)
    installmentsSteam = stream_data_to_delta(installments,"fact_installments", "dueDate", "append")
    
    originations = transform_originations(installments) \
        .withColumn("registerTimestamp", to_timestamp(col("registerDate")))

    # originations = originations.withWatermark("registerTimestamp", "1 seconds")
    originationsStream = stream_data_to_delta(originations, "fact_originations", "registerDate", "complete")


    paymentsStream.awaitTermination()
    originationsStream.awaitTermination()
    installmentsSteam.awaitTermination()


def stream_data_to_delta(data, table, partition_field, mode):
    table_checkpoint_location = checkpoint_location + table
    table_delta_location = delta_table_folder + table


    if partition_field:
        return data.writeStream \
        .outputMode(mode) \
        .format("delta") \
        .partitionBy(partition_field) \
        .option("checkpointLocation", table_checkpoint_location) \
        .trigger(processingTime="10 seconds") \
        .queryName(table) \
        .start(table_delta_location)
    else:
        return data.writeStream \
        .outputMode(mode) \
        .format("delta") \
        .option("checkpointLocation", table_checkpoint_location) \
        .trigger(processingTime="10 seconds") \
        .queryName(table) \
        .start(table_delta_location)


    # payments.select(payments.paymentValue.cast(DecimalType(38, 8)).alias('paymentValue'))

# query = payments.writeStream \
#     .outputMode("append") \
#     .format("delta") \
#     .option("checkpointLocation", checkpoint_location) \
#     .trigger(processingTime="10 seconds") \
#     .start(delta_table_path)

# # # Show data from both folders
# # print("Data from payments:")
# # payments.show()

# # # origination = spark.read.json("/data/originations/*.json", schema=originations_schema)

# # origination = spark.readStream \
# #     .schema(originations_schema) \
# #     .json("/data/originations/*.json")

# # # originations.select()

# # print("Data from originations:")
# # origination = origination.select("originationId", "registerDate").distinct()
# # origination.show()

# # installment = origination.withColumn("installment", explode(origination.installments))


# # installment = installment.select(
# #     "originationId", 
# #     "clientId", 
# #     "installment.installmentId", 
# #     col("installment.installmentId").alias("installmentId"),
# #     col("installment.dueDate").alias("dueDate"),
# #     col("installment.installmentValue").alias("installmentValue")
# # )

# # # Show the resulting DataFrame
# # print("Data from installments:")
# # installment.show()

# query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Originations and Payments")
    parser.add_argument("--mode", type=str, required=True, choices=["batch", "streaming"],
                        help="Specify whether to run in batch or streaming mode")
    
    args = parser.parse_args()

    mode = args.mode

    if mode == "batch":
        print("batch")
        load_batch()
    elif mode == "streaming":
        print("streaming")
        load_streaming()