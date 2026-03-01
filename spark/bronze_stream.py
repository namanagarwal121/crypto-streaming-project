import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json


# ==========================================
# Configuration
# ==========================================

BASE_PATH = "/home/naman/crypto-streaming-project"

DELTA_PATH = f"file://{BASE_PATH}/delta/bronze_crypto"
CHECKPOINT_PATH = f"file://{BASE_PATH}/checkpoints/bronze_crypto"


# ==========================================
# Spark Session
# ==========================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoBronzeLayer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ==========================================
# Schema Definition
# ==========================================

def define_schema():
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("bitcoin_usd", DoubleType(), True),
        StructField("ethereum_usd", DoubleType(), True)
    ])


# ==========================================
# Read From Kafka
# ==========================================

def read_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "crypto_prices")
        .option("startingOffsets", "latest")
        .load()
    )


# ==========================================
# Parse JSON
# ==========================================

def parse_stream(kafka_df, schema):
    return (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), schema).alias("data"))
        .select("data.*")
    )


# ==========================================
# Write To Delta (Bronze Layer)
# ==========================================

def write_to_delta(df):
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(DELTA_PATH)
    )


# ==========================================
# Main
# ==========================================

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = define_schema()

    kafka_df = read_from_kafka(spark)
    parsed_df = parse_stream(kafka_df, schema)

    query = write_to_delta(parsed_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()
