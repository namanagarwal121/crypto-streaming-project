import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    abs as spark_abs,
    when
)

# ----------------------------------------------------
# Paths
# ----------------------------------------------------

BASE_PATH = "/home/naman/crypto-streaming-project"

BRONZE_PATH = f"file://{BASE_PATH}/delta/bronze_crypto"
SILVER_PATH = f"file://{BASE_PATH}/delta/silver_crypto"
CHECKPOINT_PATH = f"file://{BASE_PATH}/checkpoints/silver_crypto"

# ----------------------------------------------------
# Spark Session
# ----------------------------------------------------

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoSilverLayer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ----------------------------------------------------
# Read Bronze Stream
# ----------------------------------------------------

def read_bronze_stream(spark):
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )

# ----------------------------------------------------
# Transformations (Streaming-Safe)
# ----------------------------------------------------

def transform(df):

    # 1️⃣ Convert event_time to timestamp
    df = df.withColumn("event_time", to_timestamp(col("event_time")))

    # 2️⃣ Data Quality Filters
    df = df.filter(
        (col("event_time").isNotNull()) &
        (col("bitcoin_usd").isNotNull()) &
        (col("ethereum_usd").isNotNull()) &
        (col("bitcoin_usd") > 0) &
        (col("ethereum_usd") > 0)
    )

    # 3️⃣ Watermark + Deduplication
    df = (
        df.withWatermark("event_time", "10 minutes")
          .dropDuplicates(["event_time", "bitcoin_usd", "ethereum_usd"])
    )

    # 4️⃣ Time Columns
    df = (
        df.withColumn("event_date", to_date(col("event_time")))
          .withColumn("event_hour", hour(col("event_time")))
    )

    # 5️⃣ Ratio Metric
    df = df.withColumn(
        "btc_eth_ratio",
        col("bitcoin_usd") / col("ethereum_usd")
    )

    # 6️⃣ Simple Volatility Classification (based on absolute price threshold)
    df = df.withColumn(
        "btc_volatility_flag",
        when(spark_abs(col("bitcoin_usd")) > 100000, "HIGH")  # example threshold
        .otherwise("NORMAL")
    )

    return df

# ----------------------------------------------------
# Write Silver Layer
# ----------------------------------------------------

def write_silver(df):
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .partitionBy("event_date")
        .start(SILVER_PATH)
    )

# ----------------------------------------------------
# Main
# ----------------------------------------------------

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = read_bronze_stream(spark)
    silver_df = transform(bronze_df)

    query = write_silver(silver_df)
    query.awaitTermination()

if __name__ == "__main__":
    main()
