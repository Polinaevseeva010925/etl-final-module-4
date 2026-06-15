#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import substring, to_timestamp

def build_schema():
    return StructType([
        StructField("application_id", StringType(), True),
        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True),
        ]), True),
        StructField("loan", StructType([
            StructField("amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True),
        ]), True),
        StructField("scoring", StructType([
            StructField("score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
        ]), True),
        StructField("documents", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("status", StringType(), True),
        ])), True),
        StructField("decision_status", StringType(), True),
        StructField("submitted_at", StringType(), True),
    ])

def main():
    if len(sys.argv) != 4:
        raise ValueError("Usage: batch_flatten.py <bootstrap_servers> <topic> <output_path>")

    bootstrap_servers = sys.argv[1]
    topic = sys.argv[2]
    output_path = sys.argv[3]

    spark = SparkSession.builder.appName("batch-kafka-flatten").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    schema = build_schema()

    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.scram.ScramLoginModule required '
                'username="user1" password="password1";') \
        .load()

    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    flattened = parsed.withColumn("doc", explode("documents")) \
        .select(
            col("application_id"),
            col("customer.customer_id").alias("customer_id"),
            col("customer.region").alias("region"),
            col("loan.amount").alias("loan_amount"),
            col("loan.term_months").alias("term_months"),
            col("scoring.score").alias("score"),
            col("scoring.risk_level").alias("risk_level"),
            col("doc.type").alias("document_type"),
            col("doc.status").alias("document_status"),
            col("decision_status"),
            to_timestamp(substring(col("submitted_at"), 1, 19), "yyyy-MM-dd'T'HH:mm:ss").alias("submitted_at")
        )

    flattened.write.mode("overwrite").format("parquet").save(output_path)
    print(f"Success! Wrote results to {output_path}")
    print("Amount of flattened: ", flattened.count())
    spark.stop()

if __name__ == "__main__":
    main()