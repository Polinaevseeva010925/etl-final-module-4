#!/usr/bin/env python3
import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_json, struct, lit, concat, current_timestamp,
    udf, array
)
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

def main():
    if len(sys.argv) != 4:
        raise ValueError("Usage: generate_data.py <bootstrap_servers> <topic> <num_messages>")

    bootstrap_servers = sys.argv[1]
    topic = sys.argv[2]
    num_messages = int(sys.argv[3])

    spark = SparkSession.builder.appName("KafkaDataGenerator").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    regions = ["DE-HE", "DE-BW", "RU-MOW", "NL-NH"]
    risk_levels = ["low", "medium", "high"]
    decision_statuses = ["approved", "rejected", "manual_review"]
    loan_amounts = [5000, 10000, 15000, 25000]
    term_months_list = [12, 24, 36]

    @udf(returnType=StringType())
    def random_region():
        return random.choice(regions)

    @udf(returnType=StringType())
    def random_risk():
        return random.choice(risk_levels)

    @udf(returnType=StringType())
    def random_decision():
        return random.choice(decision_statuses)

    @udf(returnType=IntegerType())
    def random_loan_amount():
        return random.choice(loan_amounts)

    @udf(returnType=IntegerType())
    def random_term():
        return random.choice(term_months_list)

    @udf(returnType=IntegerType())
    def random_customer_id():
        return random.randint(100, 999)

    @udf(returnType=IntegerType())
    def random_score():
        return random.randint(300, 850)

    df = spark.range(num_messages)

    json_df = df.withColumn("i", col("id")) \
        .withColumn("application_id", concat(lit("loan_"), col("i") + 700000)) \
        .withColumn("customer_id", concat(lit("cust_"), random_customer_id())) \
        .withColumn("region", random_region()) \
        .withColumn("loan_amount", random_loan_amount()) \
        .withColumn("term_months", random_term()) \
        .withColumn("score", random_score()) \
        .withColumn("risk_level", random_risk()) \
        .withColumn("decision_status", random_decision()) \
        .withColumn("submitted_at", current_timestamp()) \
        .select(
            to_json(
                struct(
                    col("application_id"),
                    struct(
                        col("customer_id"),
                        col("region")
                    ).alias("customer"),
                    struct(
                        col("loan_amount").alias("amount"),
                        col("term_months")
                    ).alias("loan"),
                    struct(
                        col("score"),
                        col("risk_level")
                    ).alias("scoring"),
                    array(
                        struct(
                            lit("passport").alias("type"),
                            lit("verified").alias("status")
                        )
                    ).alias("documents"),
                    col("decision_status"),
                    col("submitted_at")
                )
            ).alias("value")
        )

    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.scram.ScramLoginModule required '
                'username="user1" password="password1";') \
        .save()

    print(f"Sent {num_messages} messages to topic {topic}")
    spark.stop()

if __name__ == "__main__":
    main()