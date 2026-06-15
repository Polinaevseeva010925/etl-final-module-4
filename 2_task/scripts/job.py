import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, current_timestamp, lit

def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("applications-processing")
        .getOrCreate()
    )

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    cleaned = (
        df
        .dropDuplicates(["application_id"])
        .withColumn("application_id", trim(col("application_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("risk_level", trim(col("risk_level")))
    )

    cleaned.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    main()