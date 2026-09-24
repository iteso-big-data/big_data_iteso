import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="S3 path to the input CSV")
    args = parser.parse_args()

    # Create Spark session
    spark = (SparkSession.builder
             .appName("Batch processing")
             .getOrCreate())

    # Create the schema

    aggregated_trips_schema = StructType([
        StructField("date", DateType(), True),
        StructField("hour", StringType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("PU_Borough", StringType(), True),
        StructField("DO_Borough", StringType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_count", IntegerType(), True),
        StructField("trip_distance_sum", DoubleType(), True),
        StructField("duration_sum", DoubleType(), True),
        StructField("fare_amount_sum", DoubleType(), True),
        StructField("extra_sum", DoubleType(), True),
        StructField("mta_tax_sum", DoubleType(), True),
        StructField("tip_amount_sum", DoubleType(), True),
        StructField("tolls_amount_sum", DoubleType(), True),
        StructField("improvement_surcharge_sum", DoubleType(), True),
        StructField("congestion_surcharge_sum", DoubleType(), True),
        StructField("airport_fee_sum", DoubleType(), True),
        StructField("total_amount_sum", DoubleType(), True),
    ])


    df_nyc_taxi = (
        spark.read
        .schema(aggregated_trips_schema)
        .option("header", "true")
        .csv(args.input_path)
    )
    df_nyc_taxi.printSchema()
    df_nyc_taxi.show(2)

    n_records = df_nyc_taxi.count()
    print(f"number of records in the taxi dataset: {n_records}")

    enriched_df = (
        df_nyc_taxi
        .withColumn(
            "borough_match",
            when(col("PU_Borough") == col("DO_Borough"), lit("Same Borough"))
            .otherwise(lit("Cross Borough"))
        )
        .select(
            col("date"),
            col("hour"),
            col("PU_Borough"),
            col("DO_Borough"),
            col("borough_match"),
            col("trip_count"),
            col("total_amount_sum")
        )
    )
    enriched_df.show(10)
    

if __name__ == "__main__":
    main()