#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

def transform_load_yellow(service, year):
    
    # conf = SparkConf() \
    #     .setAppName('yellow_transform') \
    #     .set("spark.jars", "./include/lib/gcs-connector-hadoop3-2.2.5.jar") \
    #     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    #     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", './include/credentials/cred_file.json')

    # sc = SparkContext(conf=conf.set("spark.files.overwrite", "true"))

    # hadoop_conf = sc._jsc.hadoopConfiguration()

    # hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    # hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    # hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", './include/credentials/cred_file.json')
    # hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    spark = SparkSession.builder \
        .appName("NYC Taxi ETL Yellow") \
        .config("spark.jars", "/usr/local/airflow/include/lib/gcs-connector-3.0.6-shaded.jar") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/include/credentials/cred_file.json") \
        .config("spark.hadoop.fs.gs.project.id", "indigo-muse-452811-u7") \
        .config("spark.executor.extraJavaOptions", "-Dcom.google.cloud.hadoop.util.AsyncWriteChannelOptions.shouldFlush=false") \
        .config("spark.driver.extraJavaOptions", "-Dcom.google.cloud.hadoop.util.AsyncWriteChannelOptions.shouldFlush=false") \
        .getOrCreate()
                
    # spark.sparkContext.setLogLevel("DEBUG")
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("transform_load_yellow")

    schema = types.StructType([
                types.StructField('VendorID', types.LongType()  ,True),
                types.StructField('tpep_pickup_datetime', types.TimestampType()  ,True),
                types.StructField('tpep_dropoff_datetime', types.TimestampType()  ,True),
                types.StructField('passenger_count', types.IntegerType()  ,True),
                types.StructField('trip_distance', types.DoubleType()  ,True),
                types.StructField('RatecodeID', types.IntegerType()  ,True),
                types.StructField('store_and_fwd_flag', types.StringType()  ,True),
                types.StructField('PULocationID', types.IntegerType()  ,True),
                types.StructField('DOLocationID', types.IntegerType()  ,True),
                types.StructField('payment_type', types.IntegerType()  ,True),
                types.StructField('fare_amount', types.DoubleType()  ,True),
                types.StructField('extra', types.DoubleType()  ,True),
                types.StructField('mta_tax', types.DoubleType()  ,True),
                types.StructField('tip_amount', types.DoubleType()  ,True),
                types.StructField('tolls_amount', types.DoubleType()  ,True),
                types.StructField('improvement_surcharge', types.DoubleType()  ,True),
                types.StructField('total_amount', types.DoubleType()  ,True),
                types.StructField('congestion_surcharge', types.DoubleType()  ,True),
                types.StructField('airport_fee', types.DoubleType()  ,True)
        ])
    
    processed_files = 0

    for month in range(1, 13):
        month_str = f"{month:02d}"
        filename = f"{service}_tripdata_{year}-{month_str}.parquet"
        input_path = f"gs://project_raw_ingest/{service}/{year}/{filename}"
        
        try:
            logger.info(f"Loading file: {input_path}")
            df = spark.read.format("parquet").load(input_path)
            logger.info(f"Finish Loading file: {input_path}")

            for field in schema.fields:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

            df = df.withColumn('taxi_type', F.lit('yellow'))
            df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')
            df = df.withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
            df = df.withColumn('code_trip', F.xxhash64(
                'VendorID', 'pickup_datetime', 'dropoff_datetime',
                'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID', 'taxi_type'
            ))

            # Write to GCS
            # df.write.format("parquet")\
            #     .partitionBy(["VendorID"])\
            #     .mode("append")\
            #     .save(output_path)
            df = df.coalesce(10)

            # Write to BigQuery
            df.write.format("bigquery")\
                .mode("append")\
                .option("parentProject", "indigo-muse-452811-u7")\
                .option("temporaryGcsBucket", "project_clean_data" )\
                .option("createDisposition", "CREATE_IF_NEEDED")\
                .save("indigo-muse-452811-u7.project_dataset.staging_table_yellow")

            processed_files += 1
            logger.info(f"Successfully processed {filename}")

        except Exception as e:
            logger.error(f"Failed processing {filename}: {str(e)}")
            continue

    logger.info(f"Processing complete. Successfully processed {processed_files} files.")
    spark.stop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--service', type=str, required=True)
    args = parser.parse_args()

    if args.service.lower() != 'yellow':
        raise ValueError("Currently, only 'yellow' service is supported.")

    transform_load_yellow(args.service, args.year)

if __name__ == "__main__":
    main()
