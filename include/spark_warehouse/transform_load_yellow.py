#!/usr/bin/env python
# coding: utf-8
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

storage_connector = '/home/Bagas/pipeline_dev/lib/gcs-connector-hadoop3-2.2.5.jar'
google_credentials = '/home/Bagas/pipeline_dev/credentials/cred_file.json'

def transform_load_yellow(service,year):
    
    if service.lower() == 'yellow' :

        spark = SparkSession.builder \
            .appName("GCS_Connection_Test") \
            .config("spark.jars", storage_connector) \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials) \
            .getOrCreate()

        yellow_schema = types.StructType([
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

        for i in range(1,13) :
            month = f'{i:02d}'
            filename = f'{service}_tripdata_{year}-{month}.parquet'
            try:    
                df = spark.read.format("parquet").load(f"gs://project_raw_ingest/{service}/{year}/{filename}")

                for field in yellow_schema.fields :
                    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

                df = df.withColumn('taxi_type', F.lit('Yellow'))
                df = df.withColumnRenamed('tpep_pickup_datetime','pickup_datetime')
                df = df.withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

                df = df.withColumn('code_trip', F.xxhash64('vendorID','pickup_datetime','dropoff_datetime','passenger_count','trip_distance','PULocationID','DOLocationID','taxi_type'))

                df.write\
                        .format("parquet")\
                        .partitionBy(["VendorID"])\
                        .mode("overwrite")\
                        .save(f"gs://project_clean_data/{service}/{year}/{filename}")

                df.write\
                    .format("bigquery")\
                    .mode("append")\
                    .partitionBy('pickup_datetime') \
                    .option("temporaryGcsBucket", f"project_clean_data/{service}/{year}/{filename}")\
                    .option("database", "project_clean_data")\
                    .option("table", f"indigo-muse-452811-u7.project_dataset.staging_table_yellow")\
                    .option("createDisposition", "CREATE_IF_NEEDED")\
                    .save()
            except:
                break
