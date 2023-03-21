from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType

S3_URL = "s3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json"
# TODO get ALL data files (data_part_*.json)

def read_data(url):
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark.read.json(
        str(url)
    )

def unpack_structures(frame):
    unpack = {
        'coordinates': {'latitude': 'latitude', 'longitude': 'longitude'},
        'date': {'local': 'time_local', 'utc': 'time_utc'}
    }

    for org, fields in unpack.items():
        for field, target in fields.items():
            frame = frame.withColumn(target, psf.col(f"{org}.{field}"))
        frame = frame.drop(org)
    return frame

if __name__ == "__main__":
    frame_raw = read_data(S3_URL)
    frame_flat = unpack_structures(frame_raw)
    frame_types = correct_types(frame_flat)
    
    frame_types.printSchema()
    frame_types.show(10)
    
