import boto3, json
from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

S3_URL_ONE = "s3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json"
S3_URL_FULL = "s3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_*.json"

# Create a Boto3 client for Secrets Manager
secrets_client = boto3.client('secretsmanager')

# Retrieve the secret value from Secrets Manager
secret_response = secrets_client.get_secret_value(SecretId='snowflake/capstone/login')
secret_value = secret_response['SecretString']

print("Snowflake login to:")
print(json.loads(secret_value))

all_packages = [
    "org.apache.hadoop:hadoop-aws:3.1.2",
    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3",
    "net.snowflake:snowflake-jdbc:3.13.3"
]

config = {
    "spark.jars.packages": ",".join(all_packages),
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    'spark.executor.extraJavaOptions': f'-Dmy.secret={secret_value}',
}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def read_data(url):    
    return spark.read.json(
        str(url)
    )

def unpack_structures(frame):
    unpack = {
        'coordinates': {'latitude': 'latitude', 'longitude': 'longitude'},
        'date': {'utc': 'time_utc'} #'local': 'time_local', 
    }

    for org, fields in unpack.items():
        for field, target in fields.items():
            frame = frame.withColumn(target, psf.col(f"{org}.{field}"))
        frame = frame.drop(org)
    return frame

def correct_types(frame):
    types = {
        'date': ['time_utc'] #'time_local'
    }

    for field in types['date']:
        frame = frame.withColumn(field, psf.col(field).cast(TimestampType()))

    return frame

def write_frame(frame):
    sv = json.loads(secret_value)
    sfOptions = {
        "sfURL": sv["URL"],
        "sfUser": sv["USER_NAME"],
        "sfPassword": sv["PASSWORD"],
        "sfDatabase": sv["DATABASE"],
        "sfSchema": "SAM@PXL",
        "sfWarehouse": sv["WAREHOUSE"],
        "parallelism": "64"
    }

    frame.write\
        .format("net.snowflake.spark.snowflake")\
        .options(**sfOptions)\
        .option("dbtable", "SAM")\
        .mode('overwrite')\
        .save()

if __name__ == "__main__":
    frame_raw = read_data(S3_URL_FULL)

    #frame_raw.printSchema()
    #frame_raw.show(10, truncate=False)

    frame_flat = unpack_structures(frame_raw)
    frame_clean = correct_types(frame_flat)
    
    frame_clean.printSchema()
    frame_clean.show(10)
    print(f"Got {frame_clean.count()} rows")

    write_frame(frame_clean)