import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CategoryTransformation").getOrCreate()

def extract_json_from_s3():
    s3 = boto3.client('s3')
    bucket_name = 'bhavika-etl-youtube'
    object_key = 'Raw_json/category_data.json'
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    return json_data

def create_dataframe(data):
    schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("assignable", BooleanType(), True),
    StructField("etag", StringType(), True)
    ])

    arrdata = []

    for item in data['items']:
        tempdict = {
            "id": item['id'],
            "title": item['snippet']['title'],
            "assignable": item['snippet']['assignable'],
            "etag": item['etag']
        }
        arrdata.append(tempdict)

    category_df = spark.createDataFrame(arrdata, schema)
    category_df = category_df.withColumn("ingestion_date", F.current_timestamp())
    return category_df

def upload_csv_to_s3(category_df):
    s3_frame = category_df.toPandas()
    bucket_name = 'bhavika-etl-youtube'
    object_key = 'Transformed_CSV/category_data.csv'
    csv_data = s3_frame.to_csv(index=False)
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=object_key)
    print(f"CSV data uploaded to s3://{bucket_name}/{object_key}")

data = extract_json_from_s3()
if data:
    category_df = create_dataframe(data)
    upload_csv_to_s3(category_df)
else:
    print("No data fetched from S3.")


