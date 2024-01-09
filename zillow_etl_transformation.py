from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("ZillowDataToS3") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("bathrooms", FloatType()),
    StructField("bedrooms", FloatType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("currency", StringType()),
    StructField("daysOnZillow", IntegerType()),
    StructField("homeStatus", StringType()),
    StructField("homeType", StringType()),
    StructField("imgSrc", StringType()),
    StructField("isFeatured", BooleanType()),
    StructField("isNonOwnerOccupied", BooleanType()),
    StructField("isPreforeclosureAuction", BooleanType()),
    StructField("isPremierBuilder", BooleanType()),
    StructField("isZillowOwned", BooleanType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("livingArea", FloatType()),
    StructField("lotAreaValue", FloatType()),
    StructField("price", FloatType()),
    StructField("zestimate", FloatType()),
    StructField("rentZestimate", FloatType()),
    StructField("streetAddress", StringType()),
    StructField("taxAssessedValue", FloatType()),
    StructField("zipcode", StringType()),
    StructField("zpid", StringType())
])

def zillow_transformed_data():
    try:
         # Get the current date for naming purposes
        current_date = date.today().strftime("%Y-%m-%d")

        # Define the S3 path for the input data
        s3_path = f"s3://zilow-data-transformed/Api_data/zillow_data_{current_date}.csv"

        # Read data from S3 with specified schema
        df = spark.read.option("header","true").schema(schema).csv(s3_path)

        # Drop unnecessary columns
        df=df.drop('taxAssessedValue','lotAreaValue','imgSrc','zpid','currency','daysOnZillow')

        # Handle missing values and transformations
        df = df.withColumn('bathrooms', when(col('bathrooms').isNull(), 0).otherwise(col('bathrooms')))
        df = df.withColumn('bedrooms', when(col('bedrooms').isNull(), 0).otherwise(col('bedrooms')))
        df = df.withColumn('livingArea', when(col('livingArea').isNull(), 0).otherwise(col('livingArea')))
        df = df.withColumn('zestimate', when(col('zestimate').isNull(), col('price')).otherwise(col('zestimate')))
        df = df.withColumn('rentZestimate', when(col('rentZestimate').isNull(), round(col('livingArea'))).otherwise(col('rentZestimate')))
        
        # Define the S3 path for the transformed data
        transformed_data_path=f"s3://zilow-data-transformed/transformed_data/zillow_transformed_data_{current_date}.csv"

        # Write the transformed data to S3
        df.coalesce(1).write.format('csv').option("header","true").mode("append").save(transformed_data_path)
    except Exception as e:
        print(f" Error: {str(e)}")

if __name__=="__main__":
    zillow_transformed_data()