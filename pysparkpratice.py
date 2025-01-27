from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Hello Spark")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("RDD to DF") \
        .master("local") \
        .getOrCreate()

    # Set Spark log level
    spark.sparkContext.setLogLevel("ERROR")

    # Sample data
    list_data_sample = [
        ("Midhun", "IT", "CTS"),
        ("Kiran", "IT", "HCL"),
        ("Kishore", "HR", "TCS")
    ]

    # Create DataFrame
    df_creation = spark.createDataFrame(list_data_sample).toDF("Name", "Stream", "Company")

    # Show DataFrame
    df_creation.show()
