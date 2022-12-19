from pyspark.sql import SparkSession
from config import *

def get_spark_session(appName):

    spark = (SparkSession.builder
            .appName(appName)
		    .config("spark.hadoop.fs.s3a.access.key", SPARK_MINIO_ACCESS_KEY)
		    .config("spark.hadoop.fs.s3a.secret.key", SPARK_MINIO_SECRET_KEY)
		    .config("spark.hadoop.fs.s3a.path.style.access", "true")
		    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
		    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
		    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
              .getOrCreate())
    return spark

# Funcoes para leitura e escrita de arquivos
def readCSV(spark, file):
    df = (
        spark
        .read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("delimiter", ";")
        .load(file)
    )

    return df

def saveParquet(df, path):
    df.write.mode("overwrite").format("parquet").save(path)

def createTempView(spark, path, table_name):
    df = spark.read.parquet(path)
    df.createOrReplaceTempView(table_name)

def sql(query, spark):
    return spark.sql(query)    
    	