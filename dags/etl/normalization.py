from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from minio import Minio
from minio.error import S3Error
from utils import *
from config import *
import time

# Sessão Spark
spark = get_spark_session("ETL Sales - Normalization Layer")

# Constantes
list_tables = ["customer","employee"]

# Ler os arquivos na camada Landing e salva na camada Normalization
def main():    
    
    # Listando backet Minio
    client = Minio(MINIO_URL.replace("http://",""), access_key=SPARK_MINIO_ACCESS_KEY, secret_key=SPARK_MINIO_SECRET_KEY,secure=False)

    objects = client.list_objects(BUCKET_BASE, prefix=LANDING_ZONE, recursive=True)
    
    for obj in objects:        
    
        file_name = obj.object_name.replace(f"{BUCKET_BASE}/","").replace(".csv","")
        
        df_csv = readCSV(spark, BUCKET_LANDING + obj.object_name)
        
        # Trata a flag genero (de F para Femenino) e (de M para Masculino)
        if list_tables.count(file_name):
            df_csv = df_csv.withColumn("gender", when(col("gender") == "F", "Masculino").otherwise("Feminino"))    
        
        saveParquet(df_csv, BUCKET_NORMALIZATION + file_name)
                   
        time.sleep(1)
    
    spark.stop()            
  
if __name__ == "__main__":
    main()