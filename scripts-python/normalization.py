import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import glob
import time
import os

spark = SparkSession.builder.appName("etl_sales_normalization").getOrCreate()

# Constantes
list_tables = ["customer","employee"]
path_landing = "/data-lake/landing/"
path_normalization = "/data-lake/normalization/"
path_csv = path_landing + "*.csv"

# Funcoes para leitura e escrita de arquivos
def readCSV(path):
    df = (
        spark
        .read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("delimiter", ";")
        .load(path)
    )
    return df

def saveParquet(df, path):
    df.write.mode("overwrite").format("parquet").save(path)

# Ler os arquivos na camada Landing e salva na camada Normalization
def main():
    for file in glob.glob(path_csv):
        
        file_name = os.path.basename(file).replace(".csv","")
        df_csv = readCSV(file)
        
        # Trata a flag genero (de F para Femenino) e (de M para Masculino)
        if list_tables.count(file_name):
            df_csv = df_csv.withColumn("gender", when(col("gender") == "F", "Masculino").otherwise("Feminino"))    
        
        saveParquet(df_csv, path_normalization + file_name)
                   
        time.sleep(1)
    
    spark.stop()            
  
if __name__ == "__main__":
    main()