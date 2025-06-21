from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
import sys

process = sys.argv[1]
# process = "tabela_airflow"

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("Iniciando criação da SparkSession.")
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate() 

spark.sparkContext.setLogLevel("ERROR")

data = [
        ("Yan", 24, "2025-06-14"),
        ("Eduarda", 22, "2025-06-14")
     ] 

# Esquema (schema) do DataFrame
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("casamento", StringType(), True),
])

df = spark.createDataFrame(data = data, schema=schema)

df.write.format("delta").mode("overwrite").save(f"/mnt/c/Users/arcan/Documents/airflow/data/2-bronze/{process}")
