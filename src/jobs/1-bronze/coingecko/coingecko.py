from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, sha2
from delta import *
from datetime import datetime
import os
import logging
import sys

process = sys.argv[1]
valid_columns = sys.argv[2].split(",")
# process = "coingecko"
# valid_columns = ["id"]

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Definição de variáveis temporais para controle de execução
date_time_exec = datetime.now()                                                # Timestamp da execução atual
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S")              # Formato para nomes de arquivo: 20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S")      # Formato para banco: 2024-05-01 13:05:11

input_path = f"/mnt/c/Users/arcan/Documents/airflow/data/1-stage/{process}/{process}.json"
output_path = f"/mnt/c/Users/arcan/Documents/airflow/data/2-bronze/{process}"
log_file_path = f"/mnt/c/Users/arcan/Documents/airflow/data/5-logs/1-bronze/{process}"

# Nome do arquivo renomeado
renamed_file = f"{process}_{data_hora_inicio_file}.json"
renamed_path = f"/mnt/c/Users/arcan/Documents/airflow/data/1-stage/{process}/{renamed_file}"

logging.info(f'Inicio do processamento para {process} as {data_hora_inicio_format_db}')

# Estrutura do processo para logging
process_info = {
    "id_log_file_path":f'{log_file_path}_{data_hora_inicio_file}',
    "process_name": f"BRONZE_LOG_{process}",
    "start_time_script": data_hora_inicio_format_db,
    "end_time_script": '', 
    "start_create_session": '',
    "end_create_session": '',
    "input_path": renamed_path,
    "output_path": output_path,
    "start_reading_file": '',
    "end_reading_file": '',
    "start_add_control_columns": '',
    "end_add_control_columns": '',
    "start_merge_delta": '',
    "end_merge_delta": '',
    "records_processed": '0',
    "status": 'STARTED',
    "msg_erro": ''
}

os.rename(input_path, renamed_path)

logging.info(f"Dados extraído e salvos -> {renamed_path} ...")

# Iniciando sessão do Spark
process_info["start_create_session"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

spark = SparkSession.builder \
    .appName("bronzeLayer") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

process_info["end_create_session"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Carregando arquivo json em um dataframe 
process_info["start_reading_file"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df = spark.read.option("multiline","true").json(renamed_path)
df = df.drop("roi")
df_columns = df.columns

process_info["end_reading_file"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


process_info["start_add_control_columns"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Inserindo colunas de controle
df_bronze = df.withColumns({
        "key_log_file_path": lit(f'{log_file_path}_{data_hora_inicio_file}'),
        "dt_atualizacao": lit(data_hora_inicio_format_db),
        "dt_ingestao": lit(datetime.now().strftime("%Y-%m-%d")),
        "nm_arquivo_origem": lit(renamed_path),
        "hash_chave": sha2(concat_ws("||", *[col(c) for c in valid_columns]), 256), 
        "hash_columns": sha2(concat_ws("||", *[col(c) for c in df_columns]), 256) 
    })

process_info["end_add_control_columns"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Iniciando o merge da tabela delta
try:
    if(DeltaTable.isDeltaTable(spark, output_path)):
        process_info["start_merge_delta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df_bronze_original = DeltaTable.forPath(spark, output_path)
        qtd_linhas_antes = df_bronze_original.toDF().count()

        df_bronze_original.alias("delta")\
            .merge(
                df_bronze.alias("stage"), 
                "delta.hash_columns = stage.hash_columns"   # Insere apenas dados novos
                ) \
            .whenNotMatchedInsertAll() \
            .execute()
        
        
        qtd_linhas_depois = df_bronze_original.toDF().count()

        process_info["records_processed"] = str(qtd_linhas_depois - qtd_linhas_antes)
    else:
        df_bronze.write.format("delta").option("mergeSchema", "true").mode("append").save(output_path)
        process_info["records_processed"] = str(df.count())

    process_info["end_merge_delta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    process_info["status"] = 'SUCESSO'


except FileExistsError as fe:
    process_info["msg_erro"] = str(fe)
    process_info["status"] = 'IGNORADO'
    logging.warning(f"Processo abortado: {fe}")

except Exception as e:
    process_info["msg_erro"] = str(e)
    process_info["status"] = 'ERRO'
    logging.error(f"Erro na execução: {e}")

finally:
    process_info["end_time_script"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info('-' * 30)
    # logging.info('Resumo da execução:')
    # for key, value in process_info.items():
    #     logging.info(f' - {key}: {value}')
    df_log = spark.read.json(spark.sparkContext.parallelize([process_info]))
    #ordena colunas
    df_log = df_log.select(
    "id_log_file_path",
    "process_name",
    "start_time_script",
    "end_time_script", 
    "start_create_session",
    "end_create_session",
    "input_path",
    "output_path",
    "start_reading_file",
    "end_reading_file",
    "start_add_control_columns",
    "end_add_control_columns",
    "start_merge_delta",
    "end_merge_delta",
    "records_processed",
    "status",
    "msg_erro"
    )
    df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(log_file_path)
    logging.info(f'Log Salvo -> {log_file_path}')
    logging.info('FIM DA EXECUCAO!')