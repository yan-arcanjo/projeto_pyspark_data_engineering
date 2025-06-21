from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, upper, when, trim
from pyspark.sql.window import Window
from delta import *
import sys
from datetime import datetime
import logging

process = sys.argv[1]
# process = "coingecko"

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Definição de variáveis temporais para controle de execução
date_time_exec = datetime.now()                                                # Timestamp da execução atual
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S")              # Formato para nomes de arquivo: 20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S")      # Formato para banco: 2024-05-01 13:05:11

input_path = f"/mnt/c/Users/arcan/Documents/airflow/data/2-bronze/{process}"
output_path = f"/mnt/c/Users/arcan/Documents/airflow/data/3-silver/{process}"
log_file_path = f"/mnt/c/Users/arcan/Documents/airflow/data/5-logs/2-silver/{process}"

logging.info("Iniciando criação da Spark Session")

spark = SparkSession.builder \
    .appName("Spark - Silver") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate() 

logging.info("Criação da Spark Session Finalizada")

# Estrutura do processo para logging
process_info = {
    "id_log_file_path":f'{log_file_path}_{data_hora_inicio_file}',
    "process_name": f"SILVER_LOG_{process}",
    "date_run" : f"{datetime.now().strftime('%Y-%m-%d')}",
    "start_time_script": data_hora_inicio_format_db,
    "end_time_script": '',
    "status": "STARTED",
    "msg": '',
    "input_path": input_path,
    "output_path": output_path,
    "records_inserted": '0',
    "records_updated": '0',
    "start_bronze_reading": '',
    "end_bronze_reading": '',
    "start_filter_data": '',
    "end_filter_data": '',
    "start_filter_new_data": "",
    "end_filter_new_data": "",
    "start_transform_strings": '',
    "end_transform_strings": '',
    "start_add_column_log": '',
    "end_add_column_log": '',
    "start_update_delta_silver": '',
    "end_update_delta_silver": '',
}

logging.info(f'Inicio do processamento para {process} as {data_hora_inicio_format_db}')

try:
    # Leitura da Delta table e guarda ordem de colunas
    process_info["start_bronze_reading"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Lendo delta bronze -> {input_path}...")
    df_bronze = DeltaTable.forPath(spark, input_path).toDF()
    initial_column_order = df_bronze.columns 
    process_info["end_bronze_reading"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Seleciona os registros mais recentes de cada hash chave
    process_info["start_filter_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info('Selecionando os registros mais recentes de cada hash chave')
    df_bronze = df_bronze.withColumn("dt_atualizacao", col("dt_atualizacao").cast("timestamp"))
    window_spec = Window.partitionBy("hash_chave").orderBy(col("dt_atualizacao").desc())
    df_bronze = df_bronze.withColumn("rank_hash_chave", row_number().over(window_spec)).filter(col("rank_hash_chave") == 1).drop("rank_hash_chave")
    process_info["end_filter_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Pega apenas as linhas que existem alguma alteração
    process_info["start_filter_new_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if DeltaTable.isDeltaTable(spark, output_path):
        logging.info('Filtrando os dados que necessitam ser atualizados ou inseridos')
        df_silver_original = spark.read.format("delta").load(output_path)
        df_silver_new = df_bronze.join(df_silver_original, df_bronze.hash_columns == df_silver_original.hash_columns, "left_anti")
    else:
        logging.info('A tabela silver não existe, será criada com todos os registros.')
        df_silver_new = df_bronze
    process_info["end_filter_new_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    process_info["start_transform_strings"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Identifica as colunas do tipo string e aplica a transformação
    string_cols = [field.name for field in df_silver_new.schema.fields if str(field.dataType) == "StringType()" and field.name not in ['hash_columns','hash_chave']]
    for col_name in string_cols:
        df_silver_new = df_silver_new.withColumn(col_name, trim(upper(when(col(col_name) == '', None).otherwise(col(col_name)))))

    process_info["end_transform_strings"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Adiciona coluna de ID de log
    process_info["start_add_column_log"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_silver_new = df_silver_new.drop('key_log_file_path','dt_atualizacao','dt_ingestao','nm_arquivo_origem')
    df_silver_new = df_silver_new.withColumns({
        "key_log_file_path": lit(f'{log_file_path}_{data_hora_inicio_file}'),
        "dt_atualizacao": lit(data_hora_inicio_format_db),
        "dt_ingestao": lit(datetime.now().strftime("%Y-%m-%d")),
        "nm_arquivo_origem": lit(input_path),
        })
    
    process_info["end_add_column_log"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    process_info["start_update_delta_silver"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if DeltaTable.isDeltaTable(spark, output_path):
        logging.info('Realizando Merge para atualizar silver')
        df_silver = DeltaTable.forPath(spark, output_path)
        # Atualiza dados da silver com merge
        df_silver.alias("silver")\
            .merge(df_silver_new.alias("bronze"), "silver.hash_chave = bronze.hash_chave") \
            .whenNotMatchedInsertAll()\
            .whenMatchedUpdateAll()\
            .execute()

    else:
        # Inserindo todos os registros com append, pois é uma tabela nova
        logging.info('Inserindo todos os registros pois é uma nova tabela')
        df_silver_new.write.format('delta').mode("append").option("mergeSchema", "true").save(output_path)

    process_info["end_update_delta_silver"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    process_info["status"] = 'SUCESSO'
    process_info["msg"] = str(f'EXECUTADO COM SUCESSO')
    logging.info('EXECUTADO COM SUCESSO')

except Exception as e:
    process_info["msg"] = str(f'MSG ERRO -> {e}')
    process_info["status"] = 'ERRO'
    logging.error(f"Erro na execução: {e}")
    raise

finally:
    process_info["end_time_script"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info('-' * 30)

    df_log = spark.read.json(spark.sparkContext.parallelize([process_info]))
    #ordena colunas
    df_log = df_log.select(
    "id_log_file_path",
    "process_name",
    "date_run",
    "start_time_script",
    "end_time_script",
    "status",
    "msg",
    "input_path",
    "output_path",
    "records_inserted",
    "records_updated",
    "start_bronze_reading",
    "end_bronze_reading",
    "start_filter_data",
    "end_filter_data",
    "start_filter_new_data",
    "end_filter_new_data",
    "start_transform_strings",
    "end_transform_strings",
    "start_add_column_log",
    "end_add_column_log",
    "start_update_delta_silver",
    "end_update_delta_silver"
    )
    df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(log_file_path)
    logging.info(f'Log Salvo -> {log_file_path}')
    logging.info('FIM DA EXECUCAO!')