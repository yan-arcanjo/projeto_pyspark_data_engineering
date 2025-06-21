from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
import json
import requests

def extract_data_from_api():

    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    input_path = f"/mnt/c/Users/arcan/Documents/airflow/data/1-stage/coingecko/coingecko.json"

    # Extraindo dados mais recentes
    logging.info(f"Iniciando extração dos dados -> {url} ...")

    response = requests.get(url)
    with open(input_path, "w", encoding="utf-8") as file:
        json.dump(response.json(), file, ensure_ascii=False, indent=4)


    logging.info(f"Dados extraído e salvos -> {input_path} ...")


# Definição dos argumentos padrão (default_args)
default_args = {
    'owner': 'yan_arcanjo',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instanciando a DAG
with DAG(
    'PIPELINE_COINGECKO',
    default_args=default_args,
    description="Utilizando Pyspark e Delta tables para carregar dados da API Coingecko",
    schedule_interval=None,  # Executa uma vez ao dia
    catchup=False,  # Para não executar DAGs antigas que não rodaram
    tags=['pyspark', 'delta'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Iniciando pipeline de dados..."',
    )

    extract_data = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api
    )

    run_bronze_script = BashOperator(
        task_id='run_bronze_script',
        bash_command='spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
            --master local[*] \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=src/jobs/log4j2.properties" \
            /mnt/c/Users/arcan/Documents/airflow/src/jobs/1-bronze/coingecko/coingecko.py coingecko id',
        )
    
    run_silver_script = BashOperator(
        task_id='run_silver_script',
        bash_command='spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
            --master local[*] \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=src/jobs/log4j2.properties" \
            /mnt/c/Users/arcan/Documents/airflow/src/jobs/2-silver/coingecko/coingecko.py coingecko',
        )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Finalizando pipeline de dados..."',
    )

    # Definindo a ordem das tarefas
    start >> extract_data >> run_bronze_script >> run_silver_script >> end
