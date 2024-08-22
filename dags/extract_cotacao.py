from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from scr_.extract_bcb import extract_cotacao_moedas
from scr_.valida_data import valida_data



with DAG(dag_id='extract_cotacao',start_date=datetime(2024,8,21),
          schedule_interval='30 * * * * ',catchup=False)  as dag:

    extract_dados = PythonOperator(
        task_id = 'extract_dados',
        python_callable= extract_cotacao_moedas
    )

    script_valida = BranchPythonOperator(
        task_id = 'script_valida',
        python_callable=valida_data,
        provide_context = True
    )

    valida_data_bash = BashOperator(
        task_id = 'valida_data_bash',
        bash_command="echo 'Data ok'"
    )

    extract_dados >> script_valida >> valida_data_bash