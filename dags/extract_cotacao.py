from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from scr_.extract_bcb import extract_cotacao_moedas

def valida_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_dados')
    data_atual = datetime.now().strftime('%Y-%m-%d') 
    data_teste = '2023-01-23'
    df['dataHoraCotacao'] = df['dataHoraCotacao'].astype(str)
    if df['dataHoraCotacao'].str.startswith(data_atual).any():
        return 'valida_data_bash'
    else:
        return 'nvalida_data_bash'
    
def dispara_erro():
    raise ValueError("Data inválida detectada. Interrompendo o workflow.")

with DAG(dag_id='extract_cotacao',start_date=datetime(2024,8,21),
          schedule_interval='30 * * * * ',catchup=False)  as dag:

    extract_dados = PythonOperator(
        task_id='extract_dados',
        python_callable=extract_cotacao_moedas
    )

    script_valida = BranchPythonOperator(
        task_id='script_valida',
        python_callable=valida_data,
        provide_context=True  
    )

    valida_data_bash = BashOperator(
        task_id='valida_data_bash',
        bash_command="echo 'Data ok'"
    )
    nvalida_data_bash = PythonOperator(
        task_id='nvalida_data_bash',
        python_callable=dispara_erro
    )

    teste = PythonOperator(
        task_id='teste',
        python_callable=lambda: print("Tarefa de teste")
    )








    extract_dados >> script_valida >> [valida_data_bash, nvalida_data_bash]

    valida_data_bash >> teste
