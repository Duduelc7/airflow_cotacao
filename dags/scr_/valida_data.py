from datetime import datetime
import sys


def valida_data(xcom):
    df = xcom.xcom_pull(task_ids = 'extract_dados')
    data_atual = datetime.now().strftime('%Y-%m-%d') 
    df['dataHoraCotacao'] = df['dataHoraCotacao'].astype(str)
    if df['dataHoraCotacao'].str.startswith(data_atual).any():
        return 'valida'
    else:
        return 'nvalida'
    
