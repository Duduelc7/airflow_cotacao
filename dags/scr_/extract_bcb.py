import pandas as pd
from bcb import PTAX
from datetime import datetime

def extract_cotacao_moedas():
    date = datetime.now()
    date_now = date.strftime('%m/%d/%Y').lstrip('0').replace('/0','/')
    ptax = PTAX()
    df_moedas = pd.DataFrame(ptax.get_endpoint('Moedas').query().collect())
    lista_cotacao = []
    for i, r in df_moedas.iterrows():
        df_cotacao_l = pd.DataFrame(ptax.get_endpoint('CotacaoMoedaDia').query().parameters(moeda =r['simbolo'] ,dataCotacao = date_now).collect())
        df_cotacao_l['moeda'] = r['simbolo']
        df_cotacao_l['desc_moeda'] = r['nomeFormatado']
        lista_cotacao.append(df_cotacao_l)

    
    df_cotacao = pd.concat(lista_cotacao, ignore_index=True)
    
    return df_cotacao


