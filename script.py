import requests
import pandas as pd
import datetime
import os

api_url = "https://covid19-brazil-api.now.sh/api/report/v1"

def carregar_dados(api_url: str):
    response = requests.get(api_url)
    
    if response.status_code == 200:
        try:
            data = response.json()
            df = pd.DataFrame(data["data"])
            df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y/%m/%d')
            print("Dados carregados com sucesso.")
            return df
        except requests.exceptions.JSONDecodeError:
            print("Erro ao decodificar a resposta JSON.")
            return None
    else:
        print(f"Erro {response.status_code}: {response.text}")
        return None

def separar_dados_por_estado(df: pd.DataFrame):
    estados_dfs = {}
    for estado in df['uf'].unique():
        estados_dfs[estado] = df[df['uf'] == estado].reset_index(drop=True)
    return estados_dfs

def calcular_taxa_mortalidade(df: pd.DataFrame):
    df['taxa_mortalidade'] = (df['deaths'] / df['cases']) * 100
    df['taxa_mortalidade'] = df['taxa_mortalidade'].round(2).astype(str) + "%"
    print("Taxa de mortalidade calculada com sucesso.")
    return df