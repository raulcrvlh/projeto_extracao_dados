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

def consultar_estados(estados_dfs: dict):
    estados_disponiveis = ', '.join(estados_dfs.keys())
    print(f"Estados disponíveis: {estados_disponiveis}")
    
    estados_selecionados = input("Digite as siglas dos estados que deseja consultar, separados por vírgula (ex: SP,RJ): ").strip().upper()
    estados_selecionados = [estado.strip() for estado in estados_selecionados.split(',')]
    
    df_filtrado = pd.DataFrame()
    for estado in estados_selecionados:
        if estado in estados_dfs:
            print(f"\nDados do estado {estado}:")
            print(estados_dfs[estado])
            df_filtrado = pd.concat([df_filtrado, estados_dfs[estado]])
        else:
            print(f"Estado {estado} não encontrado.")
    
    return df_filtrado

def salvar_dados(df: pd.DataFrame, nome_arquivo: str, formato: str):
    if not nome_arquivo.endswith(f".{formato}"):
        nome_arquivo += f".{formato}"

    caminho_completo = os.path.join("data", nome_arquivo)
    os.makedirs("data", exist_ok=True)
    
    if formato == "csv":
        df.to_csv(caminho_completo, index=False)
    elif formato == "parquet":
        df.to_parquet(caminho_completo, engine="fastparquet", index=False)
    
    print(f"Dados salvos com sucesso no arquivo '{caminho_completo}'.")