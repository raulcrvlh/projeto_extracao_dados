import pandas as pd
import requests
import numpy as np
from datetime import datetime
import os

def carregar_dados(caminho=None, api_url=None, api_key=None):
    if caminho:
        if caminho.endswith('.csv'):
            df = pd.read_csv(caminho)
        elif caminho.endswith('.parquet'):
            df = pd.read_parquet(caminho)
        else:
            raise ValueError("Formato de arquivo não suportado. Use CSV ou Parquet.")
    # Carregar de API
    elif api_url:
        headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
        response = requests.get(api_url, headers=headers)
        data = response.json()
        if isinstance(data, dict):
            for chave in data:
                print(f'{chave}')
        elif isinstance(data, list):
            for index, item in enumerate(data):
                print(item + f'{index}.')

        chave = input("Digite o nome da chave dos dados da API: ")
        df = pd.DataFrame(data[chave])
    else:
        raise ValueError("Forneça um caminho de arquivo ou uma URL de API.")
    
    return df

def expandir_colunas_dict(df):
    for coluna in df.columns:
        if isinstance(df[coluna].iloc[0], dict):
            print(f"Expandindo coluna '{coluna}' que contém dicionário...")
            colunas_expandidas = pd.json_normalize(df[coluna])
            colunas_expandidas.columns = [f"{coluna}_{subcol}" for subcol in colunas_expandidas.columns]
            df = df.drop(columns=[coluna]).join(colunas_expandidas) # remove a coluna original e adiciona as expandidas
    
    return df

def escolher_colunas(df):
    print("Prévia dos dados:")
    print(df.head())

    print("\nColunas disponíveis:")
    for i, coluna in enumerate(df.columns):
        print(f"{i}: {coluna}")

    colunas_indices = input("\nDigite os números das colunas que deseja filtrar, separados por vírgula: ")
    colunas_indices = [int(x) for x in colunas_indices.split(',')]

    colunas_filtro = df.columns[colunas_indices]
    return colunas_filtro

def escolher_colunas_data(df):
    print("\nColunas disponíveis (para escolha de colunas de data):")
    for i, coluna in enumerate(df.columns):
        print(f"{i}: {coluna}")

    colunas_indices = input("\nDigite os números das colunas que são de data, separados por vírgula (ou pressione Enter se não houver): ")
    
    if colunas_indices:
        colunas_indices = [int(x) for x in colunas_indices.split(',')]
        colunas_data = df.columns[colunas_indices]
    else:
        colunas_data = []
    
    return colunas_data

def padronizar_colunas(df):
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

def tratar_datas(df, colunas_data):
    for coluna in colunas_data:
        valor_inicial = df[coluna].dropna().iloc[0]

        if isinstance(valor_inicial,(np.number)):
            if len(str(int(valor_inicial))) <= 10:
                print(f"Coluna '{coluna}' detectada em segundos. Convertendo para datetime...")
                try:
                    for t in df[coluna]:
                        timetoconvert = t
                        timestampe_s = timetoconvert / 1000

                        dt_object = datetime.fromtimestamp(timestampe_s)
                        formatted_update = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                        df[coluna] = formatted_update
                except TypeError:
                    print("Datas já convertidas")

            elif len(str(int(valor_inicial))) >= 13:
                print(f"Coluna '{coluna} detectada em milissegundos. Convertendo para datetime...'")
                try:
                    for t in df[coluna]:
                        timetoconvert = t
                        timestampe_s = timetoconvert / 1000

                        dt_object = datetime.fromtimestamp(timestampe_s)
                        formatted_update = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                        df[coluna] = formatted_update
                except TypeError:
                    print("Datas já convertidas")

        elif isinstance(valor_inicial, str):
            print(f"Coluna '{coluna} detectada como stirng. Tetntando converter para datetime...'")
            df[coluna] = pd.to_datetime(df[coluna], errors='coerse')
        else:
            print(f"Coluna '{coluna}' tem formato desconhecido. Tentando conversão padrão...")
            
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
    return df

def remover_duplicatas(df):
    for coluna in df.columns:
        if df[coluna].apply(lambda x: isinstance(x, (list, dict))).any():
            print(f"Coluna '{coluna}' contém listas ou dicionários. Convertendo para string...")
            df[coluna] = df[coluna].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    
    # Agora pode remover duplicatas com segurança
    return df.drop_duplicates(keep="first", ignore_index=True)


def salvar_parquet(df, api_url, caminho="data/"):
    nome_api = api_url.split('//')[-1].split('/')[0]
    data_hora = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    nome_arquivo = f"{nome_api}_{data_hora}.parquet"
    caminho_completo = os.path.join(caminho, nome_arquivo)
    df.to_parquet(caminho_completo, engine='fastparquet', index=False)
    return os.path.abspath(caminho_completo)


def ler_parquet(caminho_arquivo, columns):
    return pd.read_parquet(caminho_arquivo, engine="fastparquet", columns=columns)
    

def processo_etl(caminho=None, api_url=None, api_key=None, caminho_saida='data/'):
    df = carregar_dados(caminho, api_url, api_key)
    
    df = expandir_colunas_dict(df)
    
    df = padronizar_colunas(df)
    
    colunas_filtro = escolher_colunas(df)
    
    colunas_data = escolher_colunas_data(df)
    
    if len(colunas_data) > 0:
        df = tratar_datas(df, colunas_data)
    
    df = remover_duplicatas(df)
    
    df = df[colunas_filtro]
    
    caminho_arquivo = salvar_parquet(df, api_url, caminho_saida)
    print(f"Arquivo salvo em {caminho_arquivo}")

    df_parquet = ler_parquet(caminho_arquivo, colunas_filtro)
    print(df_parquet.sample(5))

if __name__ == "__main__":
    caminho = input("Forneça o caminho do CSV/Parquet ou pressione Enter para usar a API: ")
    if not caminho:
        api_url = input("Forneça a URL da API: ")
        api_key = input("Forneça a chave da API (ou pressione Enter se não for necessário): ")
        processo_etl(api_url=api_url, api_key=api_key)
    else:
        processo_etl(caminho=caminho)