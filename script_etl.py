import pandas as pd
import requests
import numpy as np
from datetime import datetime
import os

def carregar_dados(caminho=None, api_url=None, api_key=None):
    # TODO: testar leitura de arquivos CSV e PARQUET
    if caminho:
        if caminho.endswith('.csv'):
            df = pd.read_csv(caminho)
        elif caminho.endswith('.parquet'):
            df = pd.read_parquet(caminho)
        else:
            raise ValueError("Formato de arquivo não suportado. Use CSV ou Parquet.")
    # Carregar de API
    elif api_url and api_key:
        url_completa = f"{api_url}?key={api_key}"
        response = requests.get(url_completa)
        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict):
                    print(f'Escolha a chave onde estão os dados:')
                    for chave in data:
                        print(chave)
                elif isinstance(data, list):
                    for index, item in enumerate(data):
                        print(item + f'{index}.')

                chave = input("Digite o nome da chave dos dados da API: ")
                df = pd.DataFrame(data[chave])
            except requests.exceptions.JSONDecodeError:
                print("Resposta recebida: ",response.text)
        else:
            print(f"Resposta {response.status_codes} da API. Mensgaem: {response.text}")
            
    else:
        raise ValueError("Forneça um caminho de arquivo ou uma URL de API.")
    
    return df

def expandir_colunas_dict(df):
    # caso as colunas sejam dicionários, faz a normalização para expandir
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
        # TODO: TRATAR CASO SEJA UM DICIONNÁROI/LISTA COM DADOS DE DATA
        if coluna not in df.columns:
            print(f"Coluna {coluna} não econtrada no DataFrame.")
            continue

        valor_inicial = df[coluna].dropna().iloc[0]

        if isinstance(valor_inicial, np.number):
            try:
                valor_inicial = int(valor_inicial)
                if len(valor_inicial) <= 10:
                    print(f"Coluna '{coluna}' detectada em segundos. Convertendo para datetime...")
                    for t in df[coluna]:
                        timetoconvert = t
                        timestampe_s = timetoconvert / 1000

                        dt_object = datetime.fromtimestamp(timestampe_s)
                        formatted_update = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                        df[coluna] = formatted_update

                elif len(valor_inicial) >= 13:
                    print(f"Coluna '{coluna} detectada em milissegundos. Convertendo para datetime...'")
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
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
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
    # TODO: fazer tratamento para salvar com nome genérico ou com o próprio nome do arquivo csv
    # salva arquivo parquet de API utilizando o nome da api após o https:// + hora de criação
    nome = api_url.split('//')[-1].split('/')[0]
    data_hora = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    nome_arquivo = f"{nome}_{data_hora}.parquet"
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