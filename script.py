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

def consultar_regiao(estados_dfs: dict):
    regioes = {
        "norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
        "nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
        "centro-oeste": ["GO", "MT", "MS", "DF"],
        "sudeste": ["ES", "MG", "RJ", "SP"],
        "sul": ["PR", "RS", "SC"]
    }
    
    print("Regiões disponíveis:")
    for regiao in regioes.keys():
        print(f"- {regiao}")

    regiao_escolhida = input("Escolha uma região para consultar (ex: norte): ").lower().strip()
    
    if regiao_escolhida in regioes:
        estados_selecionados = regioes[regiao_escolhida]
        df_filtrado = pd.DataFrame()
        for estado in estados_selecionados:
            if estado in estados_dfs:
                print(f"\nDados da região {regiao_escolhida} para o estado {estado}:")
                print(estados_dfs[estado])
                df_filtrado = pd.concat([df_filtrado, estados_dfs[estado]])
        return df_filtrado
    else:
        print("Região não encontrada.")
        return pd.DataFrame()
    
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

def menu_interativo():
    dados = carregar_dados(api_url)

    if dados is not None:
        # Calcular taxa de mortalidade
        dados = calcular_taxa_mortalidade(dados)
        estados_dfs = separar_dados_por_estado(dados)
        dados_selecionados = pd.DataFrame()

        while True:
            print("\nMenu:")
            print("1. Consultar dados de estados")
            print("2. Consultar dados por região")
            print("3. Salvar dados")
            print("4. Sair")
            opcao = input("Escolha uma opção: ")

            if opcao == '1':
                estados_dados = consultar_estados(estados_dfs)
                if not estados_dados.empty:
                    dados_selecionados = pd.concat([dados_selecionados, estados_dados])
                else:
                    print("Nenhum dado foi selecionado.")

            elif opcao == '2':
                regiao_dados = consultar_regiao(estados_dfs)
                if not regiao_dados.empty:
                    dados_selecionados = pd.concat([dados_selecionados, regiao_dados])
                else:
                    print("Nenhum dado foi selecionado.")

            elif opcao == '3':
                if dados_selecionados.empty:
                    print("Não há dados selecionados para salvar. Tente novamente.")
                    continue

                formato = input("Escolha o formato para salvar (csv ou parquet): ").strip().lower()
                if formato not in ['csv', 'parquet']:
                    print("Formato inválido. Escolha 'csv' ou 'parquet'.")
                    continue

                nome_arquivo_filtrado = input(f"Digite o nome do arquivo de dados filtrados ({formato}): ")
                nome_arquivo_completo = input(f"Digite o nome do arquivo completo ({formato}): ")

                salvar_dados(dados_selecionados, nome_arquivo_filtrado, formato)
                salvar_dados(dados, nome_arquivo_completo, formato)

            elif opcao == '4':
                print("Realmente deseja sair sem salvar os dados?")
                escolha = input("Digite S[im] ou N[ão]: ").strip().lower()
                if escolha in ["s", "sim"]:
                    print("Saindo do programa.")
                    break
                elif escolha in ["n", "nao", "não"]:
                    continue
                else:
                    print("Opção inválida, tente novamente.")

            else:
                print("Opção inválida. Tente novamente.")

# Iniciar o menu interativo
menu_interativo()