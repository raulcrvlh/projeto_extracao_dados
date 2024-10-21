# Análise de Dados de COVID-19 no Brasil

Este programa foi desenvolvido para facilitar a análise dos dados de COVID-19 no Brasil. Ele se conecta a uma API que fornece informações atualizadas sobre casos, mortes e outros dados relevantes relacionados à pandemia em diferentes estados do país.

## Funcionalidades

1. **Carregamento de Dados:**
   - O programa realiza uma chamada a uma API pública para carregar dados de COVID-19 no Brasil, incluindo informações sobre casos confirmados, mortes, suspeitas e rejeições.

2. **Filtragem por Estado:**
   - Os usuários podem consultar dados específicos de um ou mais estados, permitindo uma análise detalhada das informações de COVID-19 em nível regional.

3. **Cálculo da Taxa de Mortalidade:**
   - O programa calcula a taxa de mortalidade (porcentagem de mortos em relação ao total de casos confirmados) e adiciona essa informação como uma nova coluna no DataFrame.

4. **Formato de Data:**
   - A coluna de datas é formatada no padrão `ano/mês/dia` para facilitar a leitura e a análise temporal dos dados.

5. **Menu Interativo:**
   - Um menu interativo permite que os usuários façam múltiplas consultas, escolhendo estados e visualizando dados de forma dinâmica.

6. **Salvar Resultados:**
   - Os usuários têm a opção de salvar os dados analisados em arquivos nos formatos **CSV** ou **Parquet**, facilitando a exportação e o uso posterior dos dados.

## Como Usar

1. **Clone o Repositório:**
   ```bash
   git clone https://github.com/seuusuario/seurepositorio.git
   cd seurepositorio

2. **Instale as Dependências: Certifique-se de que você tem as bibliotecas requests e pandas instaladas. Você pode instalá-las usando:**
    ```bash
    pip install requests pandas

3. **Execute o Programa: Inicie o programa em um terminal:**
    ```bash
    python script.py

4. **Interaja com o Menu: Siga as instruções no menu interativo para consultar dados de estados e salvar os resultados.**
    

