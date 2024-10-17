# Problemática: Análise de Previsão de Desastres Naturais

Você foi contratado por uma empresa de análise de risco ambiental para criar uma solução de ETL que monitore e analise dados de terremotos, furacões e outros eventos climáticos severos. Esses dados são extraídos de uma API pública e gratuita, e seu objetivo é fornecer informações para que governos e empresas possam prever e se preparar para desastres naturais.

**API Sugerida:**

* USGS Earthquake Hazards Program API (https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson)

    - A API oferece dados sobre terremotos em tempo real.

* Visual Crossing Weather API (https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline)

    - Dados climáticos históricos, incluindo informações sobre eventos climáticos extremos.
    - Você pode usar essas duas APIs para combinar dados de diferentes tipos de desastres naturais.

### Regras de Negócio:
**Extração dos Dados:**

* Utilize a API de terremotos para consultar os eventos dos últimos 10 anos.
* Utilize a API do OpenWeather para consultar informações sobre furacões e tempestades severas dos últimos 10 anos em áreas específicas (exemplo: América do Norte).
* Limite os dados a eventos que tenham magnitude/aceleração (para terremotos) ou velocidade de vento (para furacões) acima de um determinado limite.

**Transformação dos Dados:**

* Limpar os dados para remover duplicatas, normalizar as diferentes unidades de medida entre as APIs (exemplo: magnitude do terremoto, km/h para velocidade do vento).
* Criar colunas calculadas, como a densidade de eventos por região e ano.
* Enriquecer os dados combinando informações de terremotos e furacões em áreas geográficas comuns.
* Adicionar colunas de severidade do impacto com base nos critérios de magnitude (para terremotos) e velocidade de vento (para furacões).

**Carga dos Dados:**

* Armazenar os dados transformados em um banco de dados ou arquivos de formato Parquet, CSV ou Delta no Databricks.
* Crie tabelas distintas para cada tipo de evento, mas com a possibilidade de cruzamento entre terremotos e furacões.
* Análises e Relatórios:

**Distribuição de Desastres Naturais:**

* Gere gráficos de densidade geográfica de desastres (por região e por ano) para identificar áreas mais suscetíveis.
* Analisar o aumento ou a diminuição de desastres ao longo do tempo.

**Severidade dos Desastres:**

* Classificar os eventos em categorias de severidade e analisar quais regiões são mais impactadas por desastres severos.
* Identificar a relação entre a frequência e a severidade dos desastres em uma determinada região.

**Previsão de Áreas de Risco:**

* Com base nas tendências históricas, sugerir áreas de maior risco para eventos futuros.
* Usar métodos simples de regressão para prever o número esperado de eventos severos para os próximos 5 anos.

**Impacto em Infraestruturas:**

* Se possível, correlacionar áreas com infraestrutura crítica (cidades grandes, centrais elétricas, etc.) com o risco de desastres, fornecendo um relatório de vulnerabilidade.

**Desafio Adicional:**

* Implemente uma solução que permita o disparo de alertas automáticos (por e-mail ou API de notificações) quando um evento severo ocorrer em uma área de risco pré-definida.
