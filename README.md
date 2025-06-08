🧭 ♨️ COMPASS
---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão aplicação-1.0.1-blue?style=flat-square" alt="Versão Aplicação">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment** que é uma solução desenvolvida no contexto do programa Data Master, promovido pela F1rst Tecnologia, com o objetivo de disponibilizar uma plataforma robusta e escalável para captura, processamento e análise de feedbacks de clientes do Banco Santander.


![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)



`📦 artefato` `iamgacarvalho/dmc-quality-pipeline-compass`

- **Versão:** `1.0.1`
- **Repositório:** [GitHub](https://github.com/gacarvalho/quality-pipeline-compass)
- **Imagem Docker:** [Docker Hub](https://hub.docker.com/repository/docker/iamgacarvalho/dmc-quality-pipeline-compass/tags/1.0.1/sha256-19fa113e182d6186b7cdc842670e7e42de0329e95817c4531809ccd793b3c122)
- **Descrição:**  A aplicação responsável por realizar as qualidade de dados operam como um agente de qualidade do pipeline para reviews de aplicativos, provenientes de diferentes fontes (Google Play, MongoDB e Apple Store). Ele realiza as seguintes tarefas principais: Volumetria, Schema e Pattern
- **Parâmetros:**


    - `$CONFIG_ENV` (`Pre`, `Pro`) → Define o ambiente: `Pre` (Pré-Produção), `Pro` (Produção).
    - `$PARAM1` (`bronze`, `SILVER`) → Define a camada do data laker a ser validado: `bronze` (Camada do Data Lake com dados brutos), `silver` (Camada do Data Lake com dados já tratados).

| Componente          | Descrição                                                                                                                               |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| **Objetivo**        | A aplicação responsável por realizar as qualidade de dados operam como um agente de qualidade do pipeline para reviews de aplicativos, provenientes de diferentes fontes (Google Play, MongoDB e Apple Store). |
| **Entrada**         | Ambiente (pre/prod) e a Camada Silver do lake a ser validado.                                                                                                                    |
| **Saída**           | Dados válidos/inválidos em Parquet + métricas no Elasticsearch                                                                          |
| **Tecnologias**     | PySpark, Elasticsearch, Parquet, SparkMeasure                                                                                           |
| **Fluxo Principal** | 1. Coleta dos dados Bronze ou Silver → 2. Aplica verificação de qualidade → 3. Armazenamento                                                      |
| **Validações**      | Volumetria, Schema e Pattern                                                                            |
| **Particionamento** | Por data referencia de carga (odate)                                                                                                    |
| **Métricas**        | Tempo execução, memória, registros válidos/inválidos, performance Spark                                                                 |
| **Tratamento Erros**| Logs detalhados, armazenamento de dados inválidos                                                                              |
| **Execução**        | `app-code-compass-quality-pipeline.py <env>`                                                                                                       |
