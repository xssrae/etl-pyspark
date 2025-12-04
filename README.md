# Desafio de Engenharia de Dados - ETL Pipeline com PySpark

Este projeto implementa um pipeline ETL (Extract, Transform, Load) completo para processamento de dados de vendas e clientes. O sistema ingere dados de fontes heterogêneas (CSV e TXT Posicional), realiza limpeza, enriquecimento e entrega insights de negócios e dados particionados para Data Lake.

## 🚀 Funcionalidades

* **Ingestão de Dados:**
    * Leitura de `clientes.csv` (Schema Inferred).
    * Parsing manual de `vendas.txt` (formato Fixed-Width/Posicional).
* **Transformação:**
    * Tratamento de tipos de dados (Inteiros, Decimais, Datas).
    * Enriquecimento: Cálculo de Idade e categorização por Faixa Etária.
    * Joins entre dados transacionais e dimensionais.
* **Particionamento (Diferencial):**
    * Output organizado em pastas por data (`data_venda=YYYY-MM-DD`), simulando estrutura de Data Lake.
* **Analytics:**
    * Balanço financeiro por produto.
    * Análise de comportamento de compra por faixa etária.
    * Ranking de melhores clientes.

## 🛠️ Tecnologias Utilizadas

* **Python 3.14**
* **PySpark** (Processamento distribuído)
* **Bibliotecas Standard:** `csv`, `os`, `random` (para geração de massa de dados e persistência local sem dependência de Hadoop/Winutils).

## 📂 Estrutura do Projeto

```text
├── dados/                  # Arquivos de entrada (Gerados via script)
├── output/                 # Saída do Pipeline
│   ├── insights/           # Relatórios gerenciais (CSV)
│   ├── balanco_produtos.csv
│   └── vendas_detalhadas/  # Data Lake Particionado por Data
├── etl_pipeline.py         # Script Principal
├── gerar_dados.py          # Gerador de Clientes
├── gerar_vendas_massivo.py # Gerador de Vendas (Volume)
└── README.md