# Desafio de Engenharia de Dados - ETL Pipeline com PySpark

Este projeto implementa um pipeline ETL (Extract, Transform, Load) robusto utilizando PySpark para processamento de dados de vendas e clientes. O sistema ingere dados de fontes heterogêneas, realiza limpeza, aplica regras de negócio complexas e entrega dados estruturados prontos para análise (Data Lake).

## 🚀 Funcionalidades

* **Ingestão de Dados:**
    * Leitura de CSV com inferência de schema (`clientes.csv`).
    * Leitura e parsing manual de arquivos de texto posicional/Fixed-Width (`vendas.txt`).
* **Transformação & Data Quality:**
    * Tratamento de tipos de dados (Inteiros, Decimais com ajuste de escala, Datas).
    * Enriquecimento de dados (Cálculo de idade e categorização de faixa etária).
    * Cruzamento de dados (Joins) entre transações e dimensões.
* **Particionamento (Data Lake):**
    * Output detalhado organizado em diretórios particionados por data (`data_venda=YYYY-MM-DD`), otimizando consultas futuras.
* **Analytics:**
    * Geração de KPIs financeiros por produto e cliente.
    * Insights sobre ticket médio e comportamento demográfico.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.x
* **Motor de Processamento:** PySpark (Apache Spark)
* **Bibliotecas Auxiliares:** `csv`, `os`, `shutil`, `unittest` (Testes), `random` (Mock Data).
* **Ambiente:** Executável localmente (Windows/Linux/Mac) sem dependência de instalação completa do Hadoop (Winutils bypass).

---

## 📂 Estrutura do Projeto

```text
├── dados/                  # Diretório de entrada (Gerado automaticamente)
│   ├── clientes.csv        # Cadastro de clientes
│   └── vendas.txt          # Arquivo posicional legado
├── output/                 # Diretório de saída
│   ├── resumo_clientes.csv # KPI consolidado por cliente
│   ├── balanco_produtos.csv# KPI consolidado por produto
│   └── vendas_detalhadas/  # Dataset particionado (Data Lake)
├── etl_pipeline.py         # Código principal do Pipeline
├── gerar_dados.py          # Script para gerar clientes fake
├── gerar_vendas_massivo.py # Script para gerar volume de vendas fake
├── test_etl.py             # Testes Automatizados (Unitários)
└── README.md               # Documentação