# Desafio - Vaga Analista Junior - ETL Pipeline com PySpark

Este projeto implementa um pipeline ETL (Extract, Transform, Load) utilizando PySpark para integrar dados de clientes e vendas, gerar resumos por cliente e relatórios financeiros por produto. O sistema ingere dados de fontes heterogêneas, realiza limpeza, aplica regras de negócio complexas e entrega dados estruturados prontos para análise (Data Lake).

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

* **Linguagem:** Python 3
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
````

---

## ▶️ Como Executar o Pipeline

Siga os passos abaixo para rodar o projeto no seu ambiente local.

### 1\. Pré-requisitos

Certifique-se de ter o Python instalado. Instale as dependências necessárias:

```bash
pip install pyspark pandas matplotlib
```

### 2\. Gerar Massa de Dados

Como os arquivos de dados brutos não são versionados, você deve executar os scripts geradores para criar a pasta `dados/` com informações simuladas:

```bash
# 1. Gera o cadastro de clientes
python gerar_dados.py

# 2. Gera 50.000 registros de vendas (com simulação de churn/inatividade)
python gerar_vendas_massivo.py
```

### 3\. Executar o ETL

Execute o script principal. O Spark processará os arquivos, aplicará as regras de negócio e salvará os resultados na pasta `output/`.

```bash
python etl_pipeline.py
```

*Ao final, verifique a pasta `output/` para ver os relatórios CSV e a pasta particionada `vendas_detalhadas/`.*

-----

## ✅ Testes Automatizados

O projeto inclui testes unitários para garantir a integridade da lógica de transformação e leitura de arquivos posicionais.

Para rodar a suíte de testes:

```bash
python test_etl.py
```

**O que é testado:**

  * Parsing correto das posições do arquivo `vendas.txt` (garantindo que ID, Valor e Data não venham corrompidos).
  * Lógica de Join e Agregação (Soma de valores) com dados controlados (Mock).

-----

## 📄 Exemplos de Arquivos (Input & Output)

### 1\. Entrada: `vendas.txt` (Formato Posicional)

Arquivo sem separadores (vírgulas ou pipes). O layout é fixo: ID(5), Cliente(5), Produto(5), Valor(8), Data(8).

```text
000010045200100000455020230512  <-- Lê-se: Venda 1, Cliente 452, Prod 100, R$ 45.50
000020000500102001500020230512  <-- Lê-se: Venda 2, Cliente 5, Prod 102, R$ 150.00
```

### 2\. Saída: `resumo_clientes.csv`

```csv
cliente_id,nome,total_vendas,quantidade_vendas,ticket_medio
5,Derrek,25506.01,101,252.53
9,Derby,24507.70,100,245.08
```

### 3\. Saída: Particionamento de Diretórios (Data Lake)

O pipeline organiza os dados detalhados simulando a estrutura de um Data Lake (Hive Partitioning), facilitando a leitura por dia específico:

```text
output/vendas_detalhadas/
    ├── data_venda=2023-01-01/
    │      └── dados.csv
    ├── data_venda=2023-01-02/
    │      └── dados.csv
    └── ...
```

-----

## 🛡️ Resiliência e Tratamento de Erros

O código foi desenvolvido focando em robustez para ambientes Windows e Linux:

1.  **Validação de Caminhos:** O script verifica e recria automaticamente as pastas de saída para garantir idempotência (pode rodar várias vezes sem erro).
2.  **Try/Except Blocks:** Todas as funções críticas possuem tratamento de exceção para falhar de forma graciosa e informativa.
3.  **Compatibilidade Windows:** Foi implementada uma estratégia híbrida na carga de dados (coleta via Spark -\> escrita via Python CSV nativo) para contornar a necessidade de binários do Hadoop (`winutils.exe`) no Windows.
