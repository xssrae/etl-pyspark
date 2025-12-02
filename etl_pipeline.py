import sys
import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

def iniciar_spark_session(app_name="Desafio ETL Pipeline"):
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"Erro ao iniciar a sessão Spark: {e}", file=sys.stderr)
        sys.exit(1)

def ler_clientes(spark, caminho_csv):
    try:
        clientes = spark.read.csv(caminho_csv, header=True, inferSchema=True)
        return clientes
    except Exception as e:
        print(f"Erro ao ler o arquivo de clientes: {e}", file=sys.stderr)
        sys.exit(1)

def processar_vendas(spark, caminho_txt):
    try:
        raw = spark.read.text(caminho_txt)
        vendas = raw.select(
            F.substring(F.col("value"), 1, 5).cast(IntegerType()).alias("venda_id"),
            F.substring(F.col("value"), 6, 5).cast(IntegerType()).alias("cliente_id"),
            F.substring(F.col("value"), 11, 5).cast(IntegerType()).alias("produto_id"),
            (F.substring(F.col("value"), 16, 8).cast(DoubleType()) / 100).alias("valor"),
            F.to_date(F.substring(F.col("value"), 24, 8), "yyyyMMdd").alias("data_venda")
        )
        return vendas
    except Exception as e:
        print(f"Erro ao processar o arquivo de vendas: {e}", file=sys.stderr)
        raise

def gerar_resumo_clientes(vendas, clientes):
    join = vendas.join(clientes, on="cliente_id", how="inner")
    resumo = join.groupBy("cliente_id", "nome").agg(
        F.sum("valor").alias("total_vendas"),
        F.count("venda_id").alias("quantidade_vendas"),
        F.avg("valor").alias("ticket_medio")
    )
    resumo = resumo.withColumn("total_vendas", F.round("total_vendas", 2))
    resumo = resumo.withColumn("ticket_medio", F.round("ticket_medio", 2))
    return resumo

def gerar_balanco_produtos(vendas):
    balanco = vendas.groupBy("produto_id").agg(
        F.sum("valor").alias("total_vendas_produto"),
        F.count("venda_id").alias("quantidade_vendas_produto"),
        F.avg("valor").alias("ticket_medio_produto")
    )
    balanco = balanco.withColumn("total_vendas_produto", F.round("total_vendas_produto", 2))
    balanco = balanco.withColumn("ticket_medio_produto", F.round("ticket_medio_produto", 2))
    return balanco

def salvar_output(df, caminho_arquivo):
    try:
        pasta = os.path.dirname(caminho_arquivo)
        if not os.path.exists(pasta):
            os.makedirs(pasta)
            
        print(f"Salvando arquivo em: {caminho_arquivo}...")
        
        dados = df.collect()
        colunas = df.columns
        
        with open(caminho_arquivo, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colunas)
            writer.writerows(dados)
            
    except Exception as e:
        print(f"Erro ao salvar arquivo {caminho_arquivo}: {e}")
        raise

def main():
    spark = iniciar_spark_session()
    
    path_clientes = "dados/clientes.csv"
    path_vendas = "dados/vendas.txt"
    
    path_output_clientes = "output/resumo_clientes.csv"
    path_output_produtos = "output/balanco_produtos.csv"

    print("--- Iniciando Pipeline ETL ---")

    try:
        print("Lendo arquivos...")
        clientes = ler_clientes(spark, path_clientes)
        vendas = processar_vendas(spark, path_vendas)

        print("Gerando resumo por clientes...")
        resumo_clientes = gerar_resumo_clientes(vendas, clientes)

        print("Gerando balanço por produtos...")
        balanco_produtos = gerar_balanco_produtos(vendas)

        print("Salvando resultados...")
        
        salvar_output(resumo_clientes, path_output_clientes)
        salvar_output(balanco_produtos, path_output_produtos)

        print("--- Pipeline concluído com sucesso! ---")
        print(f"Arquivos gerados na pasta 'output/'.")
        
    except Exception as e:
        print(f"Ocorreu um erro fatal no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
