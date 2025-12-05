import unittest
from pyspark.sql import SparkSession
from etl_pipeline import processar_vendas, ler_clientes, gerar_resumo_clientes
import os
import shutil

class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestETL") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        os.makedirs("dados_teste", exist_ok=True)
        
        with open("dados_teste/vendas.txt", "w") as f:
            f.write("0000100001001000000200020230101")
            
        with open("dados_teste/clientes.csv", "w") as f:
            f.write("cliente_id,nome,data_nascimento\n")
            f.write("1,Teste Silva,1990-01-01")

    def tearDown(self):
        if os.path.exists("dados_teste"):
            shutil.rmtree("dados_teste")

    def test_leitura_vendas_posicional(self):
        """Testa se o parser do arquivo fixo TXT está cortando as colunas corretamente"""
        df = processar_vendas(self.spark, "dados_teste/vendas.txt")
        resultado = df.collect()[0]
        
        self.assertEqual(resultado["venda_id"], 1)
        self.assertEqual(resultado["cliente_id"], 1)
        self.assertEqual(resultado["valor"], 20.00) # 00002000 / 100

    def test_calculo_resumo(self):
        """Testa se o Join e a agregação de totais estão corretos"""
        df_vendas = processar_vendas(self.spark, "dados_teste/vendas.txt")
        df_clientes = ler_clientes(self.spark, "dados_teste/clientes.csv")
        
        df_resumo = gerar_resumo_clientes(df_vendas, df_clientes)
        resultado = df_resumo.collect()[0]
        
        self.assertEqual(resultado["nome"], "Teste Silva")
        self.assertEqual(resultado["total_vendas"], 20.00)

if __name__ == '__main__':
    unittest.main()