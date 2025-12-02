import os
import random
from datetime import datetime, timedelta

TOTAL_CLIENTES = 1000
TOTAL_VENDAS = 50000
ARQUIVO_SAIDA = 'dados/vendas.txt'

def gerar_data_aleatoria():
    data_inicio = datetime(2023, 1, 1)
    dias_totais = 180
    dias_random = random.randint(0, dias_totais)
    data_gerada = data_inicio + timedelta(days=dias_random)
    return data_gerada.strftime("%Y%m%d")

def gerar_txt_vendas_massivo():
    if not os.path.exists('dados'):
        os.makedirs('dados')

    print(f"Gerando {TOTAL_VENDAS} registros de vendas para {TOTAL_CLIENTES} clientes...")
    print(f"Arquivo alvo: {ARQUIVO_SAIDA}")

    with open(ARQUIVO_SAIDA, mode='w', encoding='utf-8') as f:
        for i in range(1, TOTAL_VENDAS + 1):
            venda_id = i
            cliente_id = random.randint(1, TOTAL_CLIENTES)
            produto_id = random.randint(100, 150)
            valor_float = random.uniform(10.00, 1500.00)
            valor_inteiro = int(valor_float * 100)
            data_str = gerar_data_aleatoria()

            linha = (
                f"{venda_id:05d}"
                f"{cliente_id:05d}"
                f"{produto_id:05d}"
                f"{valor_inteiro:08d}"
                f"{data_str}"
            )
            
            f.write(linha + "\n")

    print("Sucesso! Arquivo vendas.txt atualizado.")

if __name__ == "__main__":
    gerar_txt_vendas_massivo()
