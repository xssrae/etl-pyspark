import os
import random
from datetime import datetime, timedelta

TOTAL_CLIENTES = 1000
TOTAL_VENDAS = 50000
QTD_CLIENTES_CHURN = 60 
ARQUIVO_SAIDA = 'dados/vendas.txt'

def gerar_data_simulada(is_cliente_churn):
    data_inicio = datetime(2023, 1, 1)
    
    if is_cliente_churn:
        dias_random = random.randint(0, 60)
    else:

        dias_random = random.randint(0, 180)
        
    data_gerada = data_inicio + timedelta(days=dias_random)
    return data_gerada.strftime("%Y%m%d")

def gerar_txt_vendas_massivo():
    if not os.path.exists('dados'):
        os.makedirs('dados')

    print(f"Gerando {TOTAL_VENDAS} registros...")
    print(f"Simulação: Clientes 1 a {QTD_CLIENTES_CHURN} serão inativos (Churn).")
    
    with open(ARQUIVO_SAIDA, mode='w', encoding='utf-8') as f:
        for i in range(1, TOTAL_VENDAS + 1):
            
            venda_id = i
            cliente_id = random.randint(1, TOTAL_CLIENTES)

            eh_churn = cliente_id <= QTD_CLIENTES_CHURN

            data_str = gerar_data_simulada(eh_churn)
            
            produto_id = random.randint(100, 150)
            valor_float = random.uniform(10.00, 1500.00)
            valor_inteiro = int(valor_float * 100)

            linha = (
                f"{venda_id:05d}"
                f"{cliente_id:05d}"
                f"{produto_id:05d}"
                f"{valor_inteiro:08d}"
                f"{data_str}"
            )
            f.write(linha + "\n")

    print("Sucesso! Arquivo vendas.txt atualizado com simulação de Churn.")

if __name__ == "__main__":
    gerar_txt_vendas_massivo()