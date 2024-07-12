# cap2.3/a_extract/extVendedor.py

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Parâmetros de conexão do banco de dados
load_dotenv()
db_params = {
    'dbname': os.getenv('DB_NAME_ERP'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

# Define a consulta SQL
query = """
    SELECT codigo_vendedor , nome_vendedor , salario_fixo , faixa_comissao FROM vendedor;
"""

def executeExtract():
    """
    Parametros:
        
    Retorno:
    pd.DataFrame: Resultado da consulta SQL em um dataframe
    """
    try:
        print(f"Etapa: Extraindo Vendedores")
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)         
        cur = conn.cursor()
        
        # Executa a consulta e cria um dataframe        
        cur.execute(query)

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description] # Pega o nomes das colunas do cursor
        df = pd.DataFrame(rows, columns=colnames)
        
        # Fecha a conexão
        cur.close()
        conn.close()
        
        return df
    except Exception as e:
        print(f"[extVendedor.py|executeExtract] Ocorreu um erro: {e}")
        return None





