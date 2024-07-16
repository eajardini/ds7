# cap2.3/c_load/loadVendedor.py

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os


## Load
# Parâmetros de conexão
load_dotenv()
db_params = {
    'dbname': os.getenv('DB_NAME_DW'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

create_table_query = '''
CREATE TABLE IF NOT EXISTS bi_dvendedores (
    dvendedor_sk BIGSERIAL PRIMARY KEY,
    codigo_vendedor integer, 
    nome_vendedor varchar, 
    salario_fixo numeric(9,2),     
    faixa_comissao char(1)
    );        
    '''

insert_query = '''
INSERT INTO bi_dvendedores (
    dvendedor_sk,
    codigo_vendedor,
    nome_vendedor,
    salario_fixo,
    faixa_comissao    
)
VALUES (default, %s, %s, %s, %s)
'''


def createTableBI(connPar):   
  try:
    cur = connPar.cursor()
    cur.execute(create_table_query)
    connPar.commit()
    cur.close()
    return 1
  except Exception as e:
    print(f"[loadVendedor.py|executeTransform] Ocorreu um erro: {e}")
    return None


def insertTableBI(connPar, dfPar): 
  try:
    cur = connPar.cursor()  
    for _, row in dfPar.iterrows():
      # Necessário para inserir somente IDs que  ainda não estão no banco de dados.
      cur.execute("SELECT dvendedor_sk FROM bi_dvendedores where codigo_vendedor = %s", (row['codigo_vendedor'],))
      existing_id = cur.fetchone()    
      if existing_id == None:
        cur.execute(insert_query, (
            row['codigo_vendedor'],
            row['nome_vendedor'],
            row['salario_fixo'],
            row['faixa_comissao']            
      ))
  except Exception as e:
        print(f"[loadVendedor.py|executeTransform] Ocorreu um erro: {e}")
        return None

  connPar.commit()
  return 1


def executeLoad(dfPar):  
  try:
    print(f"Etapa: Carregando Vendedores para o banco de dados")
    # Conecta com o Postgres
    conn = psycopg2.connect(**db_params)    
    createTableBI(conn)
    insertTableBI(conn, dfPar)

    conn.close()
    return dfPar
  except Exception as e:
        print(f"[loadVendedor.py|executeTransform] Ocorreu um erro: {e}")
        return None