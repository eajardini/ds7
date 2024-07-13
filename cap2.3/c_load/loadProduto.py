# cap2.3/c_load/loadProduto.py

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
CREATE TABLE IF NOT EXISTS bi_dprodutos (
    dprodutoID BIGSERIAL PRIMARY KEY,
    codigo_produto integer, 
    unidade char(5), 
    descricao varchar, 
    valor_venda numeric(6,2), 
    valor_custo numeric(6,2), 
    qtde_minima numeric(6,2), 
    quantidade numeric(6,2), 
    comissao_produto numeric(6,2)
    );        
    '''




insert_query = '''
INSERT INTO bi_dprodutos (
  dprodutoID,
  codigo_produto,
  unidade,
  descricao,
  valor_venda,
  valor_custo,
  qtde_minima,
  quantidade,
  comissao_produto      
)
VALUES (default, %s, %s, %s, %s, %s, %s, %s, %s)
'''


def createTableBI(connPar):   
  try:
    cur = connPar.cursor()
    cur.execute(create_table_query)

    return 1
  except Exception as e:
    print(f"[loadProduto.py|executeTransform] Ocorreu um erro: {e}")
    return None


def insertTableBI(connPar, dfPar): 
  try:
    cur = connPar.cursor()  
    for _, row in dfPar.iterrows():
      # Necessário para inserir somente IDs que  ainda não estão no banco de dados.
      cur.execute("SELECT dprodutoID FROM bi_dprodutos where codigo_produto = %s", (row['codigo_produto'],))
      existing_id = cur.fetchone()    
      if existing_id == None:
        cur.execute(insert_query, (
            row['codigo_produto'],
            row['unidade'],
            row['descricao'],
            row['valor_venda'] ,    
            row['valor_custo'],
            row['qtde_minima'],
            row['quantidade'],
            row['comissao_produto']       
      ))
  except Exception as e:
        print(f"[loadProduto.py|executeTransform] Ocorreu um erro: {e}")
        return None

  connPar.commit()
  return 1


def executeLoad(dfPar):  
  try:
    print(f"Etapa: Carregando Produtos para o banco de dados")
    # Conecta com o Postgres
    conn = psycopg2.connect(**db_params)    
    createTableBI(conn)
    insertTableBI(conn, dfPar)

    return dfPar
  except Exception as e:
        print(f"[loadProduto.py|executeTransform] Ocorreu um erro: {e}")
        return None