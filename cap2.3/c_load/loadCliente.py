# cap2.3/c_load/loadCliente.py

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
CREATE TABLE IF NOT EXISTS bi_dclientes (
    dclienteID BIGSERIAL PRIMARY KEY,
    codigo_cliente integer, 
    nome_cliente varchar, 
    endereco varchar, 
    cidade varchar, 
    cep varchar, 
    uf char(2)
    );        
    '''

insert_query = '''
INSERT INTO bi_dclientes (
    dclienteID,
    codigo_cliente,
    nome_cliente,
    endereco,
    cidade,
    cep,
    uf
)
VALUES (default, %s, %s, %s, %s, %s, %s)
'''


def createTableBI(connPar):   
  try:
    cur = connPar.cursor()
    cur.execute(create_table_query)

    return 1
  except Exception as e:
    print(f"[loadCliente.py|executeTransform] Ocorreu um erro: {e}")
    return None


def insertTableBI(connPar, dfPar): 
  try:
    cur = connPar.cursor()  
    for _, row in dfPar.iterrows():
      # Necessário para inserir somente IDs que  ainda não estão no banco de dados.
      cur.execute("SELECT dclienteID FROM bi_dclientes where codigo_cliente = %s", (row['codigo_cliente'],))
      existing_id = cur.fetchone()    
      if existing_id == None:
        cur.execute(insert_query, (
            row['codigo_cliente'],
            row['nome_cliente'],
            row['endereco'],
            row['cidade'],
            row['cep'],
            row['uf']                    
      ))
  except Exception as e:
        print(f"[loadCliente.py|executeTransform] Ocorreu um erro: {e}")
        return None

  connPar.commit()
  return 1




def executeLoad(dfPar):  
  try:
    print(f"Etapa: Carregando Clientes para o banco de dados")
    # Conecta com o Postgres
    conn = psycopg2.connect(**db_params)    
    createTableBI(conn)
    insertTableBI(conn, dfPar)

    return dfPar
  except Exception as e:
        print(f"[loadCliente.py|executeTransform] Ocorreu um erro: {e}")
        return None