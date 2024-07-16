# cap2.3/c_load/loadPedido.py

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
CREATE TABLE IF NOT EXISTS bi_fpedidos (
    fpedido_sk BIGSERIAL PRIMARY KEY,
    num_pedido integer not null,
    data_pedido date not null,
    dcliente_skfk  bigint not null,
    dvendedor_skfk  bigint not null,
    dproduto_skfk bigint not null,
    quantidade integer not null,
    valor_venda numeric(10,2) not null,
    valor_custo numeric(10,2) not null,
    constraint fk_from_bifpedidos_to_bidclientes foreign key (dcliente_skfk)    
        references bi_dclientes,
    constraint fk_from_bifpedidos_to_bidvendedores foreign key (dvendedor_skfk)    
        references bi_dvendedores,
    constraint fk_from_bifpedidos_to_bidprodutos foreign key (dproduto_skfk)    
        references bi_dprodutos
    );        
    '''

insert_query = '''
INSERT INTO bi_fpedidos (
    fpedido_sk,
    num_pedido,
    data_pedido,
    dcliente_skfk,
    dvendedor_skfk,
    dproduto_skfk,
    quantidade,
    valor_venda,
    valor_custo   
)
VALUES (default, %s, %s, %s, %s, %s, %s, %s, %s)
'''


def createTableBI(connPar):   
  try:
    cur = connPar.cursor()
    cur.execute(create_table_query)
    connPar.commit()
    cur.close()
    return 1
  except Exception as e:
    print(f"[loadPedido.py|executeTransform] Ocorreu um erro: {e}")
    return None


def insertTableBI(connPar, dfPar): 
  try:
    cur = connPar.cursor()  
    for _, row in dfPar.iterrows():
      # Necessário para inserir somente IDs que  ainda não estão no banco de dados.
      cur.execute("SELECT fpedido_sk FROM bi_fpedidos where num_pedido = %s", (row['num_pedido'],))
      existing_id = cur.fetchone()    
      if existing_id == None:
        # Para cada FK, temos de buscar a SK da tabela correspondente
        cur.execute("SELECT dcliente_sk FROM bi_dclientes where codigo_cliente = %s", (row['codigo_cliente'],))
        dcliente_sk = cur.fetchone()    
        cur.execute("SELECT dvendedor_sk FROM bi_dvendedores where codigo_vendedor = %s", (row['codigo_vendedor'],))
        dvendedor_sk = cur.fetchone()    
        cur.execute("SELECT dproduto_sk FROM bi_dprodutos where codigo_produto = %s", (row['codigo_produto'],))
        dprodutos_sk = cur.fetchone() 

        cur.execute(insert_query, (
            row['num_pedido'],
            row['data_pedido'],
            dcliente_sk,
            dvendedor_sk,
            dprodutos_sk,
            row['quantidade'],
            row['valor_venda'],
            row['valor_custo'],            
      ))
  except Exception as e:
        print(f"[loadPedido.py|executeTransform] Ocorreu um erro: {e}")
        return None

  connPar.commit()
  return 1


def executeLoad(dfPar):  
  try:
    print(f"Etapa: Carregando Pedidos para o banco de dados")
    # Conecta com o Postgres
    conn = psycopg2.connect(**db_params)    
    createTableBI(conn)
    insertTableBI(conn, dfPar)

    conn.close()
    return dfPar
  except Exception as e:
        print(f"[loadPedido.py|executeTransform] Ocorreu um erro: {e}")
        return None