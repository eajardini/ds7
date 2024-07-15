# cap2.3/b_transform/transPedido.py

import pandas as pd



def executeTransform(dfPar):

  try:
    # Colocar aqui as tranformações dos clientes

    print(f"Etapa: Transformando Pedidos")
    
    # remove os registro que contains valor nulos nas colunas valor_venda e valor_custo
    dfPar = dfPar.dropna(subset=['valor_venda', 'valor_custo'])

    return dfPar
  except Exception as e:
        print(f"[transPedido.py|executeTransform] Ocorreu um erro: {e}")
        return None