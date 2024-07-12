# ./ds7/cap2.3/01_extCliente

import pandas as pd

# ETL Cliente
from a_extract.extCliente import executeExtract as cliExecuteExtract
from b_transform.transCliente import executeTransform as cliExecuteTransform
from c_load.loadCliente import executeLoad as cliExecuteLoad

# ETL Vendedor
from a_extract.extVendedor import executeExtract as vendExecuteExtract


#******** ETL Cliete *******
# Chama a função para EXTRAIR os dados
df_cliResults = cliExecuteExtract()

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_cliResults is None:
    print("[appETLpy] Falha a executar a extração de CLIENTES")
    quit()
# else:    
    # print(f"Valor do DF após extração: {df_cliResults}")


# Chama a função para TRANSFORMAR os dados
df_cliResults = cliExecuteTransform(df_cliResults)

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_cliResults is None:
    print("[appETLpy] Falha a executar a transformação de CLIENTES")
    quit()
# else:    
#     print(f"Valor do DF após transformação: {df_cliResults}")


# Chama a função para CARREGAR os dados bo banco de dados
df_cliResults = cliExecuteLoad(df_cliResults)

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_cliResults is None:
    print("[appETLpy] Falha a executar o carregamento para o banco de dados de CLIENTES")
    quit()
# else:    
#     print (f"Valor do DF após carregamento no banco de dados: ")


#******** ETL Vendedor *******
# Chama a função para EXTRAIR os dados
df_vendResults = vendExecuteExtract()

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_vendResults is None:
    print("[appETLpy] Falha a executar a extração de VENDEDORES")
    quit()
else:    
    print(f"Valor do DF após extração: {df_vendResults}")


# # Chama a função para TRANSFORMAR os dados
# df_results = cliExecuteTransform(df_results)

# # Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
# if df_results is not None:
#     print(f"Valor do DF após transformação: {df_results}")
# else:
#     print("[appETLpy] Falha a executar a trransformação")
#     quit()


# # Chama a função para CARREGAR os dados bo banco de dados
# df_results = cliExecuteLoad(df_results)

# # Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
# if df_results is not None:
#     print (f"Valor do DF após carregamento no banco de dados: ")
# else:
#     print("[appETLpy] Falha a executar o carregamento para o banco de dados")
#     quit()
