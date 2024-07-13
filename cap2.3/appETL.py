# ./ds7/cap2.3/01_extCliente

import pandas as pd

# ETL Cliente
from a_extract.extCliente import executeExtract as cliExecuteExtract
from b_transform.transCliente import executeTransform as cliExecuteTransform
from c_load.loadCliente import executeLoad as cliExecuteLoad

# ETL Vendedor
from a_extract.extVendedor import executeExtract as vendExecuteExtract
from b_transform.transVendedor import executeTransform as vendExecuteTransform
from c_load.loadVendedor import executeLoad as vendExecuteLoad

# ETL Produto
from a_extract.extProduto import executeExtract as prodExecuteExtract
from b_transform.transProduto import executeTransform as prodExecuteTransform
from c_load.loadProduto  import executeLoad as prodExecuteLoad


#******** ETL Cliente *******
# Chama a função para EXTRAIR os dados

print("++++++++++")
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
print("++++++++++")
df_vendResults = vendExecuteExtract()

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_vendResults is None:
    print("[appETLpy] Falha a executar a extração de VENDEDORES")
    quit()
# else:    
#     print(f"Valor do DF após extração: ")


# Chama a função para TRANSFORMAR os dados
df_vendResults = vendExecuteTransform(df_vendResults)

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_vendResults is None:
    print("[appETLpy] Falha a executar a transformação de VENDEDORES")
    quit()
# else:    
#     print(f"Valor do DF após transformação: ")


# Chama a função para CARREGAR os dados bo banco de dados
df_vendResults = vendExecuteLoad(df_vendResults)
# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_vendResults is None:
    print("[appETLpy] Falha a executar o carregamento para o banco de dados")
    quit()
# else:    
#     print (f"Valor do DF após carregamento no banco de dados: ")



#******** ETL Produto *******
# Chama a função para EXTRAIR os dados

print("++++++++++")
df_prodResults = prodExecuteExtract()

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_prodResults is None:
    print("[appETLpy] Falha a executar a extração de PRODUTOS")
    quit()
# else:    
#     print(f"Valor do DF após extração: {df_prodResults}")


# Chama a função para TRANSFORMAR os dados
df_prodResults = prodExecuteTransform(df_prodResults)

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_prodResults is None:
    print("[appETLpy] Falha a executar a transformação de PRODUTOS")
    quit()
# else:    
#     print(f"Valor do DF após transformação: {df_prodResults}")


# Chama a função para CARREGAR os dados bo banco de dados
df_prodResults = prodExecuteLoad(df_prodResults)

# Mostra os dados do DataFrame. Essa parte não é obrigatório. Apenas para Visulização. Defe ser retira na versão de produção
if df_prodResults is None:
    print("[appETLpy] Falha a executar o carregamento para o banco de dados de PRODUTOS")
    quit()
# else:    
#     print (f"Valor do DF após carregamento no banco de dados: ")
