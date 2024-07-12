import pandas as pd

# Identificação dos arquivos
arquivoCSV = '01PedidoClienteProduto.csv'
arquivoXLS = '02_primeiro_saida.xls'


## Extract
df = pd.read_csv(arquivoCSV, delimiter=',')

print(f"*** Conteúdo do arquivo: {arquivoCSV} ***")
print(df)

print(f"*** Tipo de dados das colunas ***")
print(df.dtypes)

print(f"*** Informações estatísticas das colunas ***")
print(df.describe())

## Transformation
# Altera o nome da coluna
df = df.rename(columns={'num_pedido': 'numero_pedido'})

# Cria um campo para ser usado como ID e move ela para a 1ª posição
df['pedidoID'] = range(1, len(df) + 1)
cols = df.columns.tolist()
cols = ['pedidoID'] + [col for col in cols if col != 'pedidoID']
df = df[cols]


print(f"*** Conteúdo do arquivo após transformação: {arquivoCSV} ***")
print(df)

## Load into file or database
print(f"*** Salvando o arquivo: {arquivoXLS} ***")
df.to_excel(arquivoXLS,  index=False, engine='xlsxwriter')


