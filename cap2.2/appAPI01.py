import pandas as pd
import requests

# Identificação das APIs
apiURLProdutosEstatisticas = "https://servicodados.ibge.gov.br/api/v1/produtos/estatisticas"


# Identificação dos arquivos
arquivoCSV = "01ProdutosEstatisiticas.csv"



# Extract
response = requests.get(apiURLProdutosEstatisticas)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

else:
    print(f"Falha em buscar a API. Código do status: {response.status_code}")
    data = []

# Transform
# Criando um dataframe com os dados da API
df = pd.DataFrame(data)

print(f"Valor do dataframe")
print(df)


# Load
df.to_csv(arquivoCSV, index=False)

print(f"Dados salvos com sucesso no arquivo {arquivoCSV}")
