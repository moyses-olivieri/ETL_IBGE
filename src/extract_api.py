"""
Tem como função de consumir os dados da API do IBGE.
"""

import requests

url = "https://servicodados.ibge.gov.br/api/v1/paises"

def busca_dados(paises, indicadores):
    """
    Esta função busca os dados da API do IBGE para os países e indicadores especificados.

    Quais regras de negócio foram implementadas:
    - Consulta a API do IBGE para obter dados de países e indicadores.
    - Tratamento de erros para requisições HTTP.
    - Retorno dos dados em formato JSON.
    - Argumentos para especificar países e indicadores.
    - Requisições para extrair os dados necessários, com status e tempo limite.
    - Utilizando fstring para formatar a URL, setando os parâmetros paises e indicadores
    """
    adress = f"{url}/{paises}/indicadores/{indicadores}" 
    response = requests.get(adress, timeout=5)
    response.raise_for_status()
    return response.json()
