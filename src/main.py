

from src.extract_api import busca_dados
from src.normalize import normaliza_dados
from src.database import criar_tabelas, salvar_dataframe

dados_paises = "BR|AR|UY|ES|DE|IT|US|MX|CA|CN|JP|NZ|AU|DZ|EG|ZA"
dados_indicadores = "77818|77819|77820"

if __name__ == "__main__":
    #1 Extrai os Dados
    dados = busca_dados(dados_paises, dados_indicadores)
    print(dados)

    #2 Normaliza os Dados
    df = normaliza_dados(dados)

    #3 Mostra os resultados
    print(df.head())
    
    #4 Insere no banco de dados e salva.
    criar_tabelas()
    salvar_dataframe(df)
