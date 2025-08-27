import pandas as pd

def normaliza_dados(dados_json):
    """
    Normaliza os dados da API IBGE para DataFrame.
    Estrutura final: pais, indicador, ano, valor
    """
    registros = []
    for item in dados_json:
        indicador = item.get("indicador")
        for serie in item.get("series", []):
            pais = serie["pais"]["id"]
            for ano_valor in serie.get("serie", []):
                for ano, valor in ano_valor.items():
                    # Ignorando anos inválidados e valores ausentes
                    if ano.isdigit() and valor not in ("", "-", None):
                        try:
                            registros.append({
                                "pais": pais,
                                "indicador": indicador,
                                "ano": int(ano),
                                "valor": float(valor)
                            })
                        except ValueError:
                            # Ignora valores que não são conversíveis.
                            continue
    df = pd.DataFrame(registros)
    print(f"[INFO] Registros normalizados: {len(df)}")
    return df
