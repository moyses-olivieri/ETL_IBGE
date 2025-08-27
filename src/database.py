
"""

Tem como função de carregar os dados da API do IBGE no postgresql.

"""

import os
from sqlalchemy import create_engine, text
import pandas as pd


# Configuração de conexão

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ibge1234")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5441")
DB_NAME = os.getenv("POSTGRES_DB", "ibge_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


# Cria tabelas

def criar_tabelas():
    """
    Cria as tabelas caso não existam.
    """
    with engine.begin() as conn:
        # Tabela indicadores
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS indicadores (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) UNIQUE NOT NULL
            );
        """))

        # Tabela paises
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS paises (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100) UNIQUE NOT NULL
            );
        """))

        # Tabela dados
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dados (
                id SERIAL PRIMARY KEY,
                pais_id INT NOT NULL REFERENCES paises(id),
                indicador_id INT NOT NULL REFERENCES indicadores(id),
                ano INT NOT NULL,
                valor NUMERIC NOT NULL
            );
        """))
    print("[INFO] Tabelas criadas com sucesso!")


# Salvar DataFrame no banco

def salvar_dataframe(df: pd.DataFrame):
    """
    Salva os dados normalizados nas três tabelas: paises, indicadores e dados.
    
    dataframe com as colunas: ['paises', 'indicadores', 'ano', 'valor']
    """
    # Insere os países
    with engine.begin() as conn:
        paises = df['pais'].unique()
        for pais in paises:
            conn.execute(
                text("INSERT INTO paises (nome) VALUES (:nome) ON CONFLICT (nome) DO NOTHING"),
                {"nome": pais}
            )

    # Insere os indicadores
    with engine.begin() as conn:
        indicadores = df['indicador'].unique()
        for ind in indicadores:
            conn.execute(
                text("INSERT INTO indicadores (nome) VALUES (:nome) ON CONFLICT (nome) DO NOTHING"),
                {"nome": ind}
            )

    # Mapea os Id's e isere os dados
    with engine.begin() as conn:
        pais_map = dict(conn.execute(text("SELECT nome, id FROM paises")).fetchall())
        indicador_map = dict(conn.execute(text("SELECT nome, id FROM indicadores")).fetchall())

        # substitui os paises e indicadores pelos id's
        df['pais_id'] = df['pais'].map(pais_map)
        df['indicador_id'] = df['indicador'].map(indicador_map)

        df_final = df[['pais_id', 'indicador_id', 'ano', 'valor']]

        # Insere dados nas tabelas
        df_final.to_sql("dados", engine, if_exists="append", index=False)
        print(f"[INFO] Inseridos {len(df_final)} registros na tabela dados.")

