import pandas as pd
from sqlalchemy import create_engine

print('started')

# Cria a engine de conexão
engine = create_engine("postgresql://seu_usuario:sua_senha@localhost:5432/nome_banco")

# Define a consulta SQL que deseja executar
query = "SELECT * FROM gold.movimento_flat"

# Lê os dados diretamente em um DataFrame do pandas
df_movimento_flat = pd.read_sql_query(query, engine)


path_exportacao = 'app/data/output'


df_movimento_flat.astype(str).to_csv(f'{path_exportacao}/movimento_flat.csv',index=False)