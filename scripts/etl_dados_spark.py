from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, current_timestamp, when, udf, length
from pyspark.sql.types import BooleanType, StringType
import psycopg2

# Configurações do PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/si_cooperative_cartoes"
connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Conexão via psycopg2 para execução de comandos SQL
conn = psycopg2.connect(
    dbname="si_cooperative_cartoes",
    user="admin",
    password="admin",
    host="postgres",
    port="5432"
)
cur = conn.cursor()

# Inicialização da Spark Session
spark = SparkSession.builder.appName("ETL silver").config("spark.jars", "jars/postgresql-42.7.3.jar").config(
    "spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.executor.extraClassPath", "jars/postgresql-42.7.3.jar"
                                                                             ).config("spark.driver.extraClassPath", "jars/postgresql-42.7.3.jar").getOrCreate()

# --------------------------------------------
# Transformação e Insert: Associado (Silver)
# --------------------------------------------

df_associado_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.associado",
    properties=connection_properties
)

# Limpeza e validações (remover espacos em branco no comeco e final do nome, sobrenome e email e filtrar por emails com formato valido):
df_associado_silver = df_associado_bronze.withColumn("nome", trim(upper(col("nome")))
                                                    ).withColumn("sobrenome", trim(upper(col("sobrenome")))
                                                    ).withColumn("email", trim(col("email"))
                                                                 ).filter(col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"))

# Escreve na staging table:
df_associado_silver.write.jdbc(
    url=jdbc_url,
    table="silver.associado_staging",
    mode="overwrite",
    properties=connection_properties
)

# UPSERT para tabela final:
upsert_associado = """
    INSERT INTO silver.associado (id, nome, sobrenome, idade, email)
    SELECT id, nome, sobrenome, idade, email FROM silver.associado_staging
    ON CONFLICT (id)
    DO UPDATE SET
        nome = EXCLUDED.nome,
        sobrenome = EXCLUDED.sobrenome,
        idade = EXCLUDED.idade,
        email = EXCLUDED.email;
"""
cur.execute(upsert_associado)
cur.execute("DROP TABLE IF EXISTS silver.associado_staging;")
conn.commit()

# --------------------------------------
# Transformação e Innsert: Conta (Silver)
# --------------------------------------

df_conta_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.conta",
    properties=connection_properties
)

# Valida tipos de conta e se data de criacao faz sentido:
df_conta_silver = df_conta_bronze.withColumn("tipo", upper(trim(col("tipo")))
                ).filter(col("tipo").isin(["CORRENTE", "POUPANCA", "EMPRESARIAL"])
                ).filter(col("data_criacao") <= current_timestamp())

# Garantin integridade referencial, que nao exista id associado sem relacao:
df_conta_silver = df_conta_silver.join(
    df_associado_silver,
    df_associado_silver.id == df_conta_silver.id_associado,
    "inner"
).select(df_conta_silver["*"])

df_conta_silver.write.jdbc(
    url=jdbc_url,
    table="silver.conta_staging",
    mode="overwrite",
    properties=connection_properties
)

upsert_conta = """
    INSERT INTO silver.conta (id, tipo, data_criacao, id_associado)
    SELECT id, tipo, data_criacao, id_associado FROM silver.conta_staging
    ON CONFLICT (id)
    DO UPDATE SET
        tipo = EXCLUDED.tipo,
        data_criacao = EXCLUDED.data_criacao,
        id_associado = EXCLUDED.id_associado;
"""
cur.execute(upsert_conta)
cur.execute("DROP TABLE IF EXISTS silver.conta_staging;")
conn.commit()

# ----------------------------------------
# Transformação e insert: Cartão (Silver)
# ----------------------------------------

df_cartao_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.cartao",
    properties=connection_properties
)

# remove espaços em branco no inicio e fim do nome e deixa em maiusculo:
df_cartao_silver = df_cartao_bronze.withColumn("nom_impresso", trim(upper(col("nom_impresso"))))

# Garantir integridade referencial, que nao exista id associado sem relacao:
df_cartao_silver = df_cartao_silver.join(
    df_associado_silver,
    df_associado_silver.id == df_cartao_silver.id_associado,
    "inner"
).join(
    df_conta_silver,
    df_conta_silver.id == df_cartao_silver.id_conta,
    "inner").select(df_cartao_silver["*"])


# Ingestao na staging:
df_cartao_silver.write.jdbc(
    url=jdbc_url,
    table="silver.cartao_staging",
    mode="overwrite",
    properties=connection_properties
)

upsert_cartao = """
    INSERT INTO silver.cartao (id, num_cartao, nom_impresso, data_criacao, id_conta, id_associado)
    SELECT id, num_cartao, nom_impresso, data_criacao,id_conta, id_associado FROM silver.cartao_staging
    ON CONFLICT (id)
    DO UPDATE SET
        num_cartao = EXCLUDED.num_cartao,
        nom_impresso = EXCLUDED.nom_impresso,
        id_conta = EXCLUDED.id_conta,
        id_associado = EXCLUDED.id_associado,
        data_criacao = EXCLUDED.data_criacao;
"""
cur.execute(upsert_cartao)
cur.execute("DROP TABLE IF EXISTS silver.cartao_staging;")
conn.commit()

# ------------------------------------------
# Transformação e Insert: Movimento (Silver)
# -------------------------------------------

df_movimento_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.movimento",
    properties=connection_properties
)

# Filtra validacoes que sao menores ou iguais a zero, pois nao fazem sentido, formata a descricao e garante datas possiveis:
df_movimento_silver = df_movimento_bronze.filter(col("vlr_transacao") > 0
                                                ).withColumn("des_transacao", trim(upper(col("des_transacao")))
                                                ).filter(col("data_movimento") <= current_timestamp())


# garantir integridade referencial:
df_movimento_silver = df_movimento_silver.join(
    df_cartao_silver,
    df_movimento_silver.id_cartao == df_cartao_silver.id,
    "inner"
).select(df_movimento_silver["*"])


df_movimento_silver.write.jdbc(
    url=jdbc_url,
    table="silver.movimento_staging",
    mode="overwrite",
    properties=connection_properties
)

upsert_movimento = """
    INSERT INTO silver.movimento (id, vlr_transacao, des_transacao, data_movimento, id_cartao)
    SELECT id, vlr_transacao, des_transacao, data_movimento, id_cartao FROM silver.movimento_staging
    ON CONFLICT (id)
    DO UPDATE SET
        vlr_transacao = EXCLUDED.vlr_transacao,
        des_transacao = EXCLUDED.des_transacao,
        data_movimento = EXCLUDED.data_movimento,
        id_cartao = EXCLUDED.id_cartao;
"""
cur.execute(upsert_movimento)
cur.execute("DROP TABLE IF EXISTS silver.movimento_staging;")

# Criação da tabela movimento_flat na camada gold:

cur.execute("""
CREATE TABLE IF NOT EXISTS gold.movimento_flat (
    nome_associado VARCHAR(100),
    sobrenome_associado VARCHAR(100),
    idade_associado BIGINT,
    vlr_transacao_movimento DECIMAL(10,2),
    des_transacao_movimento VARCHAR(100),
    data_movimento TIMESTAMP,
    numero_cartao BIGINT,
    nome_impresso_cartao VARCHAR(100),
    data_criacao_cartao TIMESTAMP,
    tipo_conta VARCHAR(100),
    data_criacao_conta TIMESTAMP
);
""")

# Insert na tabela movimento_flat:
cur.execute("""
INSERT INTO gold.movimento_flat
SELECT associado.nome as nome_associado,
    associado.sobrenome as sobrenome_associado,
    associado.idade as idade_associado,
    movimento.vlr_transacao as vlr_transacao_movimento,
    movimento.des_transacao as des_transacao_movimento,
    movimento.data_movimento as data_movimento,
    cartao.num_cartao as numero_cartao,
    cartao.nom_impresso as nome_impresso_cartao,
    cartao.data_criacao as data_criacao_cartao,
    conta.tipo as tipo_conta,
    conta.data_criacao as data_criacao_conta
FROM silver.associado as associado
INNER JOIN silver.conta as conta ON associado.id = conta.id_associado
INNER JOIN silver.cartao as cartao ON conta.id = cartao.id_conta AND associado.id = conta.id_associado
INNER JOIN silver.movimento as movimento ON cartao.id = movimento.id_cartao;
""")

# Finalização
conn.commit()
cur.close()
conn.close()
print("Silver ETL concluído com sucesso!")
spark.stop()