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
spark = SparkSession.builder.appName("ETL Silver").config("spark.jars", "jars/postgresql-42.7.3.jar").getOrCreate()

# --------------------------------------
# Transformação: Associado (Silver)
# --------------------------------------
df_associado_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.associado",
    properties=connection_properties
)

# Limpeza e validações
df_associado_silver = df_associado_bronze.dropDuplicates(["id"]) \
    .withColumn("nome", trim(upper(col("nome")))) \
    .withColumn("sobrenome", trim(upper(col("sobrenome")))) \
    .withColumn("email", trim(col("email"))) \
    .filter(col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")) \
    .filter((col("idade") >= 18) & (col("idade") <= 120)) \
    .dropDuplicates(["email"])  # Garantir unicidade de emails

# Escreve na staging table
df_associado_silver.write.jdbc(
    url=jdbc_url,
    table="silver.associado_staging",
    mode="overwrite",
    properties=connection_properties
)

# UPSERT para tabela final
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
# Transformação: Conta (Silver)
# --------------------------------------
df_conta_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.conta",
    properties=connection_properties
)

# Valida tipos de conta e datas
df_conta_silver = df_conta_bronze \
    .withColumn("tipo", upper(trim(col("tipo")))) \
    .filter(col("tipo").isin(["CORRENTE", "POUPANCA"])) \
    .filter(col("data_criacao") <= current_timestamp()) \
    .dropDuplicates(["id"])

# Valida relações com associado
df_associado_silver = spark.read.jdbc(
    url=jdbc_url,
    table="silver.associado",
    properties=connection_properties
)
df_conta_silver = df_conta_silver.join(
    df_associado_silver,
    df_conta_silver.id_associado == df_associado_silver.id,
    "inner"
).select(df_conta_silver["*"])

# Persistência
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

# --------------------------------------
# Transformação: Cartão (Silver)
# --------------------------------------
df_cartao_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.cartao",
    properties=connection_properties
)

# Função para validação Luhn
def luhn_check(card_num):
    try:
        digits = [int(d) for d in str(card_num)]
        odd_sum = sum(digits[-1::-2])
        even_digits = [2 * d for d in digits[-2::-2]]
        even_sum = sum(sum(divmod(d, 10)) for d in even_digits)
        return (odd_sum + even_sum) % 10 == 0
    except:
        return False

luhn_udf = udf(luhn_check, BooleanType())

# Validações
df_cartao_silver = df_cartao_bronze \
    .filter(length(col("num_cartao").cast("string")) == 16) \
    .withColumn("valid_luhn", luhn_udf(col("num_cartao"))) \
    .filter(col("valid_luhn")) \
    .drop("valid_luhn") \
    .dropDuplicates(["id"])

# Valida relações com conta e associado
df_conta_silver = spark.read.jdbc(
    url=jdbc_url,
    table="silver.conta",
    properties=connection_properties
)
df_cartao_silver = df_cartao_silver.join(
    df_conta_silver,
    df_cartao_silver.id_conta == df_conta_silver.id,
    "inner"
).select(df_cartao_silver["*"])

# Persistência
df_cartao_silver.write.jdbc(
    url=jdbc_url,
    table="silver.cartao_staging",
    mode="overwrite",
    properties=connection_properties
)

upsert_cartao = """
    INSERT INTO silver.cartao (id, num_cartao, nom_impresso, id_conta, id_associado)
    SELECT id, num_cartao, nom_impresso, id_conta, id_associado FROM silver.cartao_staging
    ON CONFLICT (id)
    DO UPDATE SET
        num_cartao = EXCLUDED.num_cartao,
        nom_impresso = EXCLUDED.nom_impresso,
        id_conta = EXCLUDED.id_conta,
        id_associado = EXCLUDED.id_associado;
"""
cur.execute(upsert_cartao)
cur.execute("DROP TABLE IF EXISTS silver.cartao_staging;")
conn.commit()

# --------------------------------------
# Transformação: Movimento (Silver)
# --------------------------------------
df_movimento_bronze = spark.read.jdbc(
    url=jdbc_url,
    table="bronze.movimento",
    properties=connection_properties
)

# Categorização de transações
def categorize_transaction(desc):
    desc = desc.lower()
    food_keywords = ["supermercado", "restaurante", "padaria"]
    transport_keywords = ["posto", "pedagio", "estacionamento"]
    
    if any(kw in desc for kw in food_keywords):
        return "ALIMENTACAO"
    elif any(kw in desc for kw in transport_keywords):
        return "TRANSPORTE"
    else:
        return "OUTROS"

categorize_udf = udf(categorize_transaction, StringType())

df_movimento_silver = df_movimento_bronze \
    .filter(col("vlr_transacao") > 0) \
    .withColumn("categoria", categorize_udf(col("des_transacao"))) \
    .dropDuplicates(["id"])

# Valida relação com cartão
df_cartao_silver = spark.read.jdbc(
    url=jdbc_url,
    table="silver.cartao",
    properties=connection_properties
)
df_movimento_silver = df_movimento_silver.join(
    df_cartao_silver,
    df_movimento_silver.id_cartao == df_cartao_silver.id,
    "inner"
).select(df_movimento_silver["*"])

# Persistência
df_movimento_silver.write.jdbc(
    url=jdbc_url,
    table="silver.movimento_staging",
    mode="overwrite",
    properties=connection_properties
)

upsert_movimento = """
    INSERT INTO silver.movimento (id, vlr_transacao, des_transacao, data_movimento, id_cartao, categoria)
    SELECT id, vlr_transacao, des_transacao, data_movimento, id_cartao, categoria FROM silver.movimento_staging
    ON CONFLICT (id)
    DO UPDATE SET
        vlr_transacao = EXCLUDED.vlr_transacao,
        des_transacao = EXCLUDED.des_transacao,
        data_movimento = EXCLUDED.data_movimento,
        id_cartao = EXCLUDED.id_cartao,
        categoria = EXCLUDED.categoria;
"""
cur.execute(upsert_movimento)
cur.execute("DROP TABLE IF EXISTS silver.movimento_staging;")

# Finalização
conn.commit()
cur.close()
conn.close()
spark.stop()
print("Silver ETL concluído com sucesso!")