from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import to_date,col
import psycopg2
import pandas as pd

# conexao usando o psycopg2 ao PostgreSQL:
conn = psycopg2.connect(dbname="si_cooperative_cartoes",user="admin",password="admin",host="postgres",port="5432")
cur = conn.cursor()

# Configuração da conexão com o PostgreSQL spark:
jdbc_url = "jdbc:postgresql://postgres:5432/si_cooperative_cartoes"
connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Cria a SparkSession
spark = SparkSession.builder.appName("ETL Bronze").config("spark.jars", "jars/postgresql-42.7.3.jar").config(
    "spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.executor.extraClassPath", "jars/postgresql-42.7.3.jar"
                                                                             ).config("spark.driver.extraClassPath", "jars/postgresql-42.7.3.jar").getOrCreate()


# Leitura do arquivo bruto de associado
schema_associado = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('nome', StringType(), nullable=True),
    StructField('sobrenome', StringType(), nullable=True),
    StructField('idade', IntegerType(), nullable=True),
    StructField('email', StringType(), nullable=True)
])
df_associado = spark.read.option('header', True).schema(schema_associado).csv('/app/data/associado.csv')

# Leitura do arquivo bruto de conta:
schema_conta = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('tipo', StringType(), nullable=True),
    StructField('data_criacao', StringType(), nullable=True),
    StructField('id_associado', IntegerType(), nullable=True)
])
df_conta = pd.read_xml('/app/data/conta.xml',parser = 'etree')
print(df_conta.head(4))
df_conta = spark.createDataFrame(df_conta, schema=schema_conta)
df_conta = df_conta.withColumn("data_criacao", col("data_criacao").cast(TimestampType()))
#df_conta = spark.read.schema(schema_conta).option('rowTag', 'row').format('com.databricks.spark.xml').load('/app/data/conta.xml')
#df_conta = df_conta.withColumn('data_criacao', to_date(df_conta.data_criacao, 'yyyy-MM-dd HH:mm:ss'))


# Leitura do arquivo bruto de cartao:
schema_cartao = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('num_cartao', LongType(), nullable=True),
    StructField('nom_impresso', StringType(), nullable=True),
    StructField('id_conta', IntegerType(), nullable=True),
    StructField('id_associado', IntegerType(), nullable=True)
])
df_cartao = spark.read.option('header', 'true').schema(schema_cartao).option('multiLine', True).json('/app/data/cartao.json')

# Leitura do arquivo de movimento:
schema_movimento = StructType([
    StructField('id', LongType(), nullable=False),
    StructField('vlr_transacao', DoubleType(), nullable=True),
    StructField('des_transacao', StringType(), nullable=True),
    StructField('data_movimento', StringType(), nullable=True),
    StructField('id_cartao', LongType(), nullable=True)
])
df_movimento = spark.read.option('header', 'true').schema(schema_movimento).parquet('/app/data/movimento.parquet')
df_movimento = df_movimento.withColumn('data_movimento', to_date(df_movimento.data_movimento, 'dd/MM/yyyy HH:mm:ss'))

# Criação do dataframe de metadados:
data = [
    ('associado.csv', "bronze.associado"),
    ('conta.xml', "bronze.conta"),
    ('cartao.json', "bronze.cartao"),
    ('movimento.parquet', "bronze.movimento")
]

df_metadados = spark.createDataFrame(data, ['nome_arquivo','tabela_carga'])

# Ingestão dos dados nas tabelas da camada bronze:

df_associado.write.jdbc(url=jdbc_url, table="bronze.associado_staging", mode="overwrite", properties=connection_properties)

upsert_associado = """
INSERT INTO bronze.associado (id, nome, sobrenome, idade, email)
SELECT id, nome, sobrenome, idade, email FROM bronze.associado_staging
ON CONFLICT ON CONSTRAINT associado_pkey
DO UPDATE SET 
    nome = EXCLUDED.nome,
    sobrenome = EXCLUDED.sobrenome,
    idade = EXCLUDED.idade,
    email = EXCLUDED.email;
"""
cur.execute(upsert_associado)
conn.commit()

cur.execute("DROP TABLE IF EXISTS bronze.associado_staging;")
conn.commit()
# Ingestão da tabela CONTA, usando tabela staging e depois realizando upsert:

df_conta.write.jdbc(url=jdbc_url, table="bronze.conta_staging", mode="overwrite", properties=connection_properties)

upsert_conta = """
INSERT INTO bronze.conta (id, tipo, data_criacao, id_associado)
SELECT id, tipo, data_criacao, id_associado FROM bronze.conta_staging
ON CONFLICT (id)
DO UPDATE SET 
    tipo = EXCLUDED.tipo,
    data_criacao = EXCLUDED.data_criacao,
    id_associado = EXCLUDED.id_associado;
"""
cur.execute(upsert_conta)
conn.commit()

cur.execute("DROP TABLE IF EXISTS bronze.conta_staging;")
conn.commit()

# Ingestão da tabela CARTAO, usando tabela staging e depois realizando upsert:

df_cartao.write.jdbc(url=jdbc_url, table="bronze.cartao_staging", mode="overwrite", properties=connection_properties)

upsert_cartao = """
INSERT INTO bronze.cartao (id, num_cartao, nom_impresso, id_conta, id_associado)
SELECT id, num_cartao, nom_impresso, id_conta, id_associado FROM bronze.cartao_staging
ON CONFLICT (id)
DO UPDATE SET 
    num_cartao = EXCLUDED.num_cartao,
    nom_impresso = EXCLUDED.nom_impresso,
    id_conta = EXCLUDED.id_conta,
    id_associado = EXCLUDED.id_associado;
"""
cur.execute(upsert_cartao)
conn.commit()

cur.execute("DROP TABLE IF EXISTS bronze.cartao_staging;")
conn.commit()

# Ingestão da tabela MOVIMENTO, usando tabela staging e depois realizando upsert:

df_movimento.write.jdbc(url=jdbc_url, table="bronze.movimento_staging", mode="overwrite", properties=connection_properties)

upsert_cartao = """
INSERT INTO bronze.movimento (id, vlr_transacao, des_transacao, data_movimento, id_cartao)
SELECT id, vlr_transacao, des_transacao, data_movimento, id_cartao FROM bronze.movimento_staging
ON CONFLICT (id)
DO UPDATE SET 
    vlr_transacao = EXCLUDED.vlr_transacao,
    des_transacao = EXCLUDED.des_transacao,
    data_movimento = EXCLUDED.data_movimento,
    id_cartao = EXCLUDED.id_cartao;
"""
cur.execute(upsert_cartao)
conn.commit()

cur.execute("DROP TABLE IF EXISTS bronze.movimento_staging;")
conn.commit()

df_metadados.write.jdbc(url=jdbc_url, table="bronze.metadados_cargas", mode="append", properties=connection_properties)

print('success')
cur.close()
conn.close()
spark.stop()
