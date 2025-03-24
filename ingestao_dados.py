from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import to_date

# Configuração da conexão com o PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/si_cooperative_cartoes"
connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Cria a SparkSession
spark = SparkSession.builder \
    .appName("ETL Bronze") \
    .getOrCreate()


# Leitura do arquivo bruto de associado
schema_associado = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('nome', StringType(), nullable=True),
    StructField('sobrenome', StringType(), nullable=True),
    StructField('idade', IntegerType(), nullable=True),
    StructField('email', StringType(), nullable=True)
])
df_associado = spark.read.option("header", True).schema(schema_associado).csv('data/associado.csv')

print(df_associado.show(2))
# Leitura do arquivo bruto de conta:
schema_conta = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('tipo', StringType(), nullable=True),
    StructField('data_criacao', StringType(), nullable=True),
    StructField('id_associado', IntegerType(), nullable=True)
])
df_conta = spark.read.option("header", "true").schema(schema_conta).option("rowTag", "data").format("com.databricks.spark.xml").load("data/conta.xml")
df_conta = df_conta.withColumn("data_criacao", to_date(df_conta.data_criacao, "yyyy-MM-dd HH:MM:SS"))
print(df_conta.show(2))
# Leitura do arquivo bruto de cartao:
schema_cartao = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('num_cartao', StringType(), nullable=True),
    StructField('nom_impresso', StringType(), nullable=True),
    StructField('id_conta', IntegerType(), nullable=True),
    StructField('id_associado', IntegerType(), nullable=True)
])
df_cartao = spark.read.option("header", "true").schema(schema_cartao).json("data/cartao.json")
print(df_cartao.show(2))
# Leitura do arquivo de movimento:
schema_movimento = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('vlr_transacao', DoubleType(), nullable=True),
    StructField('des_transacao', StringType(), nullable=True),
    StructField('data_movimento', StringType(), nullable=True),
    StructField('id_cartao', IntegerType(), nullable=True)
])
df_movimento = spark.read.option("header", "true").schema(schema_movimento).parquet("data/movimento.parquet")
df_movimento = df_movimento.withColumn("data_criacao", to_date(df_movimento.data_movimento, "dd/MM/yyyy HH:MM:SS"))

print(df_movimento.show(2))

# Carrega os dados brutos nas tabelas do schema bronze no PostgreSQL
#df_associado.write.jdbc(url=jdbc_url, table="bronze.associado", mode="append", properties=connection_properties)
#df_conta.write.jdbc(url=jdbc_url, table="bronze.conta", mode="append", properties=connection_properties)
#df_cartao.write.jdbc(url=jdbc_url, table="bronze.cartao", mode="append", properties=connection_properties)
#df_movimento.write.jdbc(url=jdbc_url, table="bronze.movimento", mode="append", properties=connection_properties)
# df_metadados.write.jdbc(url=jdbc_url, table="bronze.metadados_cargas", mode="append", properties=connection_properties)

spark.stop()
