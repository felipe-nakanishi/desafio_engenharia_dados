from pyspark.sql import SparkSession

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

# Leitura dos arquivos CSV com os dados brutos
df_associado = spark.read.option("header", "true").csv("data/associado.csv")
df_conta = spark.read.option("header", "true").csv("data/conta.csv")
df_cartao = spark.read.option("header", "true").csv("data/cartao.csv")
df_movimento = spark.read.option("header", "true").csv("data/movimento.csv")
# Se houver metadados das cargas, faça o mesmo:
# df_metadados = spark.read.option("header", "true").csv("data/metadados_cargas.csv")

# Carrega os dados brutos nas tabelas do schema bronze no PostgreSQL
df_associado.write.jdbc(url=jdbc_url, table="bronze.associado", mode="append", properties=connection_properties)
df_conta.write.jdbc(url=jdbc_url, table="bronze.conta", mode="append", properties=connection_properties)
df_cartao.write.jdbc(url=jdbc_url, table="bronze.cartao", mode="append", properties=connection_properties)
df_movimento.write.jdbc(url=jdbc_url, table="bronze.movimento", mode="append", properties=connection_properties)
# df_metadados.write.jdbc(url=jdbc_url, table="bronze.metadados_cargas", mode="append", properties=connection_properties)

spark.stop()
