# Imagem base com Spark e Hadoop
FROM bitnami/spark:latest

COPY jars/postgresql-42.7.3.jar /app/jars/

RUN apt-get update && apt-get install -y libxml2-dev libxslt-dev python3-lxml

# Instala dependências do Python
RUN pip install pandas psycopg2-binary lxml

# Copia os arquivos do projeto
COPY . /app
WORKDIR /app

# Comando para executar o script
CMD ["spark-submit", "--master", "local[*]", "postgresql_setup.py"]