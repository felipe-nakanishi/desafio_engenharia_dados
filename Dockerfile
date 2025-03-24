# Imagem base com Spark e Hadoop
FROM bitnami/spark:latest

# Instala dependências do Python
RUN pip install pandas psycopg2-binary

# Copia os arquivos do projeto
COPY . /app
WORKDIR /app

# Comando para executar o script
CMD ["spark-submit", "--master", "local[*]", "postgresql_setup.py"]