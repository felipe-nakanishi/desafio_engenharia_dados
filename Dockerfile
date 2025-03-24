# Imagem base com Spark e Hadoop
FROM bitnami/spark:3.5.1

# Instala dependências do Python
RUN pip install pandas psycopg2-binary

# Copia os arquivos do projeto
COPY . /app
WORKDIR /app

# Comando para executar o script
CMD ["spark-submit", "--master", "local[*]", "postgresql_setup.py"]