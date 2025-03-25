# Imagem base com Spark e Hadoop
FROM bitnami/spark:latest

# Cria os diretórios no container
WORKDIR /app

# Copia o driver JDBC para a nova estrutura
COPY jars/postgresql-42.7.3.jar /app/jars/

# Instala dependências necessárias
#RUN apt-get update && apt-get install -y libxml2-dev libxslt-dev python3-lxml

# Instala pacotes do Python
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copia os arquivos do projeto para a estrutura reorganizada
COPY scripts/ /app/scripts/
COPY data/ /app/data/
COPY jars/ /app/jars/

# Define o diretório de trabalho para os scripts
WORKDIR /app

# Comando para executar o script de setup do banco de dados
CMD ["spark-submit", "--master", "local[*]", "scripts/postgresql_setup.py"]