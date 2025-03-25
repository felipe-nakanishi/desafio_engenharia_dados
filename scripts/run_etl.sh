/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/scripts/postgresql_setup.py

# Executar a ingestao de dados em spark;
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/scripts/ingestao_dados.py

sleep 30
  # Executar a ingestao de dados em spark;
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/scripts/etl_dados_spark.py


sleep 30
    # Executar a ingestao de dados em spark;
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/scripts/exportar_arquivo_final.py