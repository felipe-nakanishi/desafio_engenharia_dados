#!/bin/sh
# Run the first Spark job
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/postgresql_setup.py

# Check if the first job succeeded
if [ $? -ne 0 ]; then
  echo "postgresql_setup.py failed"
  exit 1
fi

# Run the second Spark job
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --jars /app/jars/postgresql-42.7.3.jar \
  /app/ingestao_dados.py