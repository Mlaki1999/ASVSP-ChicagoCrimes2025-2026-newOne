# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/streaming/consumer/consumer.py
./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --driver-class-path /opt/bitnami/spark/jars/postgresql-42.7.0.jar --jars /opt/bitnami/spark/jars/postgresql-42.7.0.jar /home/streaming/consumer/consumer.py
