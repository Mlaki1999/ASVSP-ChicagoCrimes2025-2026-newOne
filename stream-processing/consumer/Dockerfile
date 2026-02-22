FROM apache/spark:3.4.0-scala2.12-java11-python3-ubuntu

USER root

# Update and install system packages
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    netcat \
    iproute2 \
    iputils-ping \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
RUN pip3 install --no-cache-dir \
    pymongo==4.6.1 \
    pandas==1.5.3

# -------------------------------------------------------
# ✅ FIX: Create writable Ivy cache for Spark user
# -------------------------------------------------------
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

# -------------------------------------------------------
# Download ALL required Kafka connector JARs
# -------------------------------------------------------
# Main Kafka connector
RUN wget --progress=dot:giga -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar

# Kafka clients
RUN wget --progress=dot:giga -O /opt/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Token provider (contains KafkaConfigUpdater)
RUN wget --progress=dot:giga -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar

# Commons Pool2 (dependency)
RUN wget --progress=dot:giga -O /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Verify all JARs are present
RUN ls -lh /opt/spark/jars/ | grep -E "(spark-sql-kafka|kafka-clients|token-provider|commons-pool)"

# -------------------------------------------------------
# Spark environment
# -------------------------------------------------------
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /opt/consumer

# Copy consumer script and entrypoint
COPY consumer.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Switch back to spark user (important)
USER spark

# Run the consumer with entrypoint for proper startup sequencing
ENTRYPOINT ["./entrypoint.sh"]