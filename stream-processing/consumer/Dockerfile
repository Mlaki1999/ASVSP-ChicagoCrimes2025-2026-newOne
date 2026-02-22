FROM apache/spark:3.4.0-scala2.12-java11-python3-ubuntu

USER root

# Install required Python packages
RUN pip3 install --no-cache-dir \
    pymongo==4.6.1 \
    pandas==1.5.3

# Set up PySpark environment
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /opt/consumer

# Copy consumer script
COPY consumer.py .

# Set environment variables
ENV SPARK_MASTER=spark://spark-master:7077

# Run the consumer
CMD ["python3", "consumer.py"]