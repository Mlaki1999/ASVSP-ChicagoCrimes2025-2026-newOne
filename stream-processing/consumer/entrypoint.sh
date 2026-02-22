#!/bin/bash
set -e

echo " Starting consumer entrypoint script..."

# Wait for Kafka brokers
echo " Waiting for Kafka brokers..."
for i in {1..30}; do
    if nc -zv kafka1 19092 2>&1 && nc -zv kafka2 29092 2>&1; then
        echo " Kafka brokers are reachable!"
        break
    else
        echo "  Kafka not ready (attempt $i/30), waiting 5s..."
        sleep 5
    fi
done

# Wait for MongoDB
echo " Waiting for MongoDB..."
for i in {1..30}; do
    if nc -zv mongodb 27017 2>&1; then
        echo " MongoDB is reachable!"
        break
    else
        echo "  MongoDB not ready (attempt $i/30), waiting 3s..."
        sleep 3
    fi
done

# Wait for Spark Master
echo " Waiting for Spark Master..."
SPARK_READY=false
for i in {1..60}; do
    if nc -zv spark-master 7077 2>&1; then
        echo " Spark master is reachable at spark-master:7077!"
        SPARK_READY=true
        break
    else
        echo "  Spark master not reachable (attempt $i/60), waiting 5s..."
        sleep 5
    fi
done

if [ "$SPARK_READY" = false ]; then
    echo " Spark master not reachable after 5 minutes. Trying to continue anyway..."
fi

# Check if we can resolve spark-master hostname
echo " Testing DNS resolution for spark-master..."
if getent hosts spark-master; then
    echo " spark-master hostname resolves"
else
    echo " Cannot resolve spark-master hostname"
    echo "Available hosts:"
    cat /etc/hosts
fi

echo " Starting consumer application..."
exec python3 /opt/consumer/consumer.py
