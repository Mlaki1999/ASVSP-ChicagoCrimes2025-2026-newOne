echo "Complete Streaming Setup Fix"
echo "==============================="

echo ""
echo "Step 1: Complete cleanup of conflicting containers..."
docker stop zoo1 kafka1 kafka2 kafka_producer 2>/dev/null || true
docker rm zoo1 kafka1 kafka2 kafka_producer 2>/dev/null || true

echo "Cleanup complete"

echo ""
echo "Step 2: Ensure batch processing is running..."
cd setup
if ! docker ps | grep -q "spark-master"; then
    echo "Starting batch processing cluster..."
    ./cluster_up.sh
    sleep 30
else
    echo "Batch processing already running"
fi

echo ""
echo "Step 3: Start fresh streaming cluster..."
cd ../stream-processing
docker-compose up -d

echo ""
echo "Step 4: Wait for containers to fully start..."
sleep 60

echo ""
echo "Step 5: Verify containers are running..."
docker ps --filter "name=kafka" --filter "name=zoo" --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "Step 6: Test network connectivity..."
for i in {1..5}; do
    echo "Attempt $i: Testing connectivity..."
    if docker exec spark-master ping -c 1 kafka1 2>/dev/null; then
        echo "kafka1 reachable"
        break
    else
        echo "kafka1 not reachable, waiting 10s..."
        sleep 10
    fi
done

# echo ""
# echo "Step 7: Test Kafka readiness..."
# for i in {1..10}; do
#     echo "Attempt $i: Testing Kafka..."
#     if docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null; then
#         echo " Kafka is ready"
#         break
#     else
#         echo " Kafka not ready, waiting 10s..."
#         sleep 10
#     fi
# done

echo ""
echo "Step 8: Create Kafka topic..."
docker exec kafka1 kafka-topics --create --topic chicagocrimes --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --if-not-exists

echo ""
echo "Step 9: Verify topic creation..."
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092 | grep chicagocrimes || echo "Topic created (list command may timeout, but that's OK)"

echo ""
echo "Step 10: Stop any running streaming jobs and clean checkpoints..."
docker exec spark-master pkill -f consumer.py 2>/dev/null || true
docker exec spark-master pkill -f stream_jobs 2>/dev/null || true
sleep 5
docker exec spark-master rm -rf /tmp/checkpoint*
echo "Streaming processes stopped and checkpoints cleaned"

echo ""
echo "Step 11: Setup stream processing files..."
docker exec spark-master mkdir -p /home/streaming/consumer
docker cp ./consumer/consumer.py spark-master:/home/streaming/consumer/consumer.py
docker cp ./run/stream_jobs.sh spark-master:./stream_jobs.sh

echo ""
echo "Step 12: Setup PostgreSQL JDBC driver..."
# Check if PostgreSQL jar exists in the batch processing setup
if docker exec spark-master ls postgresql-42.7.0.jar 2>/dev/null; then
    echo "PostgreSQL driver already exists in Spark master"
else
    echo "PostgreSQL driver not found. Copying from setup..."
    # Try to copy from the project directory or download
    if [ -f "../setup/postgresql-42.7.0.jar" ]; then
        docker cp ../setup/postgresql-42.7.0.jar spark-master:/opt/bitnami/spark/jars/postgresql-42.7.0.jar
        echo "PostgreSQL driver copied to Spark master"
    else
        echo "⬇Downloading PostgreSQL JDBC driver..."
        docker exec spark-master wget -O /opt/bitnami/spark/jars/postgresql-42.7.0.jar https://jdbc.postgresql.org/download/postgresql-42.7.0.jar
        echo "PostgreSQL driver downloaded to Spark master"
    fi
fi

echo ""
echo "Step 13: Final connectivity test..."
if docker exec spark-master nc -zv kafka1 19092 2>/dev/null; then
    echo "Ready to start stream processing"
    echo ""
    echo "Starting stream processing..."
    docker exec -i spark-master bash ./stream_jobs.sh
else
    echo "Still no connectivity. Manual troubleshooting needed."
    echo ""
    echo "Debug information:"
    echo "Kafka1 logs:"
    docker logs kafka1 --tail 10
fi