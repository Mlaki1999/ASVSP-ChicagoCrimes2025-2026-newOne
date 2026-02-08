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

echo ""
echo "Step 7: Test Kafka readiness..."
echo "Waiting for Kafka to be fully ready..."
KAFKA_READY=false
for i in {1..4}; do
    echo "Attempt $i/15: Testing Kafka..."
    # Test both brokers with correct internal ports
    if docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092 2>/dev/null && \
       docker exec kafka2 kafka-topics --list --bootstrap-server kafka2:29092 2>/dev/null; then
        echo "✅ Both Kafka brokers are ready"
        KAFKA_READY=true
        break
    else
        echo "⏳ Kafka not ready, testing individual brokers..."
        if docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092 2>/dev/null; then
            echo "  ✓ Kafka1 (19092) is responding"
        else
            echo "  ✗ Kafka1 (19092) not responding"
        fi
        if docker exec kafka2 kafka-topics --list --bootstrap-server kafka2:29092 2>/dev/null; then
            echo "  ✓ Kafka2 (29092) is responding"
        else
            echo "  ✗ Kafka2 (29092) not responding"
        fi
        echo "  Waiting 8s before retry..."
        sleep 8
    fi
done

if [ "$KAFKA_READY" = false ]; then
    echo "❌ Kafka failed to start properly. Checking logs..."
    echo ""
    echo "=== Checking Zookeeper Status ==="
    if docker exec zoo1 nc -zv zoo1 2181 2>/dev/null; then
        echo "✅ Zookeeper is responding"
    else
        echo "❌ Zookeeper is not responding - this may be the root cause"
    fi
    echo ""
    echo "=== Kafka1 logs ==="
    docker logs kafka1 --tail 30
    echo ""
    echo "=== Kafka2 logs ==="
    docker logs kafka2 --tail 30
    echo ""
    echo "=== Zookeeper logs ==="
    docker logs zoo1 --tail 20
    exit 1
fi

echo ""
echo "Step 8: Recreate Kafka topic to avoid offset issues..."
echo "Deleting existing topic (if exists)..."
docker exec kafka1 kafka-topics --delete --topic chicagocrimes --bootstrap-server kafka1:19092 2>/dev/null || echo "Topic didn't exist or couldn't be deleted"
sleep 5
echo "Creating fresh Kafka topic..."
docker exec kafka1 kafka-topics --create --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 --partitions 3 --replication-factor 2 --if-not-exists --config cleanup.policy=delete --config retention.ms=86400000 --config segment.ms=3600000

echo ""
echo "Step 9: Verify topic creation..."
TOPIC_CREATED=false
for i in {1..10}; do
    if docker exec kafka1 kafka-topics --describe --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 2>/dev/null; then
        echo "✅ Topic 'chicagocrimes' created and accessible"
        TOPIC_CREATED=true
        break
    else
        echo "⏳ Waiting for topic creation (attempt $i/10)..."
        sleep 3
    fi
done

if [ "$TOPIC_CREATED" = false ]; then
    echo "❌ Failed to create or access topic"
    exit 1
fi

echo ""
echo "Step 10: Stop any running streaming jobs and clean checkpoints..."
echo "Stopping Spark streaming applications..."
docker exec spark-master bash -c 'ps aux | grep "consumer.py\|stream_jobs" | grep -v grep | awk "{print \$2}" | xargs -r kill -9' 2>/dev/null || true
sleep 10
echo "Cleaning all checkpoint locations..."
docker exec spark-master rm -rf /tmp/checkpoint* 2>/dev/null || true
docker exec spark-master rm -rf /tmp/spark-streaming-* 2>/dev/null || true  
docker exec spark-master rm -rf /opt/bitnami/spark/work-dir/checkpoint* 2>/dev/null || true
echo "Streaming processes stopped and all checkpoints cleaned"

echo ""
echo "Step 11: Setup stream processing files..."
docker exec spark-master mkdir -p /home/streaming/consumer
docker cp ./consumer/consumer.py spark-master:/home/streaming/consumer/consumer.py
docker cp ./run/stream_jobs.sh spark-master:./stream_jobs.sh

# echo ""
# echo "Step 12: Copy connectivity test script..."
# docker cp ./test_kafka_connectivity.py spark-master:/home/test_kafka_connectivity.py

# echo ""
# echo "Step 13: Run comprehensive Kafka connectivity test..."
# echo "Running Kafka functionality test..."
# if docker exec spark-master python3 /home/test_kafka_connectivity.py; then
#     echo "✅ Kafka connectivity test passed - system is ready"
# else
#     echo "❌ Kafka connectivity test failed"
#     echo "🔍 Checking Kafka broker status..."
#     docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null || echo "Kafka1 not responding to API requests"
#     docker exec kafka2 kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null || echo "Kafka2 not responding to API requests"
#     exit 1
# fi

echo ""
echo "Step 14: Setup PostgreSQL JDBC driver..."
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
echo "Step 15: Final verification..."
echo "Checking topic details one more time:"
docker exec kafka1 kafka-topics --describe --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 || echo "⚠️  Topic description failed (may be normal due to timeouts)"

echo ""
echo "Step 16: Final connectivity test..."
CONNECTIVITY_OK=false
for i in {1..5}; do
    if docker exec spark-master nc -zv kafka1 19092 2>/dev/null && docker exec spark-master nc -zv kafka2 29092 2>/dev/null; then
        echo "✅ Connectivity verified - Ready to start stream processing"
        CONNECTIVITY_OK=true
        break
    else
        echo "⏳ Testing connectivity (attempt $i/5)..."
        # Test individual brokers
        if docker exec spark-master nc -zv kafka1 19092 2>/dev/null; then
            echo "  ✓ kafka1:19092 reachable"
        else
            echo "  ✗ kafka1:19092 not reachable"
        fi
        if docker exec spark-master nc -zv kafka2 29092 2>/dev/null; then
            echo "  ✓ kafka2:29092 reachable"
        else
            echo "  ✗ kafka2:29092 not reachable"
        fi
        sleep 5
    fi
done

if [ "$CONNECTIVITY_OK" = true ]; then
    echo ""
    echo "🚀 Starting stream processing..."
    docker exec -i spark-master bash ./stream_jobs.sh
else
    echo "❌ Connectivity issues persist. Manual troubleshooting needed."
    echo ""
    echo "🐛 Debug information:"
    echo "=== Spark Master Network ==="
    docker exec spark-master ip route
    echo "=== Kafka1 Status ==="
    docker exec kafka1 netstat -tlnp | grep 19092
    echo "=== Kafka1 Recent Logs ==="
    docker logs kafka1 --tail 20
    echo "=== Kafka2 Recent Logs ==="
    docker logs kafka2 --tail 20
    exit 1
fi