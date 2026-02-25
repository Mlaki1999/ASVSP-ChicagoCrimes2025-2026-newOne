#!/bin/bash
# Get script directory for absolute paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Complete Streaming Setup Fix"
echo "==============================="

echo ""
echo "Step 1: Complete cleanup of conflicting containers..."
docker stop zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true
docker rm zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true

echo "Cleanup complete"

echo ""
echo "Step 2: Ensure batch processing is running..."
if ! docker ps | grep -q "spark-master"; then
    echo "Starting batch processing cluster..."
    
    # Create network if not exists
    docker network create big_data_network 2>/dev/null || echo "Network already exists"
    sleep 2
    
    # Start batch processing containers
    cd "$SCRIPT_DIR/batch_processing"
    docker-compose up -d
    sleep 5
    
    # Check if data file exists and copy it
    DATA_FILE="$SCRIPT_DIR/data/chicago_crimes.csv"
    if [ -f "$DATA_FILE" ]; then
        echo "Copying data to namenode..."
        MSYS_NO_PATHCONV=1 docker cp "$DATA_FILE" namenode:/batch_data.csv
        sleep 2
    fi
    
    # Wait for HDFS to leave safe mode
    echo "Waiting for HDFS to be ready..."
    MAX_RETRIES=30
    RETRIES=0
    while [ $RETRIES -lt $MAX_RETRIES ]; do
        if ! docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is ON"; then
            echo "HDFS ready"
            break
        fi
        echo "Waiting for HDFS... ($RETRIES/$MAX_RETRIES)"
        sleep 10
        RETRIES=$((RETRIES + 1))
    done
    
    # Create HDFS directories and upload data if data file exists
    if [ -f "$DATA_FILE" ]; then
        echo "Uploading data to HDFS..."
        MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/raw
        MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/transform
        MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -copyFromLocal -f /batch_data.csv /user/root/data-lake/raw/batch_data.csv
        sleep 2
    fi
    
    # Copy PostgreSQL JAR
    JAR_FILE="$SCRIPT_DIR/data/postgresql-42.7.0.jar"
    if [ -f "$JAR_FILE" ]; then
        echo "Copying PostgreSQL JAR..."
        MSYS_NO_PATHCONV=1 docker cp "$JAR_FILE" spark-master:/opt/bitnami/spark/jars/postgresql-42.7.0.jar 2>/dev/null || true
        MSYS_NO_PATHCONV=1 docker cp "$JAR_FILE" spark-master:./postgresql-42.7.0.jar 2>/dev/null || true
    fi
    
    echo "Batch processing cluster started"
    sleep 10
else
    echo "Batch processing already running"
fi

echo ""
echo "Step 3: Start fresh streaming cluster..."
cd "$SCRIPT_DIR/stream-processing"
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
        echo " Both Kafka brokers are ready"
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
    echo " Kafka failed to start properly. Checking logs..."
    echo ""
    echo "=== Checking Zookeeper Status ==="
    if docker exec zoo1 nc -zv zoo1 2181 2>/dev/null; then
        echo " Zookeeper is responding"
    else
        echo " Zookeeper is not responding - this may be the root cause"
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
docker exec kafka1 kafka-topics --create --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 --partitions 3 --replication-factor 1 --if-not-exists --config cleanup.policy=delete --config retention.ms=86400000 --config segment.ms=3600000

echo ""
echo "Step 9: Verify topic creation..."
TOPIC_CREATED=false
for i in {1..10}; do
    if docker exec kafka1 kafka-topics --describe --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 2>/dev/null; then
        echo " Topic 'chicagocrimes' created and accessible"
        TOPIC_CREATED=true
        break
    else
        echo " Waiting for topic creation (attempt $i/10)..."
        sleep 3
    fi
done

if [ "$TOPIC_CREATED" = false ]; then
    echo " Failed to create or access topic"
    exit 1
fi

echo ""
echo "Step 10: Stop any running streaming jobs and clean checkpoints..."
echo "Stopping any conflicting containers..."
docker stop stream_consumer 2>/dev/null || true
docker rm stream_consumer 2>/dev/null || true
echo "Streaming processes stopped"

echo ""
echo ""
echo "Step 11: Start streaming consumer..."
echo "Starting the stream consumer service..."
docker-compose up -d consumer
sleep 30

echo "Verifying consumer startup..."
if docker ps | grep -q "stream_consumer"; then
    echo " Stream consumer started successfully"
else
    echo " Stream consumer failed to start"
    docker logs stream_consumer --tail 20
    exit 1
fi

echo "Step 12: Monitor consumer startup..."
echo "Checking consumer logs for MongoDB connection..."
sleep 15
echo "Recent consumer logs:"
docker logs stream_consumer --tail 50

echo ""
echo "Step 13: Check MongoDB connectivity..."
echo "Testing MongoDB connection from consumer..."
sleep 5
if docker exec mongodb mongosh -u root -p mongodb123 --eval "db.adminCommand('ismaster')" > /dev/null 2>&1; then
    echo " MongoDB is accessible"
else
    echo " MongoDB connection failed"
    docker logs mongodb --tail 20
    exit 1
fi

echo ""
echo "Step 14: Final verification..."
echo "Checking topic details one more time:"
docker exec kafka1 kafka-topics --describe --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 || echo "⚠️  Topic description failed (may be normal due to timeouts)"

echo ""
echo "Step 15: Final connectivity test..."
echo "Waiting for consumer to fully initialize (installing dependencies)..."
sleep 30

echo "Checking if consumer application has started..."
for i in {1..10}; do
    echo "Checking consumer startup progress (attempt $i/10)..."
    
    # Check if the consumer logs show successful startup
    if docker logs stream_consumer 2>&1 | grep -q "Connected to MongoDB\|Starting streaming session\|Started successfully"; then
        echo " Consumer application started successfully"
        CONNECTIVITY_OK=true
        break
    elif docker logs stream_consumer 2>&1 | grep -q "Error\|Failed\|Exception"; then
        echo " Consumer application encountered errors"
        echo "Recent consumer logs:"
        docker logs stream_consumer --tail 20
        CONNECTIVITY_OK=false
        break
    else
        echo " Consumer still initializing... (downloading dependencies)"
        sleep 15
    fi
done

# If we didn't get a clear success/failure, check if it's still installing
if [ -z "$CONNECTIVITY_OK" ]; then
    echo "Consumer is still starting up. Checking if it can reach services..."
    
    # Test using Python instead of nc
    SERVICES_OK=false
    if docker exec stream_consumer python3 -c "
import socket
def test_port(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

kafka_ok = test_port('kafka1', 19092) and test_port('kafka2', 29092)
mongo_ok = test_port('mongodb', 27017) 
spark_ok = test_port('spark-master', 7077)

print(f'Kafka: {kafka_ok}, MongoDB: {mongo_ok}, Spark: {spark_ok}')
if kafka_ok and mongo_ok and spark_ok:
    exit(0)
else:
    exit(1)
" 2>/dev/null; then
        echo " All services are reachable from consumer"
        CONNECTIVITY_OK=true
    else
        echo " Some services not reachable from consumer"
        CONNECTIVITY_OK=false
    fi
fi

if [ "$CONNECTIVITY_OK" = true ]; then
    echo ""
    echo " STREAMING PIPELINE READY!"
    echo " Access Points:"
    echo "  • Kafka UI:     http://localhost:8091"
    echo "  • Mongo Express: http://localhost:8083 (admin/admin123)" 
    echo "  • MongoDB:      mongodb://root:mongodb123@localhost:27018/chicago_crimes"
    echo ""
    echo "To monitor:"
    echo "  docker logs kafka_producer -f      # Producer logs"
    echo "  docker logs stream_consumer -f     # Consumer logs"
    echo "  docker logs mongodb -f             # MongoDB logs"
    echo ""
    echo "Data will be stored in MongoDB collections:"
    echo "  • stream_crime_hotspots"
    echo "  • stream_pattern_analysis" 
    echo "  • stream_violence_escalation"
    echo "  • stream_domestic_correlation"
    echo "  • stream_temporal_patterns"
else
    echo " Connectivity issues persist. Manual troubleshooting needed."
    echo ""
    echo " Debug information:"
    echo "=== Consumer Container Network ==="
    docker exec stream_consumer ip route 2>/dev/null || echo "Cannot access consumer network info"
    echo "=== Consumer Logs (recent) ==="
    docker logs stream_consumer --tail 30
    echo "=== Kafka1 Status ==="
    docker exec kafka1 netstat -tlnp | grep 19092 2>/dev/null || echo "Cannot access kafka1 network info"
    echo "=== MongoDB Status ==="
    docker exec mongodb netstat -tlnp | grep 27017 2>/dev/null || echo "Cannot access mongodb network info"
    echo "=== All Container Status ==="
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    exit 1
fi