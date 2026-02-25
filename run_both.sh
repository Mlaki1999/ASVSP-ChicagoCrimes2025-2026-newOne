# #!/bin/bash
# # ================================================================
# # Chicago Crimes Analytics Pipeline - Unified Script
# # ================================================================
# # Handles everything:
# # - Network creation
# # - Cluster startup (HDFS, Spark, PostgreSQL)
# # - Data upload to HDFS
# # - Batch processing execution (inline Spark jobs)
# # - Stream processing setup and execution
# # - Cluster shutdown
# #
# # NO OTHER SCRIPTS ARE CALLED - Everything is inlined!
# # ================================================================

# set -e  # Exit on error

# # Color output
# RED='\033[0;31m'
# GREEN='\033[0;32m'
# YELLOW='\033[1;33m'
# CYAN='\033[0;36m'
# NC='\033[0m' # No Color

# # ============================
# # Helper functions
# # ============================
# print_step() { echo -e "\n${CYAN}===> $1${NC}"; }
# print_success() { echo -e "${GREEN}✓ $1${NC}"; }
# print_error() { echo -e "${RED}✗ $1${NC}"; }
# print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }

# # ============================
# # Parse arguments
# # ============================
# SHUTDOWN=false
# SKIP_BATCH=false
# SKIP_STREAM=false
# DELETE_VOLUMES=false

# while [[ "$#" -gt 0 ]]; do
#     case $1 in
#         --down) SHUTDOWN=true ;;
#         --skip-batch) SKIP_BATCH=true ;;
#         --skip-stream) SKIP_STREAM=true ;;
#         --delete-volumes) DELETE_VOLUMES=true ;;
#         -h|--help)
#             echo "Usage: $0 [OPTIONS]"
#             echo "Options:"
#             echo "  --down              Shutdown all services"
#             echo "  --skip-batch        Skip batch processing"
#             echo "  --skip-stream       Skip stream processing"
#             echo "  --delete-volumes    Delete volumes when shutting down"
#             echo "  -h, --help          Show this help message"
#             exit 0
#             ;;
#         *) echo "Unknown parameter: $1"; exit 1 ;;
#     esac
#     shift
# done

# # ============================
# # Script directory
# # ============================
# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# cd "$SCRIPT_DIR"

# # ============================
# # Shutdown function
# # ============================
# shutdown_cluster() {
#     print_step "Shutting down cluster..."

#     local delete_vols=$DELETE_VOLUMES
#     if [ "$DELETE_VOLUMES" = false ]; then
#         read -p "Delete volumes? [y/N] " -n 1 -r
#         echo
#         [[ $REPLY =~ ^[Yy]$ ]] && delete_vols=true
#     fi

#     # Stop stream processing
#     print_step "Stopping stream processing..."
#     cd "$SCRIPT_DIR/stream-processing"
#     if [ -f "docker-compose.yml" ]; then
#         if [ "$delete_vols" = true ]; then docker-compose down -v
#         else docker-compose down; fi
#     fi

#     # Stop batch processing
#     print_step "Stopping batch processing..."
#     cd "$SCRIPT_DIR/batch_processing"
#     if [ "$delete_vols" = true ]; then docker-compose down -v
#     else docker-compose down; fi

#     # Delete network
#     sleep 3
#     print_step "Deleting 'big_data_network' network..."
#     docker network rm big_data_network 2>/dev/null || true

#     print_success "Cluster shut down complete."
#     exit 0
# }

# # Handle shutdown
# $SHUTDOWN && shutdown_cluster

# # ============================
# # Main header
# # ============================
# echo -e "${GREEN}"
# cat << "EOF"
# ╔══════════════════════════════════════════════════════════╗
# ║  Chicago Crimes Analytics Pipeline - Master Script      ║
# ║  Complete Batch & Stream Processing Pipeline            ║
# ╚══════════════════════════════════════════════════════════╝
# EOF
# echo -e "${NC}"

# # ============================
# # STEP 1: Network Setup
# # ============================
# print_step "Creating Docker network 'big_data_network'..."
# if docker network create big_data_network 2>/dev/null; then
#     print_success "Network created"
# else
#     print_warning "Network already exists (continuing...)"
# fi
# sleep 2

# # ============================
# # STEP 2: Batch Processing Setup
# # ============================
# if [ "$SKIP_BATCH" = false ]; then
#     echo ""
#     echo "============================================================"
#     echo -e "${YELLOW}BATCH PROCESSING PIPELINE${NC}"
#     echo "============================================================"

#     # Start batch containers
#     print_step "Starting batch processing containers..."
#     cd "$SCRIPT_DIR/batch_processing"
#     docker-compose up -d
#     sleep 5
#     print_success "Batch containers started"

#     # Data upload
#     DATA_FILE="$SCRIPT_DIR/data/chicago_crimes.csv"
#     if [ -f "$DATA_FILE" ]; then
#         print_step "Copying data to namenode..."
#         docker cp "$DATA_FILE" namenode:/batch_data.csv
#         sleep 2
#         print_success "Data copied"

#         # Wait for HDFS safe mode to clear
#         print_step "Waiting for HDFS to leave safe mode..."
#         for RETRIES in {1..30}; do
#             if ! docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is ON"; then
#                 print_success "HDFS ready"
#                 break
#             fi
#             echo -ne "  Waiting for HDFS... ($RETRIES/30)\r"
#             sleep 10
#         done

#         # Upload to HDFS
#         print_step "Uploading data to HDFS..."
#         MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/raw
#         MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/transform
#         MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -copyFromLocal -f /batch_data.csv /user/root/data-lake/raw/batch_data.csv
#         sleep 2
#         print_success "Data uploaded to HDFS"
#     else
#         print_warning "No data file found at $DATA_FILE, skipping HDFS upload."
#     fi

#     # Spark readiness & PostgreSQL JAR copy
#     print_step "Ensuring Spark master is ready for JAR copy..."
#     SPARK_READY=false
#     for i in {1..20}; do
#         if docker ps --filter "name=spark-master" --format "{{.Names}}" | grep -q "spark-master" && \
#            docker exec spark-master ls /opt/bitnami/spark/jars &>/dev/null; then
#             SPARK_READY=true
#             echo "  → Spark master is ready"
#             break
#         fi
#         echo "  → Waiting for Spark master to initialize... ($i/20)"
#         sleep 5
#     done
#     $SPARK_READY || { print_error "Spark master did not become ready"; exit 1; }

#     JAR_FILE="$SCRIPT_DIR/data/postgresql-42.7.0.jar"
#     if [ -f "$JAR_FILE" ]; then
#         print_step "Copying PostgreSQL JAR to Spark master..."
#         docker cp "$JAR_FILE" spark-master:/opt/bitnami/spark/jars/postgresql-42.7.0.jar
#         docker cp "$JAR_FILE" spark-master:./postgresql-42.7.0.jar
#         print_success "PostgreSQL JAR copied successfully"
#     else
#         print_warning "PostgreSQL JAR not found at: $JAR_FILE"
#     fi

#     # Run batch jobs
#     print_step "Running batch processing jobs..."
#     docker exec -i spark-master /spark/bin/spark-submit /home/batch/preprocessing.py
#     sleep 5
#     if docker exec -i spark-master /spark/bin/spark-submit --driver-class-path postgresql-42.7.0.jar /home/batch/processing.py; then
#         print_success "Batch processing completed successfully!"
#     else
#         print_error "Batch processing encountered errors"
#     fi
# else
#     print_warning "Skipping batch processing (--skip-batch flag set)"
# fi

# # ============================
# # STEP 3: Stream Processing Setup
# # ============================
# if [ "$SKIP_STREAM" = false ]; then
#     echo ""
#     echo "============================================================"
#     echo -e "${YELLOW}STREAM PROCESSING PIPELINE${NC}"
#     echo "============================================================"

#     # Cleanup conflicting containers
#     print_step "Cleaning up conflicting streaming containers..."
#     docker stop zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true
#     docker rm zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true
#     print_success "Cleanup complete"

#     # Start streaming cluster
#     print_step "Starting streaming cluster..."
#     cd "$SCRIPT_DIR/stream-processing"
#     docker-compose up -d
#     sleep 10
#     print_success "Streaming containers started"

#     # Wait for containers to stabilize
#     print_step "Waiting for containers to fully initialize..."
#     sleep 50

#     # Verify network connectivity
#     print_step "Testing network connectivity to Kafka..."
#     CONNECTED=false
#     for i in {1..5}; do
#         if docker exec spark-master ping -c 1 kafka1 &>/dev/null; then
#             CONNECTED=true
#             break
#         fi
#         echo "  Attempt $i/5 failed, retrying..."
#         sleep 10
#     done
#     $CONNECTED || { print_error "Network connectivity failed"; exit 1; }

#     # Kafka readiness & topic creation
#     print_step "Testing Kafka readiness..."
#     KAFKA_READY=false
#     for i in {1..15}; do
#         if docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092 &>/dev/null && \
#            docker exec kafka2 kafka-topics --list --bootstrap-server kafka2:29092 &>/dev/null; then
#             KAFKA_READY=true
#             break
#         fi
#         sleep 8
#     done
#     $KAFKA_READY || { print_error "Kafka failed to start"; docker logs kafka1 --tail 30; docker logs kafka2 --tail 30; exit 1; }

#     print_step "Creating Kafka topic 'chicagocrimes'..."
#     docker exec kafka1 kafka-topics --delete --topic chicagocrimes --bootstrap-server kafka1:19092 2>/dev/null || true
#     sleep 3
#     docker exec kafka1 kafka-topics --create \
#         --topic chicagocrimes \
#         --bootstrap-server kafka1:19092,kafka2:29092 \
#         --partitions 3 --replication-factor 1 \
#         --if-not-exists --config cleanup.policy=delete \
#         --config retention.ms=86400000 --config segment.ms=3600000
#     sleep 3

#     # Start stream consumer
#     print_step "Starting stream consumer..."
#     docker stop stream_consumer 2>/dev/null || true
#     docker rm stream_consumer 2>/dev/null || true
#     docker-compose up -d consumer
#     sleep 30
#     if docker ps --filter "name=stream_consumer" --format "{{.Names}}" | grep -q "stream_consumer"; then
#         print_success "Stream consumer started successfully"
#     else
#         print_error "Stream consumer failed to start"
#         docker logs stream_consumer --tail 20
#         exit 1
#     fi

#     # Test MongoDB
#     print_step "Testing MongoDB connectivity..."
#     sleep 5
#     if docker exec mongodb mongosh -u root -p mongodb123 --eval "db.adminCommand('ismaster')" &>/dev/null; then
#         print_success "MongoDB is accessible"
#     else
#         print_warning "MongoDB connection test failed (may be normal during initialization)"
#     fi

#     print_success "STREAMING PIPELINE READY!"
#     cd "$SCRIPT_DIR"
# else
#     print_warning "Skipping stream processing (--skip-stream flag set)"
# fi

# # ============================
# # FINAL STATUS
# # ============================
# echo ""
# echo "============================================================"
# echo -e "${GREEN}PIPELINE DEPLOYMENT COMPLETE!${NC}"
# echo "============================================================"

# echo -e "\n${CYAN}📊 Access Points:${NC}"
# echo "  • HDFS UI:         http://localhost:9870"
# echo "  • Spark Master:    http://localhost:8080"
# echo "  • Kafka UI:        http://localhost:8091"
# echo "  • Mongo Express:   http://localhost:8083 (admin/admin123)"
# echo "  • MongoDB:         mongodb://root:mongodb123@localhost:27018/chicago_crimes"

# echo -e "\n${CYAN}🔍 Monitoring Commands:${NC}"
# echo "  docker logs kafka_producer -f      # Producer logs"
# echo "  docker logs stream_consumer -f     # Consumer logs"
# echo "  docker logs mongodb -f             # MongoDB logs"
# echo "  docker logs spark-master -f        # Spark logs"

# echo -e "\n${CYAN}💾 Data Storage:${NC}"
# echo "  • HDFS: /user/root/data-lake/"
# echo "  • MongoDB Collections:"
# echo "    - stream_crime_hotspots"
# echo "    - stream_pattern_analysis"
# echo "    - stream_violence_escalation"
# echo "    - stream_domestic_correlation"
# echo "    - stream_temporal_patterns"

# echo -e "\n${CYAN}🛑 To shutdown:${NC}"
# echo "  ./run_all.sh --down                  # Shutdown (keep volumes)"
# echo "  ./run_all.sh --down --delete-volumes # Shutdown (delete volumes)"

# echo -e "\n${GREEN}✅ All systems operational!${NC}\n"



#!/bin/bash
# ================================================================
# Chicago Crimes - Batch Processing Pipeline
# ================================================================
# Simple self-contained script for batch processing only.
# No other scripts are called - everything is inlined.
# ================================================================
#ovde pomak
# set -e  # Exit on error

# # Colors
# RED='\033[0;31m'
# GREEN='\033[0;32m'
# YELLOW='\033[1;33m'
# CYAN='\033[0;36m'
# NC='\033[0m'

# print_step() {
#     echo -e "\n${CYAN}===> $1${NC}"
# }

# print_success() {
#     echo -e "${GREEN}✓ $1${NC}"
# }

# print_error() {
#     echo -e "${RED}✗ $1${NC}"
# }

# print_warning() {
#     echo -e "${YELLOW}⚠ $1${NC}"
# }

# # Get script directory
# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# cd "$SCRIPT_DIR"

# echo -e "${GREEN}"
# echo "╔════════════════════════════════════════════════╗"
# echo "║  Chicago Crimes - Batch Processing Pipeline   ║"
# echo "╚════════════════════════════════════════════════╝"
# echo -e "${NC}"

# # ============================================
# # STEP 1: Create Network
# # ============================================
# print_step "Creating Docker network 'big_data_network'..."
# if docker network create big_data_network 2>/dev/null; then
#     print_success "Network created"
# else
#     print_warning "Network already exists (continuing...)"
# fi
# sleep 2

# # ============================================
# # STEP 2: Start Batch Processing Containers
# # ============================================
# print_step "Starting batch processing containers..."
# cd "$SCRIPT_DIR/batch_processing"
# docker-compose up -d
# sleep 5
# print_success "Containers started"

# # ============================================
# # STEP 3: Copy Data to Namenode
# # ============================================

# DATA_FILE_WIN="$SCRIPT_DIR/data/chicago_crimes.csv"
# DATA_FILE=$(cygpath -u "$DATA_FILE_WIN")

# if [ -f "$DATA_FILE_WIN" ]; then
#     print_step "Copying data file to namenode..."
#     docker cp "$DATA_FILE" namenode:/batch_data.csv
#     sleep 2
#     print_success "Data copied to namenode"
# else
#     print_warning "No data file found at: $DATA_FILE_WIN"
#     print_warning "Skipping data upload"
# fi

# # ============================================
# # STEP 4: Wait for HDFS Safe Mode
# # ============================================
# print_step "Waiting for HDFS to leave safe mode..."
# MAX_RETRIES=30
# RETRIES=0
# while [ $RETRIES -lt $MAX_RETRIES ]; do
#     if ! docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is ON"; then
#         print_success "HDFS ready"
#         break
#     fi
#     echo -ne "  Waiting for HDFS... ($RETRIES/$MAX_RETRIES)\r"
#     sleep 10
#     RETRIES=$((RETRIES + 1))
# done

# if [ $RETRIES -eq $MAX_RETRIES ]; then
#     print_error "HDFS failed to leave safe mode"
#     exit 1
# fi

# # ============================================
# # STEP 5: Upload Data to HDFS
# # ============================================
# if [ -f "$DATA_FILE" ]; then
#     print_step "Creating HDFS directories and uploading data..."
    
#     echo "  → Creating HDFS directories..."
#     MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/raw
#     MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/transform
    
#     echo "  → Uploading to HDFS..."
#     MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -copyFromLocal -f /batch_data.csv /user/root/data-lake/raw/batch_data.csv
    
#     print_success "Data uploaded to HDFS"
#     sleep 2
# fi

# # ============================================
# # STEP 6: Copy PostgreSQL JAR to Spark
# # ============================================
# print_step "Copying PostgreSQL JAR to Spark Master..."

# # Wait for spark-master
# echo "  → Waiting for Spark master..."
# SPARK_READY=false
# for i in {1..15}; do
#     if docker ps --filter "name=spark-master" --filter "status=running" --format "{{.Names}}" | grep -q "spark-master"; then
#         SPARK_READY=true
#         break
#     fi
#     sleep 2
# done

# if [ "$SPARK_READY" = false ]; then
#     print_error "Spark master not running"
#     exit 1
# fi

# # Give Spark extra time to fully initialize
# sleep 10

# JAR_FILE="$SCRIPT_DIR/data/postgresql-42.7.0.jar"
# if [ -f "$JAR_FILE" ]; then
#     echo "  → Copying JAR file..."
    
#     # Try copying with timeout - use MSYS_NO_PATHCONV for Windows Git Bash
#     MSYS_NO_PATHCONV=1 timeout 30 docker cp "$JAR_FILE" spark-master:/opt/bitnami/spark/jars/postgresql-42.7.0.jar 2>/dev/null || print_warning "Copy to jars folder timed out"
#     MSYS_NO_PATHCONV=1 timeout 30 docker cp "$JAR_FILE" spark-master:./postgresql-42.7.0.jar 2>/dev/null || print_warning "Copy to home folder timed out"
    
#     print_success "PostgreSQL JAR copied"
# else
#     print_warning "PostgreSQL JAR not found at: $JAR_FILE"
# fi
# sleep 2

# # ============================================
# # STEP 7: Run Spark Jobs
# # ============================================
# print_step "Running Spark batch processing jobs..."

# echo ""
# echo "  → Running preprocessing job..."
# MSYS_NO_PATHCONV=1 docker exec -i spark-master /spark/bin/spark-submit /home/batch/preprocessing.py

# echo ""
# echo "  → Running processing job (with PostgreSQL)..."
# MSYS_NO_PATHCONV=1 docker exec -i spark-master /spark/bin/spark-submit \
#     --driver-class-path postgresql-42.7.0.jar \
#     /home/batch/processing.py

# # ============================================
# # DONE
# # ============================================
# echo ""
# echo "============================================================"
# print_success "BATCH PROCESSING COMPLETE!"
# echo "============================================================"

# echo -e "\n${CYAN}📊 Access Points:${NC}"
# echo "  • HDFS UI:      http://localhost:9870"
# echo "  • Spark Master: http://localhost:8080"
# echo "  • Metabase:     http://localhost:3000"
# echo "  • PostgreSQL:   localhost:5432 (postgres/postgres)"

# echo -e "\n${CYAN}💾 Data Locations:${NC}"
# echo "  • HDFS Raw:        /user/root/data-lake/raw/batch_data.csv"
# echo "  • HDFS Processed:  /user/root/data-lake/transform/chicago_crimes.csv"
# echo "  • PostgreSQL DB:   big_data (10 tables with results)"

# echo -e "\n${CYAN}🔍 Check Results:${NC}"
# echo "  docker exec -i postgresql psql -U postgres -d big_data -c '\\dt'"
# echo "  docker logs spark-master"

# echo -e "\n${CYAN}🛑 To Shutdown:${NC}"
# echo "  cd batch_processing && docker-compose down"
# echo "  docker network rm big_data_network"

# echo -e "\n${GREEN}✅ Done!${NC}\n"

# za all:
#!/bin/bash
# ================================================================
# Chicago Crimes - Complete Pipeline (Batch + Stream)
# ================================================================
# This script does EVERYTHING in one go:
# 1. Batch Processing Infrastructure + Jobs
# 2. Stream Processing Infrastructure + Consumer
# No other scripts are called - completely self-contained.
# ================================================================

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

print_step() {
    echo -e "\n${CYAN}===> $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${GREEN}"
cat << "EOF"
╔════════════════════════════════════════════════════════╗
║  Chicago Crimes Analytics - Complete Pipeline         ║
║  Batch Processing + Stream Processing                 ║
╚════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# ============================================
# PART 1: BATCH PROCESSING
# ============================================
echo ""
echo "============================================================"
echo -e "${YELLOW}PART 1: BATCH PROCESSING${NC}"
echo "============================================================"

# Step 1: Create Network
print_step "Creating Docker network 'big_data_network'..."
if docker network create big_data_network 2>/dev/null; then
    print_success "Network created"
else
    print_warning "Network already exists (continuing...)"
fi
sleep 2

# Step 2: Start Batch Containers
print_step "Starting batch processing containers..."
cd "$SCRIPT_DIR/batch_processing"
docker-compose up -d
sleep 5
print_success "Batch containers started"
cd "$SCRIPT_DIR"

# Step 3: Copy Data to Namenode
print_step "Copying data file to namenode..."
cd "$SCRIPT_DIR"
DATA_FILE="data/chicago_crimes.csv"

if [ -f "$DATA_FILE" ]; then
    echo "  → Found data file: $DATA_FILE"
    MSYS_NO_PATHCONV=1 docker cp "$DATA_FILE" namenode:/batch_data.csv
    sleep 2
    print_success "Data copied to namenode"
else
    print_warning "No data file found at: $DATA_FILE"
    print_warning "Current directory: $(pwd)"
    print_warning "Batch processing will be skipped"
    SKIP_BATCH=true
fi

# Step 4: Wait for HDFS
if [ "$SKIP_BATCH" != true ]; then
    print_step "Waiting for HDFS to leave safe mode..."
    MAX_RETRIES=30
    RETRIES=0
    while [ $RETRIES -lt $MAX_RETRIES ]; do
        if ! docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is ON"; then
            print_success "HDFS ready"
            break
        fi
        echo -ne "  Waiting for HDFS... ($RETRIES/$MAX_RETRIES)\r"
        sleep 10
        RETRIES=$((RETRIES + 1))
    done

    if [ $RETRIES -eq $MAX_RETRIES ]; then
        print_error "HDFS failed to leave safe mode"
        exit 1
    fi

    # Step 5: Upload to HDFS
    print_step "Creating HDFS directories and uploading data..."
    echo "  → Creating HDFS directories..."
    MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/raw
    MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /user/root/data-lake/transform
    
    echo "  → Uploading to HDFS..."
    MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -copyFromLocal -f /batch_data.csv /user/root/data-lake/raw/batch_data.csv
    print_success "Data uploaded to HDFS"
    sleep 2

    # Step 6: Copy PostgreSQL JAR
    print_step "Copying PostgreSQL JAR to Spark Master..."
    echo "  → Waiting for Spark master..."
    SPARK_READY=false
    for i in {1..15}; do
        if docker ps --filter "name=spark-master" --filter "status=running" --format "{{.Names}}" | grep -q "spark-master"; then
            SPARK_READY=true
            break
        fi
        sleep 2
    done

    if [ "$SPARK_READY" = false ]; then
        print_error "Spark master not running"
        exit 1
    fi

    sleep 10  # Extra time for Spark to initialize

    cd "$SCRIPT_DIR"
    JAR_FILE="data/postgresql-42.7.0.jar"
    if [ -f "$JAR_FILE" ]; then
        echo "  → Copying JAR file..."
        MSYS_NO_PATHCONV=1 timeout 30 docker cp "$JAR_FILE" spark-master:/opt/bitnami/spark/jars/postgresql-42.7.0.jar 2>/dev/null || print_warning "Copy to jars folder timed out"
        MSYS_NO_PATHCONV=1 timeout 30 docker cp "$JAR_FILE" spark-master:./postgresql-42.7.0.jar 2>/dev/null || print_warning "Copy to home folder timed out"
        print_success "PostgreSQL JAR copied"
    else
        print_warning "PostgreSQL JAR not found at: $JAR_FILE"
    fi
    sleep 2

    # Step 7: Run Batch Jobs
    print_step "Running Spark batch processing jobs..."
    echo ""
    echo "  → Running preprocessing job..."
    MSYS_NO_PATHCONV=1 docker exec -i spark-master /spark/bin/spark-submit /home/batch/preprocessing.py

    echo ""
    echo "  → Running processing job (with PostgreSQL)..."
    MSYS_NO_PATHCONV=1 docker exec -i spark-master /spark/bin/spark-submit \
        --driver-class-path postgresql-42.7.0.jar \
        /home/batch/processing.py

    print_success "Batch processing complete!"
fi

# ============================================
# PART 2: STREAM PROCESSING
# ============================================
echo ""
echo "============================================================"
echo -e "${YELLOW}PART 2: STREAM PROCESSING${NC}"
echo "============================================================"

# Step 1: Cleanup conflicting containers
print_step "Cleaning up conflicting streaming containers..."
docker stop zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true
docker rm zoo1 kafka1 kafka2 kafka_producer mongodb mongo-express kafka-ui 2>/dev/null || true
print_success "Cleanup complete"

# Step 2: Start Streaming Cluster
print_step "Starting streaming cluster..."
cd "$SCRIPT_DIR/stream-processing"
docker-compose up -d
sleep 10
print_success "Streaming containers started"
cd "$SCRIPT_DIR"

# Step 3: Wait for initialization
print_step "Waiting for containers to fully initialize..."
sleep 50

# Step 4: Verify containers
print_step "Verifying containers..."
docker ps --filter "name=kafka" --filter "name=zoo" --format "table {{.Names}}\t{{.Status}}"

# Step 5: Test network connectivity
print_step "Testing network connectivity..."
CONNECTED=false
for i in {1..5}; do
    echo -n "  Attempt $i/5: Testing connectivity to kafka1..."
    if docker exec spark-master ping -c 1 kafka1 &>/dev/null; then
        echo " ✓"
        CONNECTED=true
        break
    else
        echo " ✗ (retrying...)"
        sleep 10
    fi
done

if [ "$CONNECTED" = false ]; then
    print_error "Network connectivity failed"
    exit 1
fi

# Step 6: Test Kafka readiness
print_step "Testing Kafka readiness..."
KAFKA_READY=false
for i in {1..15}; do
    echo -n "  Attempt $i/15: Testing Kafka brokers..."
    
    if docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092 &>/dev/null && \
       docker exec kafka2 kafka-topics --list --bootstrap-server kafka2:29092 &>/dev/null; then
        echo " ✓"
        KAFKA_READY=true
        break
    else
        echo " ✗ (retrying...)"
        sleep 8
    fi
done

if [ "$KAFKA_READY" = false ]; then
    print_error "Kafka failed to start properly"
    echo ""
    echo "=== Kafka1 Logs ==="
    docker logs kafka1 --tail 30
    echo ""
    echo "=== Kafka2 Logs ==="
    docker logs kafka2 --tail 30
    exit 1
fi

# Step 7: Create Kafka topic
print_step "Creating Kafka topic 'chicagocrimes'..."

# Check if topic exists first (modern Kafka CLI)
EXISTS=$(docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092,kafka2:29092 | grep -w chicagocrimes || true)
if [ -z "$EXISTS" ]; then
    echo "  → Topic not found. Creating..."
    docker exec kafka1 kafka-topics --create \
        --topic chicagocrimes \
        --bootstrap-server kafka1:19092,kafka2:29092 \
        --partitions 3 \
        --replication-factor 1 \
        --config cleanup.policy=delete \
        --config retention.ms=86400000 \
        --config segment.ms=3600000
    print_success "Topic 'chicagocrimes' created successfully"
else
    print_success "Topic 'chicagocrimes' already exists"
fi

# Step 8: Verify topic creation
print_step "Verifying topic creation..."
TOPIC_CREATED=false
for i in {1..10}; do
    if docker exec kafka1 kafka-topics --describe --topic chicagocrimes --bootstrap-server kafka1:19092,kafka2:29092 &>/dev/null; then
        print_success "Topic 'chicagocrimes' verified successfully"
        TOPIC_CREATED=true
        break
    fi
    sleep 3
done

if [ "$TOPIC_CREATED" = false ]; then
    print_error "Failed to create or access topic 'chicagocrimes'"
    exit 1
fi

# Step 9: Clean up old consumer
print_step "Stopping any old streaming consumers..."
docker stop stream_consumer 2>/dev/null || true
docker rm stream_consumer 2>/dev/null || true

# Step 10: Start consumer
print_step "Starting stream consumer service..."
cd "$SCRIPT_DIR/stream-processing"
docker-compose up -d consumer
sleep 30
cd "$SCRIPT_DIR"

# Step 11: Verify consumer started
if docker ps --filter "name=stream_consumer" --format "{{.Names}}" | grep -q "stream_consumer"; then
    print_success "Stream consumer started successfully"
else
    print_error "Stream consumer failed to start"
    docker logs stream_consumer --tail 20
    exit 1
fi

# Step 12: Monitor consumer startup
print_step "Monitoring consumer startup..."
sleep 15
echo "Recent consumer logs:"
docker logs stream_consumer --tail 50

# Step 13: Test MongoDB connectivity
print_step "Testing MongoDB connectivity..."
sleep 5
if docker exec mongodb mongosh -u root -p mongodb123 --eval "db.adminCommand('ismaster')" &>/dev/null; then
    print_success "MongoDB is accessible"
else
    print_warning "MongoDB connection test failed (may be normal during initialization)"
fi

# Step 14: Final verification
print_step "Final verification - waiting for consumer to fully initialize..."
sleep 30

print_success "STREAMING PIPELINE READY!"

# ============================================
# FINAL STATUS
# ============================================
echo ""
echo "============================================================"
echo -e "${GREEN}COMPLETE PIPELINE DEPLOYMENT SUCCESSFUL!${NC}"
echo "============================================================"

echo -e "\n${CYAN}📊 Batch Processing Access Points:${NC}"
echo "  • HDFS UI:      http://localhost:9870"
echo "  • Spark Master: http://localhost:8080"
echo "  • Metabase:     http://localhost:3000"
echo "  • PostgreSQL:   localhost:5432 (postgres/postgres)"

echo -e "\n${CYAN}📊 Stream Processing Access Points:${NC}"
echo "  • Kafka UI:        http://localhost:8091"
echo "  • Mongo Express:   http://localhost:8083 (admin/admin123)"
echo "  • MongoDB:         mongodb://root:mongodb123@localhost:27018/chicago_crimes"

echo -e "\n${CYAN}💾 Data Storage:${NC}"
echo "  Batch (HDFS):"
echo "    • Raw:        /user/root/data-lake/raw/batch_data.csv"
echo "    • Processed:  /user/root/data-lake/transform/chicago_crimes.csv"
echo "  Batch (PostgreSQL):"
echo "    • Database: big_data (10 tables with analytical results)"
echo "  Stream (MongoDB):"
echo "    • stream_crime_hotspots"
echo "    • stream_pattern_analysis"
echo "    • stream_violence_escalation"
echo "    • stream_domestic_correlation"
echo "    • stream_temporal_patterns"

echo -e "\n${CYAN}🔍 Monitoring Commands:${NC}"
echo "  docker logs kafka_producer -f      # Producer logs"
echo "  docker logs stream_consumer -f     # Consumer logs"
echo "  docker logs mongodb -f             # MongoDB logs"
echo "  docker logs spark-master -f        # Spark logs"

echo -e "\n${CYAN}🔍 Check Results:${NC}"
echo "  # Batch results in PostgreSQL:"
echo "  docker exec -i postgresql psql -U postgres -d big_data -c '\\dt'"
echo ""
echo "  # Stream results in MongoDB:"
echo "  docker exec mongodb mongosh -u root -p mongodb123 chicago_crimes --eval 'show collections'"

echo -e "\n${CYAN}🛑 To Shutdown Everything:${NC}"
echo "  cd stream-processing && docker-compose down"
echo "  cd ../batch_processing && docker-compose down"
echo "  docker network rm big_data_network"

echo -e "\n${GREEN}✅ All systems operational!${NC}\n"
