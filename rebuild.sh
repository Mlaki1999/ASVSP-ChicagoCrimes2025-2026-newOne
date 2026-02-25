#!/bin/bash
# ================================================================
# Rebuild Stream Processing Producer & Consumer
# ================================================================
# This script:
# 1. Stops producer and consumer containers
# 2. Removes them
# 3. Rebuilds them with fresh Docker images (no cache)
# ================================================================

set -e

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

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/stream-processing"

echo -e "${GREEN}"
echo "╔════════════════════════════════════════════════╗"
echo "║  Rebuild Producer & Consumer                   ║"
echo "╚════════════════════════════════════════════════╝"
echo -e "${NC}"

# Step 1: Stop containers
print_step "Stopping producer and consumer containers..."
docker stop kafka_producer 2>/dev/null || echo "  Producer not running"
docker stop stream_consumer 2>/dev/null || echo "  Consumer not running"
print_success "Containers stopped"

# Step 2: Remove containers
print_step "Removing producer and consumer containers..."
docker rm kafka_producer 2>/dev/null || echo "  Producer already removed"
docker rm stream_consumer 2>/dev/null || echo "  Consumer already removed"
print_success "Containers removed"

# Step 3: Rebuild with no cache
print_step "Rebuilding producer and consumer (no cache)..."
echo "  This may take a few minutes..."
docker-compose build --no-cache producer consumer

print_success "Rebuild complete!"

# Step 4: Start containers
print_step "Starting producer and consumer..."
docker-compose up -d producer consumer

sleep 5
print_success "Containers started"

# Step 5: Show status
print_step "Container status:"
docker ps --filter "name=kafka_producer" --filter "name=stream_consumer" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"

echo ""
echo -e "${GREEN}✅ Producer and Consumer rebuilt and running!${NC}"
echo ""
echo -e "${CYAN}📋 Monitor logs:${NC}"
echo "  docker logs kafka_producer -f"
echo "  docker logs stream_consumer -f"
echo ""
