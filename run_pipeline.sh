#!/bin/bash

echo "🚀 Chicago Crimes Analytics Pipeline - Quick Start"
echo "=================================================="

# Step 1: Start the cluster
echo ""
echo "📦 Step 1: Starting the big data cluster..."
cd setup
chmod +x cluster_up.sh
./cluster_up.sh

# Step 2: Wait a bit for services to be ready
echo ""
echo "⏳ Step 2: Waiting for services to be ready..."
sleep 30

# Step 3: Run batch processing
echo ""
echo "🔄 Step 3: Running batch processing jobs..."
cd ../batch-processing/run
chmod +x batch_jobs_run.sh
./batch_jobs_run.sh

# Step 4: Show completion message
echo ""
echo "Pipeline completed successfully!"
echo ""
