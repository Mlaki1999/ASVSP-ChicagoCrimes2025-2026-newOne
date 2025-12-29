# Creating docker network.
echo
echo "> Creating docker network 'big_data_network'..."
docker network create big_data_network
sleep 3

# Starting containers using Docker Compose in the background.
echo 
cd ../batch_processing
echo "> Starting containers using Docker Compose..."
docker-compose up -d
sleep 3

# Copying upload script to the Namenode container.
echo
cd ../setup
echo "> Copying upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh
sleep 3

# Copying chicago crimes data CSV files to the Namenode container.
echo 
cd ../data
echo "> Copying data to namenode..."
docker cp chicago_crimes.csv namenode:/batch_data.csv
sleep 3

# Wait until HDFS is out of safe mode.
echo
while docker exec namenode hdfs dfsadmin -safemode get | grep -q "Safe mode is ON"; do
    echo "> Waiting for HDFS to leave safe mode..."
    sleep 20
done

# Executing the upload to HDFS commands.
echo "> Executing upload to HDFS commands..."
docker exec namenode bash ./upload_hdfs.sh
sleep 3

# Copying the PostgreSQL JAR file to the Spark Master container.
echo 
echo "> Copying PostgreSQL JAR file to Spark Master..."
docker cp postgresql-42.7.0.jar spark-master:./postgresql-42.7.0.jar
sleep 3

# Fininshing cluster setup.
echo
echo "> Cluster ready for use."
echo
sleep 3
