# REMOVE LATER
#hdfs dfs -rm -r /user/root/data-lake/raw
#hdfs dfs -rm -r /user/root/data-lake/transform

# Create directories in HDFS.
hdfs dfs -mkdir -p /user/root/data-lake/raw
hdfs dfs -mkdir -p /user/root/data-lake/transform

# Copy local files to HDFS.
hdfs dfs -copyFromLocal -f ./batch_data.csv /user/root/data-lake/raw/batch_data.csv