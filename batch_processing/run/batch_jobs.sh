echo "> Starting preprocessing script..."
sleep 5
./spark/bin/spark-submit /home/batch/preprocessing.py 

echo 
echo "> Starting processing script..."
sleep 5
./spark/bin/spark-submit --driver-class-path postgresql-42.7.0.jar /home/batch/processing.py


# echo "> Starting test script..."
# sleep 5
# ./spark/bin/spark-submit /home/batch/test.py 