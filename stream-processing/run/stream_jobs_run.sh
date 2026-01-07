cd ..

docker compose up -d

docker cp ./run/stream_jobs.sh spark-master:./stream_jobs.sh
# docker exec -it spark-master bash ./stream_jobs.sh
docker exec -i spark-master bash ./stream_jobs.sh
