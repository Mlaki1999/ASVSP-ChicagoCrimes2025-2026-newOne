# Shutting down the cluster.
echo
echo "> Shutting down cluster..."
sleep 3

# Check if the volumes should be deleted.
echo
read -p "> Delete volumes? [y/N] " -n 1 -r
echo

# Check the user's response.
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Check to see if stream processing docker is running.
    cd ../stream_processing
    if docker-compose ps &> /dev/null; then
        docker-compose down -v
    fi
z
    docker-compose -f ../batch_processing/docker-compose.yml down -v
    # docker rmi -f $(docker images -q)

else
    # Check to see if stream processing docker is running.
    cd ../stream_processing
    if docker-compose ps &> /dev/null; then
        docker-compose down
    fi
    
    docker-compose -f ../batch_processing/docker-compose.yml down 
    
fi

# Delete the Docker network.
sleep 3
echo
echo "> Deleting 'big_data_network' network..."
docker network rm big_data_network
sleep 3

# Finishing cluster shut down.
echo
echo "> Cluster down."          
echo
sleep 3
