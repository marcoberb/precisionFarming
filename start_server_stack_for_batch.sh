#!/bin/bash
echo "Starting server stack for batch analysis"
docker-compose -f docker-compose.yml up -d mongo1 mongo2 mongo3
echo "SLEEPTIME 3s"
sleep 3s
echo "Configuring replica set"
docker exec mongo1 mongo --eval "rs.initiate(
  {
    _id : 'rs0',
    members: [
      { _id : 0, host : 'mongo1:27017' },
      { _id : 1, host : 'mongo2:27017' },
      { _id : 2, host : 'mongo3:27017' }
    ]
  }
)"
echo "Done"
echo "Copying meteo csv into mongo1 instance"
docker cp data/meteo_commas_2020.csv mongo1:/
echo "SLEEPTIME 10s"
sleep 10s
echo "Importing meteo csv into mongoDB"
docker exec mongo1 mongoimport --type csv -d precision_farming -c meteo --headerline /meteo_commas_2020.csv --drop

docker-compose -f docker-compose.yml up -d mongo-express

echo "All done"