#!/bin/bash
echo "Starting server stack"
docker-compose -f docker-compose.yml up -d
sleep 5s
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
