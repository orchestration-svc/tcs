#!/bin/bash

: "${ZK_IP?Need to set ZK_IP}"
: "${RMQ_IP?Need to set RMQ_IP}"
: "${MYSQL_IP?Need to set MYSQL_IP}"
: "${NUM_SHARDS?Need to set NUM_SHARDS}"

echo "ZooKeeper IP: ${ZK_IP}"
echo "RabbitMQ IP: ${RMQ_IP}"
echo "MySQL IP: ${MYSQL_IP}"
echo "Number of shards: ${NUM_SHARDS}"

sed "s/ZK_IP/$ZK_IP/g;s/RMQ_IP/$RMQ_IP/g;s/MYSQL_IP/$MYSQL_IP/g;s/NUM_SHARDS/$NUM_SHARDS/g" conf/config-template.json > conf/config.json
cat conf/config.json

echo "Creating database tcsdb, and tables"
mysql --host=$MYSQL_IP --user=root --password=root < conf/create_tcs_db.sql

wget http://$RMQ_IP:15672/cli/rabbitmqadmin
chmod +x rabbitmqadmin
export PATH=$PATH:$PWD

echo "Creating RabbitMQ exchanges and queues"
bin/configure_rmq.sh $RMQ_IP $NUM_SHARDS
rm -f rabbitmqadmin

echo "Creating Helix/ZooKeeper configuration"
bin/configure_shard.sh
