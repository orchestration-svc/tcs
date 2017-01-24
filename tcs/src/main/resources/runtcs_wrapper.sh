#!/bin/bash

if [ $CONFIG_TCS == 'true' ]
then
  echo "Configuring TCS"
  bin/configure_tcs.sh
else
  sed "s/ZK_IP/$ZK_IP/g;s/RMQ_IP/$RMQ_IP/g;s/MYSQL_IP/$MYSQL_IP/g;s/NUM_SHARDS/$NUM_SHARDS/g" conf/config-template.json > conf/config.json
  cat conf/config.json
fi

echo "TCS Service instance: name: $1"
export TCS_SERVICE_INSTANCE=$1

bin/runtcs.sh $TCS_SERVICE_INSTANCE
