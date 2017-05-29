#!/bin/bash

if [ -z "$ZK_IP" ]; then
    echo "ZK_IP is not set; defaulting to 0.0.0.0"
    ZK_IP=0.0.0.0
fi

if [ -z "$RMQ_IP" ]; then
    echo "RMQ_IP is not set; defaulting to 0.0.0.0"
    RMQ_IP=0.0.0.0
fi

if [ -z "$MYSQL_IP" ]; then
    echo "MYSQL_IP is not set; defaulting to 0.0.0.0"
    MYSQL_IP=0.0.0.0
fi

if [ -z "$NUM_SHARDS" ]; then
    echo "NUM_SHARDS is not set; defaulting to 1"
    NUM_SHARDS=1
fi

if [ "$CONFIG_TCS" = 'true' ]; then
  echo "Configuring TCS"
  bin/configure_tcs.sh
else
  sed "s/ZK_IP/$ZK_IP/g;s/RMQ_IP/$RMQ_IP/g;s/MYSQL_IP/$MYSQL_IP/g;s/NUM_SHARDS/$NUM_SHARDS/g" conf/config-template.json > conf/config.json
  cat conf/config.json
fi

if [ $# -ne 1 ]; then
    echo "illegal number of arguments: Must specify TCSInstanceName"
    echo "Usage: bin/runtcs_wrapper.sh <TCSInstanceName>"
    exit 1
fi

echo "TCS Service instance: name: $1"
export TCS_SERVICE_INSTANCE=$1

bin/runtcs.sh $TCS_SERVICE_INSTANCE
