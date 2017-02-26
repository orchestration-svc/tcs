#!/bin/bash

if [ $# -ne 2 ];
    then echo "illegal number of arguments: Must specify RMQHostIPAddress and PartitionCount"
    exit 1
fi

echo "RMQHOST: $1"
echo "PARITION COUNT: $2"

export RMQHOST=$1
export TCS_JOB_REGISTER_QUERY_EXCHANGE="tcs.exchange.registerjob"
export TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE="tcs.exchange.execjob"

echo "Declare exchange: $TCS_JOB_REGISTER_QUERY_EXCHANGE"
rabbitmqadmin -H ${RMQHOST} declare exchange name=$TCS_JOB_REGISTER_QUERY_EXCHANGE type=topic
echo "Declare exchange: $TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE"
rabbitmqadmin -H ${RMQHOST} declare exchange name=$TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE type=direct

# The following exchange is created for testing purposes
echo "Declare exchange: tcs.exchange.test (for testing purposes)"
rabbitmqadmin -H ${RMQHOST} declare exchange name=tcs.exchange.test type=topic

echo "Declare queue: tcs.registerjob"
rabbitmqadmin -H ${RMQHOST} declare queue name=tcs.registerjob durable=true auto_delete=false
echo "Declare bindings: tcs.registerjob.route"
rabbitmqadmin -H ${RMQHOST} declare binding source=$TCS_JOB_REGISTER_QUERY_EXCHANGE destination_type="queue" destination="tcs.registerjob" routing_key="tcs.registerjob.route"


echo "Declare queue: tcs.queryjob"
rabbitmqadmin -H ${RMQHOST} declare queue name=tcs.queryjob durable=true auto_delete=false
echo "Declare bindings: tcs.queryjob.route"
rabbitmqadmin -H ${RMQHOST} declare binding source=$TCS_JOB_REGISTER_QUERY_EXCHANGE destination_type="queue" destination="tcs.queryjob" routing_key="tcs.queryjob.route"


echo "Declare queue: tcs.submitjob"
rabbitmqadmin -H ${RMQHOST} declare queue name=tcs.submitjob durable=true auto_delete=false
echo "Declare bindings: tcs.submitjob.route"
rabbitmqadmin -H ${RMQHOST} declare binding source=$TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE destination_type="queue" destination="tcs.submitjob" routing_key="tcs.submitjob.route"


parition_count="$2"
parition_count=$((parition_count-1))

for index in $(seq 0 $parition_count)
  do
    JOB_EXE_QUEUE_NAME="tcs.job.exec.tcs-shard_$index"
    JOB_EXE_BINDING_KEY="tcs.job.exec.route.tcs-shard_$index"
    echo "Declare queue: $JOB_EXE_QUEUE_NAME"
    rabbitmqadmin -H ${RMQHOST} declare queue name=$JOB_EXE_QUEUE_NAME durable=true auto_delete=false
    echo "Declare bindings: $JOB_EXE_BINDING_KEY"
    rabbitmqadmin -H ${RMQHOST} declare binding source=$TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE destination_type="queue" destination=$JOB_EXE_QUEUE_NAME routing_key=$JOB_EXE_BINDING_KEY


    TASK_NOTIFY_QUEUE_NAME="tcs.task.notif.tcs-shard_$index"
    TASK_NOTIFY_BINDING_KEY="tcs.task.notif.route.tcs-shard_$index"
    echo "Declare queue: $TASK_NOTIFY_QUEUE_NAME"
    rabbitmqadmin -H ${RMQHOST} declare queue name=$TASK_NOTIFY_QUEUE_NAME durable=true auto_delete=false
    echo "Declare bindings : $TASK_NOTIFY_BINDING_KEY"
    rabbitmqadmin -H ${RMQHOST} declare binding source=$TCS_JOB_EXE_TASK_NOTIFY_EXCHANGE destination_type="queue" destination=$TASK_NOTIFY_QUEUE_NAME routing_key=$TASK_NOTIFY_BINDING_KEY

  done

