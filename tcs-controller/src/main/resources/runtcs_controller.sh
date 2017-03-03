#!/bin/bash
export TCS_DIR=$PWD

if [ $# -lt 1 ]; then
    echo "illegal number of arguments: Must specify application.properties"
    exit 1
fi

export TCS_APPLICATION_PROPERTIES=$1

if [ $# -eq 2 ]; then
    export ZOOKEEPER_IP=$2
fi

export TCS_JAR_DIR=$TCS_DIR/bin

java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$TCS_DIR -XX:+UseG1GC -XX:-UseSplitVerifier -Xms100m -XX:MaxPermSize=450m -XX:+UseCompressedOops -XX:MinHeapFreeRatio=20 -XX:NewRatio=3 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dtcs.zookeeperIP=$ZOOKEEPER_IP -jar $TCS_JAR_DIR/tcs-controller-1.0.jar --spring.config.location=file:$TCS_APPLICATION_PROPERTIES
