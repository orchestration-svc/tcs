#!/bin/bash

if [ $# -ne 2 ];
    then echo "illegal number of arguments: Must specify ConfigFile and DesiredPartitionCount"
    exit 1
fi

echo "CONFIG_FILE: $1"
echo "DESIRED PARITION COUNT: $2"

export CONFIG_FILE=$1
export PARTITION_COUNT=$2

export TCS_DIR=$PWD
export TCS_JAR_DIR=$TCS_DIR/bin

java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$TCS_DIR -XX:+UseG1GC -XX:-UseSplitVerifier -Xms100m -XX:MaxPermSize=450m -XX:+UseCompressedOops -XX:MinHeapFreeRatio=20 -XX:NewRatio=3 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dtcs.log=$TCS_DIR/$TCS_SERVICE_INSTANCE.log -cp $TCS_JAR_DIR/tcs-1.0-SNAPSHOT-jar-with-dependencies.jar net.tcs.utils.TCSClusterPartitionGrower $CONFIG_FILE $PARTITION_COUNT
