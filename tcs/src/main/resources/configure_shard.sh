#!/bin/bash
export TCS_DIR=$PWD

export CONFIG_FILE=conf/config.json
export TCS_JAR_DIR=$TCS_DIR/bin

java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$TCS_DIR -XX:+UseG1GC -XX:-UseSplitVerifier -Xms100m -XX:MaxPermSize=450m -XX:+UseCompressedOops -XX:MinHeapFreeRatio=20 -XX:NewRatio=3 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dtcs.log=$TCS_DIR/$TCS_SERVICE_INSTANCE.log -cp $TCS_JAR_DIR/tcs-1.0-SNAPSHOT-jar-with-dependencies.jar net.tcs.utils.TCSClusterConfigDriver $CONFIG_FILE
