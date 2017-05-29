#!/bin/bash

if [ -z "$MYSQL_IP" ]; then
    MYSQL_IP=0.0.0.0
fi

sed "s/MYSQL_IP/$MYSQL_IP/g" conf/application.properties > conf/application2.properties
mv conf/application2.properties conf/application.properties
cat conf/application.properties

bin/runtcs_controller.sh conf/application.properties $ZK_IP
