FROM openjdk:7

RUN apt-get update && apt-get install -y mysql-client

ADD tcs-1.0-bin.tar.gz /tmp

ENV ZK_IP localhost
ENV RMQ_IP localhost
ENV MYSQL_IP localhost
ENV NUM_SHARDS 4
ENV TCS_INSTANCE tcsinst1000
ENV CONFIG_TCS false

WORKDIR /tmp
ENTRYPOINT bin/runtcs_wrapper.sh $TCS_INSTANCE
