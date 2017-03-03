FROM openjdk:7

ADD tcs-controller-1.0-SNAPSHOT-bin.tar.gz /tmp

ENV ZK_IP localhost
ENV MYSQL_IP localhost

EXPOSE 8080

WORKDIR /tmp
ENTRYPOINT bin/runtcs_controller_wrapper.sh
