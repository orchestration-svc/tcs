package net.messaging.clusterbox.litemq.broker;

public interface BrokerConfig {

    String getIpAddress();

    String getFrontendProtocol();

    String getFrontendPort();

}
