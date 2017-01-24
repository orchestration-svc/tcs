package net.messaging.clusterbox;

import net.messaging.clusterbox.litemq.broker.BrokerConfig;

public interface ClusterBoxConfig {

    BrokerConfig getBrokerConfig();

    String getClusterBoxName();

    String getClusterBoxId();

}
