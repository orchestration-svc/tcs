package net.messaging.clusterbox;

import net.messaging.clusterbox.message.RequestMessage;

public interface ClusterBoxMessageHandler<T> {

    void handleMessage(RequestMessage<?> message, ClusterBoxDropBox dropBox);

    String getRequestName();

}
