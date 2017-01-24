package net.messaging.clusterbox;

import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;

/*
 * Producer Interface to produce the message to external or required destination
 * */
public interface ClusterBoxDropBox {

    void drop(Message<?> message) throws ClusterBoxMessageSendFailed;

    void shutdown();
}
