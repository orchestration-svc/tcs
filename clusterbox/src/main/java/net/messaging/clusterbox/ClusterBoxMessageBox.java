package net.messaging.clusterbox;

public interface ClusterBoxMessageBox {

    ClusterBoxMessageHandler<?> getMessageHandler();

    String getMessageBoxId();

    String getMessageBoxName();
}
