package net.messaging.clusterbox;

public interface ClusterBox {

    ClusterBoxConfig getClusterBoxConfig();

    void start();

    void shutDown();

    void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox);

    ClusterBoxDropBox getDropBox();

    boolean isOn();

}
