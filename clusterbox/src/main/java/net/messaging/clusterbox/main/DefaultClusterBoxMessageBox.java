package net.messaging.clusterbox.main;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;

public class DefaultClusterBoxMessageBox implements ClusterBoxMessageBox {

    /*
     * Queue name
     */
    private final String messageBoxId;
    /*
     * Routingkey
     */
    private final String messageBoxName;
    private ClusterBoxMessageHandler<?> messageHandler;

    public DefaultClusterBoxMessageBox(String messageBoxId, String messageBoxName) {
        super();
        this.messageBoxId = messageBoxId;
        this.messageBoxName = messageBoxName;
    }

    @Override
    public String getMessageBoxId() {
        return messageBoxId;
    }

    @Override
    public String getMessageBoxName() {
        return messageBoxName;
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        if (messageHandler == null) {
            messageHandler = new ClusterBoxMessageHandler<MyPayload>() {
                @Override
                public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {
                    MyPayload payload = (MyPayload) message.getPayload();
                    System.out.println("Got payload " + payload);
                }

                @Override
                public String getRequestName() {
                    return MyPayload.class.getName();
                }

            };
        }
        return messageHandler;
    }

}
