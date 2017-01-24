package net.messaging.clusterbox.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.exception.ClusterBoxDropBoxCreateFailed;
import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;

public class RabbitmqClusterBoxDropBox implements ClusterBoxDropBox {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqClusterBoxDropBox.class);
    private final Connection connection;
    private final Channel channel;
    private ObjectMapper mapper;

    public RabbitmqClusterBoxDropBox(Connection connection) {
        this.connection = connection;
        try {
            this.channel = connection.createChannel();
        } catch (IOException e) {
            throw new ClusterBoxDropBoxCreateFailed(e.getMessage());
        }
        mapper = new ObjectMapper();
        mapper.enableDefaultTyping();

    }

    @Override
    public void drop(Message<?> message) throws ClusterBoxMessageSendFailed {
        if (channel == null) {
            throw new ClusterBoxMessageSendFailed("transport channel is null");
        }
        String exchangeName = message.getTo().getClusterBoxName();
        String routingKey = message.getTo().getClusterBoxMailBoxName();
        try {
            channel.basicPublish(exchangeName, routingKey, null, mapper.writeValueAsBytes(message));
        } catch (Exception e) {
            throw new ClusterBoxMessageSendFailed(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

    private void validateMessage(Message<?> message) {
        if (message.getTo() == null) {
            LOGGER.error("Message Transmisson failed. Reason - to address is null");
            throw new ClusterBoxMessageSendFailed("To Address is null");
        }
        if (message.peekFrom() == null) {
            LOGGER.error("Message Transmisson failed. Reason - From address is null");
            throw new ClusterBoxMessageSendFailed("From Address is null");
        }
    }
}
