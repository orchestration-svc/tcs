package net.messaging.clusterbox.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.ClusterBoxConfig;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.exception.ClusterBoxCreateFailed;
import net.messaging.clusterbox.exception.ClusterBoxMessageBoxStartFailed;
import net.messaging.clusterbox.message.RequestMessage;

public class RabbitmqClusterBox implements ClusterBox {

    private final RabbitmqClusterBoxConfig clusterBoxConfig;
    private final Connection connection;
    private RabbitmqClusterBoxDropBox clusterBoxDropBox;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqClusterBox.class);

    // TODO : Need to make it singleton
    private RabbitmqClusterBox(RabbitmqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(clusterBoxConfig.getBrokerConfig().getIpAddress());
        if (clusterBoxConfig.getBrokerConfig().getFrontendPort() != null) {
            factory.setPort(Integer.valueOf(clusterBoxConfig.getBrokerConfig().getFrontendPort()));
        }
        try {
            connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new ClusterBoxCreateFailed(e.getMessage());
        }
        clusterBoxDropBox = new RabbitmqClusterBoxDropBox(connection);
    }

    public static ClusterBox newClusterBox(RabbitmqClusterBoxConfig clusterBoxConfig) {
        return new RabbitmqClusterBox(clusterBoxConfig);
    }

    @Override
    public ClusterBoxConfig getClusterBoxConfig() {
        return clusterBoxConfig;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutDown() {
        // TODO Auto-generated method stub

    }

    @Override
    public void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox) {
        try {
            Channel channel = connection.createChannel();
            ClusterBoxWorker worker = new ClusterBoxWorker(clusterBoxMessageBox, clusterBoxDropBox, channel,
                    clusterBoxConfig.getClusterBoxName());
            worker.start(clusterBoxConfig.isAutoDeclare());
        } catch (IOException e) {
            e.printStackTrace();
            throw new ClusterBoxMessageBoxStartFailed(e.getMessage());
        }

    }

    @Override
    public ClusterBoxDropBox getDropBox() {
        return clusterBoxDropBox;
    }

    private static class ClusterBoxWorker {

        private AtomicBoolean on = new AtomicBoolean(false);
        private final Channel channel;
        private final ClusterBoxMessageBox clusterBoxMessageBox;
        private final ObjectMapper mapper;
        private final String exchangeName;

        public ClusterBoxWorker(ClusterBoxMessageBox messageBox, RabbitmqClusterBoxDropBox dropBox, Channel channel,
                String exchangeName) {
            this.clusterBoxMessageBox = messageBox;
            this.channel = channel;
            mapper = new ObjectMapper();
            mapper.enableDefaultTyping();
            this.exchangeName = exchangeName;
        }

        public void start(boolean autoDeclare) {
            try {
                if (autoDeclare) {
                    // TODO : Need to clean this up a bit more
                    channel.exchangeDeclare(exchangeName, "direct", true);
                    channel.queueDeclare(clusterBoxMessageBox.getMessageBoxId(), true, false, false, null);
                    channel.queueBind(clusterBoxMessageBox.getMessageBoxId(), exchangeName,
                            clusterBoxMessageBox.getMessageBoxName());
                }
                String consumerTag = clusterBoxMessageBox.getMessageBoxName();
                channel.basicConsume(clusterBoxMessageBox.getMessageBoxId(), false, consumerTag,
                        new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag, Envelope envelope,
                                    AMQP.BasicProperties properties, byte[] body) {
                                RequestMessage<?> message = null;
                                try {
                                    message = mapper.readValue(body, RequestMessage.class);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    LOGGER.error("Error during message deserializing. Cause - {}", e.getMessage());
                                }
                                LOGGER.info("Got Request Message {}", message);
                                if (message != null) {
                                    clusterBoxMessageBox.getMessageHandler().handleMessage(message, null);
                                    try {
                                        channel.basicAck(envelope.getDeliveryTag(), false);
                                    } catch (IOException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                        LOGGER.error("Error during message ack. Cause - {}", e.getMessage());
                                    }
                                }
                            }

                        });
            } catch (IOException e) {
                e.printStackTrace();
                throw new ClusterBoxMessageBoxStartFailed(e.getMessage());
            }

        }

    }

    @Override
    public boolean isOn() {
        return connection.isOpen();
    }

}
