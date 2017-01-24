package net.messaging.clusterbox.zerromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;

public class ZerromqClusterBoxDropBox implements ClusterBoxDropBox {

    private final ZerromqClusterBoxConfig clusterBoxConfig;
    private final ZMQ.Context dropBoxContext;
    private ZMQ.Socket frontendSocket;
    private ObjectMapper mapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerromqClusterBoxDropBox.class);

    public ZerromqClusterBoxDropBox(ZerromqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
        dropBoxContext = ZMQ.context(1);
        mapper = new ObjectMapper();
        mapper.enableDefaultTyping();
        LOGGER.info("Started ZerromqClusterBoxDropBox");
    }

    public void drop(Message<?> message) throws ClusterBoxMessageSendFailed {
        // Make the Socket Connection
        validateMessage(message);
        frontendSocket = dropBoxContext.socket(ZMQ.DEALER);
        frontendSocket.setIdentity(message.peekFrom().toJson().getBytes());
        frontendSocket.connect(clusterBoxConfig.getBrokerConfig().getFrontendProtocol() + "://"
                + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                + clusterBoxConfig.getBrokerConfig().getFrontendPort());
        try {
            ZMsg msg = new ZMsg();
            msg.add(message.getTo().toJson());
            msg.add(mapper.writeValueAsString(message));
            msg.send(frontendSocket);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new ClusterBoxMessageSendFailed(e.getMessage());
        } finally {
            frontendSocket.close();
        }
        LOGGER.info("Message {} Sent.....", message);
    }

    public void shutdown() {
        dropBoxContext.term();
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
