package net.messaging.clusterbox.litemq.broker.zerromq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.litemq.broker.LitemqBroker;

public class ZerromqBroker implements LitemqBroker, Runnable {

    private AtomicBoolean on = new AtomicBoolean(false);
    private Thread brokerThread;
    private ZContext brokerContext;
    private ZMQ.Socket frontendSocket;
    private ZMQ.Socket backendSocket;
    private ZerromqBrokerConfig config;
    private ConcurrentHashMap<String, Set<ClusterBox>> discoveredClusterBox = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> pingedAt = new ConcurrentHashMap<>();
    private Poller pollItems;

    private static final Integer DEFAULT_IO_THREAD_COUNT = 1;
    private static final String CLUSTER_BOX_READY = "CLUSTER_BOX_READY";
    private static final String CLUSTER_BOX_SHUTDOWN = "CLUSTER_BOX_SHUTDOWN";
    private static final String REGISTER_CLUSTER_BOX = "REGISTER_CLUSTER_BOX";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZerromqBroker.class);

    private ZerromqBroker(ZerromqBrokerConfig config) {
        this.config = config;
        this.pollItems = new Poller(2);
        if (config.isAutoStart()) {
            startBroker();
        }
    }

    public static ZerromqBroker newBroker(ZerromqBrokerConfig config) {
        return new ZerromqBroker(config);
    }

    @Override
    public void run() {
        // TODO : Guard Against Exceptions
        while (on.get() && !brokerThread.isInterrupted()) {
            pollItems.poll();
            if (pollItems.getItem(0).isReadable()) {
                ZMsg fromClient = ZMsg.recvMsg(frontendSocket, 0);
                if (fromClient == null) {
                    continue;
                }
                Address to = Address.fromJson(new String(fromClient.peek().getData()));
                String clusterBoxName = to.getClusterBoxName();
                if (discoveredClusterBox.containsKey(clusterBoxName)) {
                    ClusterBox selectedClusterBox = refreshAndGetActiveClusterBox(clusterBoxName);
                    if (selectedClusterBox == null) {
                        LOGGER.error("No Active ClusterBox present. Message {} dropped", fromClient);
                        continue;
                    }
                    fromClient.addFirst(selectedClusterBox.clusterBoxId);
                    LOGGER.info("Message {} send to clusterBoxName", fromClient);
                    fromClient.send(backendSocket);
                } else {
                    LOGGER.error("No {} available skip this request", clusterBoxName);
                }
            } else if (pollItems.getItem(1).isReadable()) {
                ZMsg msg = ZMsg.recvMsg(backendSocket, 0);
                if (msg == null) {
                    break;
                }
                String clusterBoxId = new String(msg.removeFirst().getData());
                setPingedAt(clusterBoxId);
                if (CLUSTER_BOX_READY.equals(new String(msg.getFirst().getData()))) {
                    LOGGER.info("ClusterBox {} is ready", clusterBoxId);
                } else if (CLUSTER_BOX_SHUTDOWN.equals(new String(msg.getFirst().getData()))) {

                } else if (REGISTER_CLUSTER_BOX.equals(new String(msg.peekFirst().getData()))) {
                    msg.removeFirst();
                    String clusterBoxName = new String(msg.getFirst().getData());
                    Long pingInterval = Long.valueOf(new String(msg.getLast().getData()));
                    ClusterBox clusterBox = new ClusterBox(clusterBoxName, clusterBoxId, pingInterval);
                    if (discoveredClusterBox.containsKey(clusterBoxName)) {
                        discoveredClusterBox.get(clusterBoxName).add(clusterBox);
                    } else {
                        discoveredClusterBox.put(clusterBoxName, new HashSet<ClusterBox>(Arrays.asList(clusterBox)));
                    }
                    LOGGER.info("Registers Cluster box {} with name {}", clusterBoxId, clusterBoxName);
                    LOGGER.info("ClusterBoxes {} count {}", clusterBoxName,
                            discoveredClusterBox.get(clusterBoxName).size());
                } else {
                    // Not understood , just send request back
                    msg.addFirst(clusterBoxId);
                    msg.send(backendSocket);
                }
            }
        }
        if (frontendSocket != null) {
            brokerContext.destroySocket(frontendSocket);
        }
        if (backendSocket != null) {
            brokerContext.destroySocket(backendSocket);
        }
        if (brokerContext != null) {
            brokerContext.destroy();
        }
        LOGGER.info("ZerroMq Broker shutdown");
    }

    public void startBroker() {
        LOGGER.info("Starting Zerromq Broker");
        brokerContext = new ZContext(DEFAULT_IO_THREAD_COUNT);
        frontendSocket = brokerContext.createSocket(ZMQ.ROUTER);
        backendSocket = brokerContext.createSocket(ZMQ.ROUTER);
        frontendSocket
                .bind(config.getFrontendProtocol() + "://" + config.getIpAddress() + ":" + config.getFrontendPort());
        backendSocket.bind(config.getBackendProtocol() + "://" + config.getIpAddress() + ":" + config.getBackendPort());
        pollItems.register(frontendSocket, Poller.POLLIN);
        pollItems.register(backendSocket, Poller.POLLIN);
        brokerThread = new Thread(this, "zerromq-broker");
        on = new AtomicBoolean(true);
        brokerThread.start();
    }

    public void stopBroker() {
        on.set(false);
    }

    public void dumpClusterBox() {
        try {
            LOGGER.info("Discovered ClusterBoxes - {}", new ObjectMapper().writeValueAsString(discoveredClusterBox));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void setPingedAt(String clusterBoxId) {
        pingedAt.put(clusterBoxId, System.currentTimeMillis());
    }

    private ClusterBox refreshAndGetActiveClusterBox(String clusterBoxName) {
        if (discoveredClusterBox.get(clusterBoxName) == null) {
            return null;
        }
        Iterator<ClusterBox> iterator = discoveredClusterBox.get(clusterBoxName).iterator();
        ClusterBox result = null;
        while (iterator.hasNext()) {
            ClusterBox clusterBox = iterator.next();
            LOGGER.info("Verifying cluster box {}. It was pingedAt", clusterBox, pingedAt.get(clusterBox.clusterBoxId));
            if ((System.currentTimeMillis()) < pingedAt.get(clusterBox.clusterBoxId) + clusterBox.pingInterval) {
                result = clusterBox;
            } else {
                LOGGER.info("Removing Dead Cluster Box {}", clusterBox);
                discoveredClusterBox.get(clusterBoxName).remove(clusterBox);
                if (discoveredClusterBox.get(clusterBoxName).size() == 0) {
                    discoveredClusterBox.remove(clusterBoxName);
                }
            }
        }
        return result;

    }

    private static class ClusterBox {
        private String clusterBoxName;
        private String clusterBoxId;
        private Long pingInterval;

        private ClusterBox(String clusterBoxName, String clusterBoxId, Long pingInterval) {
            super();
            this.clusterBoxName = clusterBoxName;
            this.clusterBoxId = clusterBoxId;
            this.pingInterval = pingInterval;
        }

        @Override
        public String toString() {
            return "ClusterBox [clusterBoxName=" + clusterBoxName + ", clusterBoxId=" + clusterBoxId + ", pingInterval="
                    + pingInterval + "]";
        }
    }

}
