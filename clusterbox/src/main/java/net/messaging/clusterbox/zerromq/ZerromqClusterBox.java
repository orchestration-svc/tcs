package net.messaging.clusterbox.zerromq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMsg;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.ClusterBoxConfig;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.litemq.broker.zerromq.ZerromqBrokerConfig;

public class ZerromqClusterBox implements ClusterBox, Runnable {

    private static final String WORKER_READY = "WORKER_READY";
    private static final String WORKER_SHUTDOWN = "WORKER_SHUTDOWN";
    private static final String REGISTER_WORKER = "REGISTER_WORKER";
    private static final Integer DEFAULT_IO_THREAD_COUNT = 1;
    private static final String CLUSTER_BOX_READY = "CLUSTER_BOX_READY";
    private static final String CLUSTER_BOX_SHUTDOWN = "CLUSTER_BOX_SHUTDOWN";
    private static final String REGISTER_CLUSTER_BOX = "REGISTER_CLUSTER_BOX";
    private static final AtomicInteger PING_RETRY_COUNT = new AtomicInteger(3);
    private static final ZFrame PING_FRAME = new ZFrame("ping");
    private static final Long PING_INTERVAL = 3000L; // Unit in ms

    private AtomicBoolean on = new AtomicBoolean(false);
    private ZContext clusterBoxContext;
    private ZMQ.Socket frontendSocket;
    private ZMQ.Socket backendSocket;
    private Thread clusterBoxThread;
    private Poller pollItems;
    private ClusterBoxDropBox clusterBoxDropBox;
    private ConcurrentHashMap<String, Set<ClusterBoxWorker>> registersWorkers = new ConcurrentHashMap<String, Set<ClusterBoxWorker>>();
    private ConcurrentHashMap<String, Set<String>> discoveredWorkers = new ConcurrentHashMap<String, Set<String>>();
    private AtomicBoolean pongPending = new AtomicBoolean(true);
    private Long pingInterval = PING_INTERVAL;
    private AtomicBoolean dirty = new AtomicBoolean(true);

    private final ZerromqClusterBoxConfig clusterBoxConfig;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerromqClusterBox.class);

    private ZerromqClusterBox(ZerromqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
        pingInterval = clusterBoxConfig.getPingInterval() == null ? PING_INTERVAL : clusterBoxConfig.getPingInterval();
        this.pollItems = new Poller(2);
        clusterBoxContext = new ZContext(DEFAULT_IO_THREAD_COUNT);
        clusterBoxDropBox = new ZerromqClusterBoxDropBox(clusterBoxConfig);
        if (clusterBoxConfig.isAutoStart()) {
            start();
        } else {
            LOGGER.info("Cluster box needs an explicit start");
        }
    }

    @Override
    public ClusterBoxConfig getClusterBoxConfig() {
        return clusterBoxConfig;
    }

    public static ClusterBox newClusterBox(ZerromqClusterBoxConfig clusterBoxConfig) {
        return new ZerromqClusterBox(clusterBoxConfig);
    }

    @Override
    public void start() {
        LOGGER.info("Staring clusterbox {} - {}", clusterBoxConfig.getClusterBoxName(),
                clusterBoxConfig.getClusterBoxId());
        startFrontEnd();
        startBackEnd();
        clusterBoxThread = new Thread(this,
                clusterBoxConfig.getClusterBoxName() + "-" + clusterBoxConfig.getClusterBoxId());
        on.set(true);
        clusterBoxThread.start();
    }

    @Override
    public void run() {
        while (on.get() && !clusterBoxThread.isInterrupted()) {
            if (dirty.get()) {
                reInitPolling();
            }
            pollItems.poll(pingInterval);
            if (pollItems.getItem(0).isReadable()) {
                ZMsg msg = ZMsg.recvMsg(frontendSocket, 0);
                if (msg.isEmpty()) {
                    LOGGER.error("Got an Unexpected Empty message. ShuttingDown !!");
                    break;
                }
                // Check if Frame is HeartBeat
                assert (clusterBoxConfig.getClusterBoxId().equalsIgnoreCase(new String(msg.removeFirst().getData())));
                if (!PING_FRAME.equals(msg.peek())) {
                    Address to = Address.fromJson(new String(msg.peek().getData()));
                    String workName = to.getClusterBoxMailBoxName();
                    if (discoveredWorkers.containsKey(workName)) {
                        String workerName = discoveredWorkers.get(workName).iterator().next();
                        msg.addFirst(workerName);
                        LOGGER.info("Sending work {}", msg);
                        msg.send(backendSocket);

                    } else {
                        LOGGER.error("No worker with name {} Discovered yet. Skipping the message", workName);
                    }
                }
                resetLiveliness();
                pongPending.set(false);
            } else if (pollItems.getItem(1).isReadable()) {
                // Message on the Backend
                ZMsg msg = ZMsg.recvMsg(backendSocket, 0);
                if (msg == null) {
                    break;
                }
                String workerName = new String(msg.removeFirst().getData());
                if (WORKER_READY.equals(new String(msg.getLast().getData()))) {
                    LOGGER.info("Worker {} is ready !!", workerName);
                } else if (WORKER_SHUTDOWN.equals(new String(msg.getLast().getData()))) {
                    // TODO Clean up HashMap
                    LOGGER.info("Worker {} is shutdown !!", workerName);
                } else if (REGISTER_WORKER.equals(new String(msg.peekFirst().getData()))) {
                    msg.removeFirst();
                    String workName = new String(msg.getFirst().getData());
                    LOGGER.info("Registering Worker {} for work {}", workerName, workName);
                    if (discoveredWorkers.containsKey(workName)) {
                        discoveredWorkers.get(workName).add(workerName);
                    } else {
                        discoveredWorkers.put(workName, new HashSet<String>(Arrays.asList(workerName)));
                    }
                }
            } else {
                if (pongPending.get() && PING_RETRY_COUNT.decrementAndGet() == 0) {
                    LOGGER.error("Detected Dead broker. Reconnecting ...");
                    clusterBoxContext.destroySocket(frontendSocket);
                    startFrontEnd();
                    resetLiveliness();
                    dirty.set(true);
                }
                sendPing();
                pongPending.set(true);
            }
        }
        on.set(false);
        if (frontendSocket != null) {
            clusterBoxContext.destroySocket(frontendSocket);
            ;
        }
        if (backendSocket != null) {
            clusterBoxContext.destroySocket(backendSocket);
            ;
        }
        if (clusterBoxContext != null) {
            clusterBoxContext.destroy();
        }
    }

    @Override
    public void shutDown() {
        on.set(false);
        // TODO : Shutdown every mailbox
    }

    @Override
    public void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox) {
        ClusterBoxWorker worker = new ClusterBoxWorker(clusterBoxMessageBox, clusterBoxDropBox, clusterBoxConfig);

        worker.start();
        if (registersWorkers.containsKey(worker.workerId)) {
            registersWorkers.get(worker.workerId).add(worker);
        } else {
            registersWorkers.put(worker.workerId, new HashSet<ClusterBoxWorker>(Arrays.asList(worker)));
        }
    }

    @Override
    public ClusterBoxDropBox getDropBox() {
        return this.clusterBoxDropBox;
    }

    @Override
    public boolean isOn() {
        return (on.get() && clusterBoxThread.isAlive());
    }

    private void sendPing() {
        ZMsg pingMsg = new ZMsg();
        pingMsg.add("ping");
        pingMsg.send(frontendSocket);
    }

    private void resetLiveliness() {
        PING_RETRY_COUNT.set(ZerromqBrokerConfig.LIVELINESS_COUNT);
    }

    private void startFrontEnd() {
        frontendSocket = clusterBoxContext.createSocket(ZMQ.DEALER);
        frontendSocket.setIdentity(clusterBoxConfig.getClusterBoxId().getBytes());
        frontendSocket.connect(clusterBoxConfig.getBrokerConfig().getBackendProtocol() + "://"
                + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                + clusterBoxConfig.getBrokerConfig().getBackendPort());
        ZMsg bootstrapMsg = new ZMsg();
        bootstrapMsg.add(REGISTER_CLUSTER_BOX);
        bootstrapMsg.add(clusterBoxConfig.getClusterBoxName());
        bootstrapMsg.add(String.valueOf(pingInterval).getBytes());
        bootstrapMsg.send(frontendSocket);
        LOGGER.info("Started and registered clusterbox {} with broker", clusterBoxConfig.getClusterBoxName());
        ZFrame readyFrame = new ZFrame(CLUSTER_BOX_READY);
        readyFrame.send(frontendSocket, 0);
    }

    private void startBackEnd() {
        backendSocket = clusterBoxContext.createSocket(ZMQ.ROUTER);
        backendSocket.bind(clusterBoxConfig.getBackendProtocol() + "://" + clusterBoxConfig.getBackendAddress());
    }

    private void reInitPolling() {
        this.pollItems = new Poller(2);
        pollItems.register(frontendSocket, Poller.POLLIN);
        pollItems.register(backendSocket, Poller.POLLIN);
        dirty.set(false);
    }

    private static class ClusterBoxWorker implements Runnable {
        private final ClusterBoxMessageBox clusterBoxMailBox;
        private final ZerromqClusterBoxConfig clusterBoxConfig;
        private ZMQ.Context clusterBoxWorkerContext;
        private ZMQ.Socket frontendSocket;
        private Thread clusterBoxWorkerThread;
        private AtomicBoolean on = new AtomicBoolean(false);
        private String workerId;
        private String workerName;
        private ClusterBoxDropBox dropBox;

        protected ClusterBoxWorker(ClusterBoxMessageBox clusterBoxMailBox, ClusterBoxDropBox dropBox,
                ZerromqClusterBoxConfig clusterBoxConfig) {
            this.clusterBoxMailBox = clusterBoxMailBox;
            this.clusterBoxConfig = clusterBoxConfig;
            clusterBoxWorkerContext = ZMQ.context(DEFAULT_IO_THREAD_COUNT);
            this.workerId = clusterBoxMailBox.getMessageBoxId();
            this.workerName = clusterBoxMailBox.getMessageBoxName();
            this.dropBox = dropBox;
        }

        @Override
        public void run() {
            frontendSocket
                    .connect(clusterBoxConfig.getBackendProtocol() + "://" + clusterBoxConfig.getBackendAddress());
            ZMsg bootstrapMsg = new ZMsg();
            bootstrapMsg.add(REGISTER_WORKER);
            bootstrapMsg.add(workerName);
            bootstrapMsg.send(frontendSocket);
            ZFrame readyFrame = new ZFrame(WORKER_READY);
            readyFrame.send(frontendSocket, 0);
            while (on.get() && !clusterBoxWorkerThread.isInterrupted()) {
                ZMsg msg = ZMsg.recvMsg(frontendSocket);
                if (msg == null) {
                    break;
                }
                String request = new String(msg.getFirst().getData());
                String message = new String(msg.getLast().getData());
                LOGGER.info("Request {} Message Received {}", request, message);
                // Execute the Handler
            }
            // Finished current work shut down all stuffs
            if (frontendSocket != null) {
                ZMsg doneMsg = new ZMsg();
                doneMsg.add(WORKER_SHUTDOWN);
                doneMsg.send(frontendSocket);
                frontendSocket.close();
            }
            if (clusterBoxWorkerContext != null) {
                clusterBoxWorkerContext.term();
            }
        }

        protected void start() {
            frontendSocket = clusterBoxWorkerContext.socket(ZMQ.DEALER);
            frontendSocket.setIdentity(workerId.getBytes());
            on.set(true);
            clusterBoxWorkerThread = new Thread(this, workerId);
            clusterBoxWorkerThread.start();
            LOGGER.info("Worker {} for work {} started", workerId, workerName);
        }

        protected void stop() {
            on.set(false);
        }
    }
}
