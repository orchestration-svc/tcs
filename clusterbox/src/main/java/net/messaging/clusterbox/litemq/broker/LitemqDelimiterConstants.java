package net.messaging.clusterbox.litemq.broker;

public interface LitemqDelimiterConstants {

    final String FRONTEND_DELIMITER = "DONE";
    final String BACKEND_DELIMITER = "DONE";
    final String WORKER_READY = "READY";
    public byte[] CLUSTER_BOX_READY = { '\101' };
    public byte[] CLUSTER_BOX_DONE = { '\100' };
}
