package net.messaging.clusterbox.exception;

public class ClusterBoxDropBoxCreateFailed extends RuntimeException {

    private static final long serialVersionUID = 6893934635398573797L;

    private String reason;

    public ClusterBoxDropBoxCreateFailed(String reason) {
        super();
        this.setReason(reason);
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

}
