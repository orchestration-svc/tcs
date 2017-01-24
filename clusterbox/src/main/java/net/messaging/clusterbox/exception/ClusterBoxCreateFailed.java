package net.messaging.clusterbox.exception;

public class ClusterBoxCreateFailed extends RuntimeException {

    private static final long serialVersionUID = 6893934635398573797L;

    private String reason;

    public ClusterBoxCreateFailed(String reason) {
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
