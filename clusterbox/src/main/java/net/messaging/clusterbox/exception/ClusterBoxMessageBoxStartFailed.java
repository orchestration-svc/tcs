package net.messaging.clusterbox.exception;

public class ClusterBoxMessageBoxStartFailed extends RuntimeException {

    private static final long serialVersionUID = 6893934635398573797L;

    private String reason;

    public ClusterBoxMessageBoxStartFailed(String reason) {
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
