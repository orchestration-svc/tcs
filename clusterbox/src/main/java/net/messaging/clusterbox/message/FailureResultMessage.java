package net.messaging.clusterbox.message;

public class FailureResultMessage extends Message<FailureResponse> {

    public FailureResultMessage(FailureResponse response) {
        super(response);
    }
}
