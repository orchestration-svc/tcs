package net.messaging.clusterbox.message;

public class FailureResponse {

    private String errorCode;
    private String errorMessage;
    private String detailedMessage;

    public FailureResponse() {

    }

    public FailureResponse(String errorCode, String errorMessage, String detailedMessage) {
        super();
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.detailedMessage = detailedMessage;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public void setDetailedMessage(String detailedMessage) {
        this.detailedMessage = detailedMessage;
    }

    @Override
    public String toString() {
        return "FailureResponse [errorCode=" + errorCode + ", errorMessage=" + errorMessage + ", detailedMessage="
                + detailedMessage + "]";
    }

}
