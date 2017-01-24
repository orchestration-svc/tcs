package com.task.coordinator.base.message;

public class ErrorResponse {

    private String errorCode;
    private String errorMessage;
    private String detailedMessage;

    public ErrorResponse(){

    }

    public ErrorResponse(String errorCode, String errorMessage, String detailedMessage) {
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
        return "ErrorResponse [errorCode=" + errorCode + ", errorMessage=" + errorMessage + ", detailedMessage="
                + detailedMessage + "]";
    }


}
