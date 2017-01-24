package com.task.coordinator.base.message;

public abstract class TcsCtrlMessage {

    private Class<? extends TcsCtrlMessage> requestType;

    private Integer retryAttempt;

    private Integer maxRetryCount = DEFAULT_MAX_RETRY;

    private static final Integer DEFAULT_MAX_RETRY = 5;

    public Class<? extends TcsCtrlMessage> getRequestType() {
        return requestType;
    }
    public void setRequestType(Class<? extends TcsCtrlMessage> requestType) {
        this.requestType = requestType;
    }
    public Integer getRetryAttempt() {
        return retryAttempt;
    }
    public void setRetryAttempt(Integer retryAttempt) {
        this.retryAttempt = retryAttempt;
    }
    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }
    public void setMaxRetryCount(Integer maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
}
