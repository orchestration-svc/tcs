package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class QueryJobInstanceResponse extends TcsAsyncCtrlMessage {

    public QueryJobInstanceResponse(String jobInstanceId, String shardId, String status) {
        super();
        this.jobInstanceId = jobInstanceId;
        this.shardId = shardId;
        this.status = status;
    }

    public QueryJobInstanceResponse() {
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    private String jobInstanceId;
    private String shardId;
    private String status;
}
