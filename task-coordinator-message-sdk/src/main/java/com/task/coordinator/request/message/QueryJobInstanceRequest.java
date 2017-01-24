package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsSyncCtrlMessage;

public class QueryJobInstanceRequest extends TcsSyncCtrlMessage {

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    private String jobInstanceId;
}
