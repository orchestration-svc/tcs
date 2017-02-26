package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class RollbackJobMessage extends TcsAsyncCtrlMessage {

    public RollbackJobMessage() {
        super();
    }

    public RollbackJobMessage(String jobId) {
        super();
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    private String jobId;
}
