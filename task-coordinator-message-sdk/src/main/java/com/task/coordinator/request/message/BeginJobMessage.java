package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class BeginJobMessage extends TcsAsyncCtrlMessage {

    public BeginJobMessage() {
        super();
    }

    public BeginJobMessage(String jobId) {
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
