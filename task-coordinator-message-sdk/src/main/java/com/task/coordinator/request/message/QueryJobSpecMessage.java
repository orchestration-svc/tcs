package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsSyncCtrlMessage;

public class QueryJobSpecMessage extends TcsSyncCtrlMessage {

    private String jobName;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

}
