package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsSyncCtrlMessage;

import net.tcs.task.JobDefinition;

public class JobSpecRegistrationMessage extends TcsSyncCtrlMessage {

    private JobDefinition jobSpec;

    public JobDefinition getJobSpec() {
        return jobSpec;
    }

    public void setJobSpec(JobDefinition jobSpec) {
        this.jobSpec = jobSpec;
    }
}
