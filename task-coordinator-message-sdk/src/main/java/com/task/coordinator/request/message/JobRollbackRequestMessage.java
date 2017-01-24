package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

import net.tcs.messages.JobRollbackRequest;

public class JobRollbackRequestMessage extends TcsAsyncCtrlMessage {

    private JobRollbackRequest request;

    public JobRollbackRequest getRequest() {
        return request;
    }

    public void setRequest(JobRollbackRequest request) {
        this.request = request;
    }
}
