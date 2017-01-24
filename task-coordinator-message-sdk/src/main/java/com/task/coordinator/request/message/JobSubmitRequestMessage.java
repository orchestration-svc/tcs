package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

import net.tcs.messages.JobSubmitRequest;

public class JobSubmitRequestMessage extends TcsAsyncCtrlMessage{

    private JobSubmitRequest request;


    public JobSubmitRequest getRequest() {
        return request;
    }


    public void setRequest(JobSubmitRequest request) {
        this.request = request;
    }
}
