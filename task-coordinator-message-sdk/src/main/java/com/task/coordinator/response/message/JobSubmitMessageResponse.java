package com.task.coordinator.response.message;

import com.task.coordinator.base.message.TcsCtrlMessageResult;

import net.tcs.messages.JobSubmitResponse;

public class JobSubmitMessageResponse extends TcsCtrlMessageResult<JobSubmitResponse> {

    public JobSubmitMessageResponse(JobSubmitResponse response) {
        super(response);
    }

}
