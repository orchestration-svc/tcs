package com.task.coordinator.response.message;

import com.task.coordinator.base.message.TcsCtrlMessageResult;

import net.tcs.messages.JobRollbackResponse;

public class JobRollbackMessageResponse extends TcsCtrlMessageResult<JobRollbackResponse> {

    public JobRollbackMessageResponse(JobRollbackResponse response) {
        super(response);
    }
}
