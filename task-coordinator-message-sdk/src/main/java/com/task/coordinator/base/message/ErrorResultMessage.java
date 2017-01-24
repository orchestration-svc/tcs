package com.task.coordinator.base.message;

public class ErrorResultMessage extends TcsCtrlMessageResult<ErrorResponse> {

    public ErrorResultMessage(ErrorResponse response) {
        super(true, response);
    }
}
