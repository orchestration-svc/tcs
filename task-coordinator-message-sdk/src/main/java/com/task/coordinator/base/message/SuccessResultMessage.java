package com.task.coordinator.base.message;

public class SuccessResultMessage<T> extends TcsCtrlMessageResult<T> {

    public SuccessResultMessage(T response){
        super(response);
    }
}
