package com.task.coordinator.base.message;

public abstract class TcsAsyncCtrlMessage extends TcsCtrlMessage {
    public TcsAsyncCtrlMessage(){
        setRequestType(this.getClass());
    }
}
