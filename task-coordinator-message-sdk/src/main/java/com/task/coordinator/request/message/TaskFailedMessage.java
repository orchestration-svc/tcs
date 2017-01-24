package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class TaskFailedMessage extends TcsAsyncCtrlMessage {
    public TaskFailedMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public TaskFailedMessage(String taskId, String taskOutput) {
        this.taskId = taskId;
        this.error = taskOutput;
    }

    private String taskId;

    private String error;
}
