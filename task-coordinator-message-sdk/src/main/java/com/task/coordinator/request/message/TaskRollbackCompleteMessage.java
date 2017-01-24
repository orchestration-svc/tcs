package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class TaskRollbackCompleteMessage extends TcsAsyncCtrlMessage {
    public TaskRollbackCompleteMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRollbackCompleteMessage(String taskId) {
        super();
        this.taskId = taskId;
    }

    private String taskId;
}
