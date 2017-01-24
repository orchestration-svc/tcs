package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class TaskRollbackDisallowedMessage extends TcsAsyncCtrlMessage {
    public TaskRollbackDisallowedMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRollbackDisallowedMessage(String taskId) {
        super();
        this.taskId = taskId;
    }

    private String taskId;
}
