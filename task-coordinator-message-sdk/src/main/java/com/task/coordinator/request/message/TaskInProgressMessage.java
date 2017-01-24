package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class TaskInProgressMessage extends TcsAsyncCtrlMessage {
    public TaskInProgressMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskInProgressMessage(String taskId) {
        this.taskId = taskId;
    }

    private String taskId;
}
