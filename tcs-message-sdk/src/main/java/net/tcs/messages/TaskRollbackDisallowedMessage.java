package net.tcs.messages;

public class TaskRollbackDisallowedMessage {
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
