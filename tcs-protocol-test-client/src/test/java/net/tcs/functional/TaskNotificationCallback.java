package net.tcs.functional;

public interface TaskNotificationCallback {
    public void taskComplete(String taskId);

    public void taskFailed(String taskId);

    public void taskRolledBack(String taskId);
}
