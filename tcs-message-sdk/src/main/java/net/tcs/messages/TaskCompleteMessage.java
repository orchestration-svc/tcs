package net.tcs.messages;

import java.util.HashMap;
import java.util.Map;

public class TaskCompleteMessage {
    public TaskCompleteMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setTaskOutput(String taskOutput) {
        this.taskOutput = taskOutput;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getTaskOutput() {
        return taskOutput;
    }

    public Map<String, String> getTaskContextOutput() {
        final Map<String, String> taskContextOutput = new HashMap<>(this.taskContextOutput);
        return taskContextOutput;
    }

    public void setTaskContextOutput(Map<String, String> taskContextOutput) {
        this.taskContextOutput = new HashMap<>(taskContextOutput);
    }

    public TaskCompleteMessage(String taskId, String jobId, String taskOutput) {
        super();
        this.taskId = taskId;
        this.jobId = jobId;
        this.taskOutput = taskOutput;
    }

    private String taskId;

    private String jobId;

    private String taskOutput;

    private Map<String, String> taskContextOutput = new HashMap<>();
}
