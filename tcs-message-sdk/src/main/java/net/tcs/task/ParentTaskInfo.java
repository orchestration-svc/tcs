package net.tcs.task;

public class ParentTaskInfo {
    public ParentTaskInfo(String instanceId, String output) {
        this.instanceId = instanceId;
        this.output = output;
    }

    public ParentTaskInfo() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    private String instanceId;

    private String output;
}
