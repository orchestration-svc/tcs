package net.tcs.exceptions;

public class UnregisteredTaskSpecException extends IllegalArgumentException {

    @Override
    public String toString() {
        return "UnregisteredTaskSpecException [jobName=" + jobName + ", taskName=" + taskName + "]";
    }

    public String getJobName() {
        return jobName;
    }

    public String getTaskName() {
        return taskName;
    }

    public UnregisteredTaskSpecException(String jobName, String taskName) {
        super();
        this.jobName = jobName;
        this.taskName = taskName;
    }
    private static final long serialVersionUID = 1L;
    private final String jobName;
    private final String taskName;
}
