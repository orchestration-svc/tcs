package net.tcs.core;

final class TaskHandlerKey {
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((jobName == null) ? 0 : jobName.hashCode());
        result = prime * result + ((taskName == null) ? 0 : taskName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TaskHandlerKey other = (TaskHandlerKey) obj;
        if (jobName == null) {
            if (other.jobName != null)
                return false;
        } else if (!jobName.equals(other.jobName))
            return false;
        if (taskName == null) {
            if (other.taskName != null)
                return false;
        } else if (!taskName.equals(other.taskName))
            return false;
        return true;
    }

    public TaskHandlerKey(String jobName, String taskName) {
        this.jobName = jobName;
        this.taskName = taskName;
    }

    private final String jobName;
    private final String taskName;
}