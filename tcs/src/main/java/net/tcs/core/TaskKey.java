package net.tcs.core;

public final class TaskKey {
    @Override
    public String toString() {
        return "TaskKey [taskName=" + taskName + ", parallelExecutionIndex=" + parallelExecutionIndex + "]";
    }

    public final String taskName;

    public final int parallelExecutionIndex;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + parallelExecutionIndex;
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
        final TaskKey other = (TaskKey) obj;
        if (parallelExecutionIndex != other.parallelExecutionIndex)
            return false;
        if (taskName == null) {
            if (other.taskName != null)
                return false;
        } else if (!taskName.equals(other.taskName))
            return false;
        return true;
    }

    public TaskKey(String taskName, int parallelExecutionIndex) {
        this.taskName = taskName;
        this.parallelExecutionIndex = parallelExecutionIndex;
    }
}
