package net.tcs.task;

import java.util.Set;

public interface TaskSpec {

    public String getTaskName();

    public String getTaskExecutionTarget();

    public Set<String> getParents();
}
