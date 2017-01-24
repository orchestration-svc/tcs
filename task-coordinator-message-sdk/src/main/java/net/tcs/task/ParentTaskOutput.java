package net.tcs.task;

import java.util.Set;

/**
 * A task is scheduled for execution, only after all its predecessor tasks are
 * complete. The output of all predecessor tasks is passed as input for the Task
 * execution.
 */
public interface ParentTaskOutput {
    /**
     * Get the predecessor task names
     *
     * @return
     */
    public Set<String> getPredecessors();

    public ParentTaskInfo getPredecessorTaskInfo(String predecessorTaskName);
}
