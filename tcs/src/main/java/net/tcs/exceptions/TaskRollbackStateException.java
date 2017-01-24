package net.tcs.exceptions;

import net.tcs.state.TaskRollbackState;

public class TaskRollbackStateException extends IllegalStateException {

    public TaskRollbackState getExpectedState() {
        return expectedState;
    }

    public TaskRollbackState getActualState() {
        return actualState;
    }

    public TaskRollbackStateException(TaskRollbackState expectedState, TaskRollbackState actualState) {
        super();
        this.expectedState = expectedState;
        this.actualState = actualState;
    }

    private static final long serialVersionUID = 1L;

    private final TaskRollbackState expectedState;

    private final TaskRollbackState actualState;
}
