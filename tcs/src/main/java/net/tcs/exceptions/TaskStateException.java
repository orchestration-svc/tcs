package net.tcs.exceptions;

import net.tcs.state.TaskState;

public class TaskStateException extends IllegalStateException {

    public TaskState getExpectedState() {
        return expectedState;
    }

    public TaskState getActualState() {
        return actualState;
    }

    public TaskStateException(TaskState expectedState, TaskState actualState) {
        super();
        this.expectedState = expectedState;
        this.actualState = actualState;
    }
    private static final long serialVersionUID = 1L;

    private final TaskState expectedState;

    private final TaskState actualState;
}
