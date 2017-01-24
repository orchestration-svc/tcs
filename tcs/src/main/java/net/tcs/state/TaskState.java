package net.tcs.state;

public enum TaskState {
    INIT("INIT"), INPROGRESS("INPROGRESS"), COMPLETE("COMPLETE"), FAILED("FAILED");

    private final String state;

    private TaskState(final String state) {
        this.state = state;
    }

    public String value() {
        return state;
    }

    public static TaskState get(String state) {
        switch (state) {
        case "INIT":
            return TaskState.INIT;
        case "INPROGRESS":
            return TaskState.INPROGRESS;
        case "COMPLETE":
            return TaskState.COMPLETE;
        case "FAILED":
            return TaskState.FAILED;
        default:
            throw new IllegalArgumentException(state);
        }
    }
}
