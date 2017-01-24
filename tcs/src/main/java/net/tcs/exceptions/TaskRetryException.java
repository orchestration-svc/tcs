package net.tcs.exceptions;

public class TaskRetryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TaskRetryException(String message) {
        super(message);
    }

    public TaskRetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
