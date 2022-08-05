package org.solution.delaymessage.exception;

public class DelayMessageException extends RuntimeException {

    public DelayMessageException() {
    }

    public DelayMessageException(String message) {
        super(message);
    }

    public DelayMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public DelayMessageException(Throwable cause) {
        super(cause);
    }

    public DelayMessageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
