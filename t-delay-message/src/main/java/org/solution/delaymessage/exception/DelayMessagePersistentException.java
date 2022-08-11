package org.solution.delaymessage.exception;

/**
 * @author huxuewang
 */
public class DelayMessagePersistentException extends RuntimeException {

    public DelayMessagePersistentException() {
    }

    public DelayMessagePersistentException(String message) {
        super(message);
    }

    public DelayMessagePersistentException(String message, Throwable cause) {
        super(message, cause);
    }

    public DelayMessagePersistentException(Throwable cause) {
        super(cause);
    }

    public DelayMessagePersistentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
