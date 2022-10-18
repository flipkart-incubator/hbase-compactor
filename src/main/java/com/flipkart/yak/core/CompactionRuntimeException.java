package com.flipkart.yak.core;

public class CompactionRuntimeException extends Exception {
    public CompactionRuntimeException() {
    }

    public CompactionRuntimeException(String message) {
        super(message);
    }

    public CompactionRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CompactionRuntimeException(Throwable cause) {
        super(cause);
    }

    public CompactionRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
