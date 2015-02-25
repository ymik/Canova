package org.canova.api.exceptions;

/**
 * @author sonali
 */
public class UnknownFormatException extends Exception {
    public UnknownFormatException() {
    }

    public UnknownFormatException(String message) {
        super(message);
    }

    public UnknownFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownFormatException(Throwable cause) {
        super(cause);
    }

    public UnknownFormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
