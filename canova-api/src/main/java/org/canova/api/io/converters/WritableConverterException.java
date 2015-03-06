package org.canova.api.io.converters;

/**
 * Writable converter exception represents an error
 * for being unable to convert a writable
 * @author Adam Gibson
 */
public class WritableConverterException extends Exception {
    public WritableConverterException() {
    }

    public WritableConverterException(String message) {
        super(message);
    }

    public WritableConverterException(String message, Throwable cause) {
        super(message, cause);
    }

    public WritableConverterException(Throwable cause) {
        super(cause);
    }

    public WritableConverterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
