package network.tiesdb.handler.impl.v0r0.controller;

public class ConversionException extends RuntimeException {

    private static final long serialVersionUID = 4638693398537491524L;

    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConversionException(String message) {
        super(message);
    }

    public ConversionException(Throwable cause) {
        super(cause);
    }

}
