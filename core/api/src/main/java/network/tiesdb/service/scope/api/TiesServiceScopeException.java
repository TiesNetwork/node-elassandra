package network.tiesdb.service.scope.api;

import network.tiesdb.exception.TiesException;

public class TiesServiceScopeException extends TiesException {

    private static final long serialVersionUID = -1037712902215077519L;

    public TiesServiceScopeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TiesServiceScopeException(String message) {
        super(message);
    }

}