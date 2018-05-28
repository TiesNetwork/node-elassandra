package network.tiesdb.service.scope.api;

import java.io.Closeable;
import java.util.Map;

import network.tiesdb.api.TiesVersion;

public interface TiesServiceScope extends Closeable {

    TiesVersion getServiceVersion();

    void insert(TiesServiceScopeActionInsert action) throws TiesServiceScopeException;
    void update(TiesServiceScopeActionInsert action) throws TiesServiceScopeException;

}
